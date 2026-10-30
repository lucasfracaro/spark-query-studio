"""
Estimativa do tamanho da sessão do Glue (tipo e número de workers) para uma query.

Nada roda no Athena. As informações vêm de:

1. Glue Data Catalog: as partições que o filtro da orquestração escolhe, o tamanho guardado nelas
   (estatísticas do crawler, do Hive ou do ANALYZE TABLE), o formato e as colunas da tabela.
2. S3, quando o catálogo não tem o tamanho: soma os arquivos das partições, mas só nos buckets da sua
   conta. A lista deles vem do ListBuckets, que só devolve buckets da própria conta: um bucket de outra
   conta nunca recebe nenhuma chamada.
3. Histórico do Athena: se a query já rodou no Athena, os números daquela execução (dados lidos, linhas,
   dados trocados entre as etapas). Ler o histórico não roda query nem ocupa sessão do Athena.
4. A própria query: JOINs, funções de janela, DISTINCT, ORDER BY.

A conta está toda em `estimate()` e é simples de propósito: o trabalho (em segundos de um núcleo) é o
volume lido dividido pela velocidade de leitura de um núcleo, mais o custo de abrir cada pedaço de
arquivo, mais o volume trocado entre as etapas (shuffle) dividido pela velocidade do shuffle. O número de
núcleos é o que termina esse trabalho no tempo desejado, limitado pelo paralelismo que os dados permitem.
No Glue cada worker roda um executor, e um dos workers é o driver.
"""
from __future__ import annotations

import math
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from engines import GLUE_WORKER_TYPES
from settings_store import MAX_WORKERS, MIN_WORKERS

try:
    import sqlglot
    from sqlglot import exp
except ImportError:  # pragma: no cover - tratado na tela
    sqlglot = None
    exp = None

MiB, GiB, TiB = 1024 ** 2, 1024 ** 3, 1024 ** 4

# ---- parâmetros do modelo (aproximados: servem para escolher a ordem de grandeza, não o segundo exato)
READ_RATE = 50 * MiB                # bytes/s que um núcleo lê de Parquet/ORC comprimido, já filtrando
READ_RATE_DYNAMIC_FRAME = 20 * MiB  # idem pelo DynamicFrame do Glue (todas as colunas, registro a registro)
SHUFFLE_RATE = 60 * MiB             # bytes/s (volume descomprimido) que um núcleo troca no shuffle
TASK_COST_S = 0.15                  # por pedaço de arquivo: abrir, ler o rodapé do Parquet, latência do S3
SPLIT_BYTES = 128 * MiB             # spark.sql.files.maxPartitionBytes
EXPANSION = 3.0                     # dados descomprimidos / Parquet comprimido
SHUFFLE_FACTOR = {"leve": 0.05, "média": 0.5, "pesada": 1.2}   # shuffle / dados lidos descomprimidos
OVERHEAD_S = 15                     # planejar, listar as partições e iniciar as tarefas
DEFAULT_TARGET_S = 120
SHUFFLE_DISK_RATIO = 0.5            # os arquivos de shuffle são comprimidos: ~metade do volume
DISK_USE = 0.6                      # parte do disco livre do worker que o shuffle pode ocupar
FREE_DISK_GB = {"G.1X": 44, "G.2X": 78, "G.4X": 230, "G.8X": 485}   # disco livre por worker (docs do Glue)
TYPES = ("G.1X", "G.2X", "G.4X", "G.8X")
DEFAULT_SHUFFLE_PARTITIONS = 200
MAX_SHUFFLE_PARTITIONS = 20000

# ---- limites da coleta (a janela fica esperando por ela)
MAX_PARTITIONS = 100_000            # partições listadas por tabela
S3_SAMPLE = 40                      # partições medidas no S3 por tabela (o resto é proporcional)
S3_PAGES = 5                        # páginas de 1000 arquivos por partição
S3_PAGES_TABLE = 20                 # idem para tabela sem partição
GLUE_BUDGET_S = 30.0
ATHENA_BUDGET_S = 10.0
ATHENA_MAX_PER_WORKGROUP = 300
ATHENA_MAX_WORKGROUPS = 10

HOW_TO_IMPROVE = ("Para melhorar a estimativa: cole o ID de uma execução desta query no Athena (console do Athena → "
                  "Recent queries), ou peça para rodarem o crawler / `ANALYZE TABLE ... COMPUTE STATISTICS` nas "
                  "tabelas, para o catálogo guardar o tamanho das partições.")

COLUMNAR = {"parquet", "orc", "iceberg", "delta", "hudi"}
_SIZE_KEYS = ("totalSize", "spark.sql.statistics.totalSize", "sizeKey")
_ROW_KEYS = ("numRows", "spark.sql.statistics.numRows", "recordCount")
_FILE_KEYS = ("numFiles", "objectCount")
_NUMERIC_TYPES = ("tinyint", "smallint", "int", "bigint", "long", "decimal", "double", "float", "real")
_TYPE_WEIGHTS = (("string", 3.0), ("varchar", 3.0), ("char", 3.0), ("binary", 3.0), ("array", 5.0), ("map", 5.0),
                 ("struct", 5.0), ("boolean", 0.5), ("tinyint", 0.5), ("smallint", 0.5))
_EXECUTION_ID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_COMMENTS = re.compile(r"--[^\n]*|/\*.*?\*/", re.S)


# --------------------------------------------------------------------------------------------
# Estruturas
# --------------------------------------------------------------------------------------------
@dataclass
class TableSize:
    """O que se sabe do tamanho de uma base da orquestração."""
    alias: str
    database: str
    table: str
    filter: str = ""
    format: str = ""
    partitioned: bool = False
    partitions: int | None = None          # partições escolhidas pelo filtro
    partitions_total: int | None = None    # partições da tabela (quando foi preciso listar todas)
    bytes: int | None = None               # arquivos das partições escolhidas (todas as colunas)
    bytes_source: str = ""
    files: int | None = None
    rows: int | None = None
    columns_used: int | None = None
    columns_total: int | None = None
    read_fraction: float = 1.0             # parte dos bytes que a leitura nativa lê (só as colunas usadas)
    notes: list[str] = field(default_factory=list)

    @property
    def read_bytes(self) -> int | None:
        return None if self.bytes is None else int(self.bytes * self.read_fraction)


@dataclass
class AthenaRun:
    """Uma execução da query no histórico do Athena (só os números dela: nada é executado)."""
    execution_id: str
    workgroup: str = ""
    state: str = ""
    submitted: object = None               # datetime
    match: str = "exata"                   # "id" | "exata" | "parecida" (mesma query com outros valores)
    scanned_bytes: int | None = None
    engine_ms: int | None = None
    reused: bool = False                   # reaproveitou o resultado de outra execução (números zerados)
    input_rows: int | None = None
    output_rows: int | None = None
    exchange_bytes: int | None = None      # soma do que as etapas passaram umas para as outras
    error: str = ""

    @property
    def usable(self) -> bool:
        return self.state == "SUCCEEDED" and not self.reused and bool(self.scanned_bytes)

    def describe(self) -> str:
        tipo = {"id": "execução informada", "exata": "mesma query",
                "parecida": "mesma query com outros valores (ex.: outra data)"}.get(self.match, self.match)
        quando = self.submitted.strftime("%d/%m %H:%M") if hasattr(self.submitted, "strftime") else ""
        partes = []
        if self.scanned_bytes is not None:
            partes.append(f"{fmt_bytes(self.scanned_bytes)} lidos")
        if self.engine_ms:
            partes.append(f"{fmt_duration(self.engine_ms / 1000)} de execução")
        if self.input_rows:
            partes.append(f"{fmt_count(self.input_rows)} de linhas lidas")
        if self.exchange_bytes is not None:
            partes.append(f"{fmt_bytes(self.exchange_bytes)} trocados entre as etapas")
        texto = (f"Athena, {tipo}" + (f" de {quando}" if quando else "")
                 + (f" (workgroup `{self.workgroup}`)" if self.workgroup else ""))
        texto += (": " + ", ".join(partes)) if partes else ""
        if self.state == "FAILED":
            texto += f". Ela falhou ({self.error[:200] or 'sem mensagem'}): os números são parciais"
        elif self.reused:
            texto += ". Ela reaproveitou o resultado de outra execução, então não tem os números da leitura"
        return texto + "."


@dataclass
class QueryProfile:
    """O que pesa na query além da leitura."""
    joins: int = 0
    windows: int = 0
    count_distinct: int = 0
    select_distinct: int = 0
    group_by: int = 0
    unions: int = 0
    global_order: bool = False             # ORDER BY sem LIMIT no resultado final
    star: bool = False                     # o resultado final tem * vindo direto de uma base
    columns: frozenset = frozenset()       # nomes de coluna citados em qualquer lugar da query

    @property
    def weight(self) -> str:
        if self.windows or self.count_distinct or self.joins >= 3 or self.global_order:
            return "pesada"
        if self.joins or self.group_by or self.select_distinct or self.unions:
            return "média"
        return "leve"

    def describe(self) -> str:
        partes = []
        if self.joins:
            partes.append(f"{self.joins} JOIN" + ("s" if self.joins > 1 else ""))
        if self.windows:
            partes.append(f"{self.windows} função(ões) de janela")
        if self.count_distinct:
            partes.append("COUNT(DISTINCT)")
        if self.select_distinct:
            partes.append("SELECT DISTINCT")
        if self.group_by:
            partes.append("GROUP BY")
        if self.unions:
            partes.append("UNION")
        if self.global_order:
            partes.append("ORDER BY sem LIMIT")
        return ", ".join(partes)


@dataclass
class SizingData:
    """Tudo o que foi coletado (rede). A conta em si (`estimate`) é instantânea e pode ser refeita."""
    tables: list[TableSize]
    athena: AthenaRun | None
    profile: QueryProfile
    notes: list[str] = field(default_factory=list)


@dataclass
class Estimate:
    worker_type: str | None = None
    num_workers: int | None = None
    read_bytes: int | None = None
    read_source: str = ""
    shuffle_bytes: int | None = None
    shuffle_source: str = ""
    tasks: int | None = None
    seconds: float | None = None
    confidence: str = "baixa"              # "alta" | "média" | "baixa"
    reasons: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    shuffle_partitions: int | None = None  # sugestão para spark.sql.shuffle.partitions
    alternative: Estimate | None = None    # se a leitura cair no DynamicFrame (Glue 4.0 com Lake Formation)

    @property
    def ok(self) -> bool:
        return self.worker_type is not None and self.num_workers is not None

    @property
    def dpu(self) -> int | None:
        return GLUE_WORKER_TYPES[self.worker_type].dpu * self.num_workers if self.ok else None

    @property
    def label(self) -> str:
        return f"{self.worker_type} × {self.num_workers}" if self.ok else "—"


# --------------------------------------------------------------------------------------------
# Formatação
# --------------------------------------------------------------------------------------------
def _num(v: float) -> str:
    if v >= 100 or abs(v - round(v)) < 0.05:
        return f"{v:.0f}"
    return f"{v:.1f}".replace(".", ",")


def fmt_bytes(n: float | None) -> str:
    if n is None:
        return "—"
    for unidade, fator in (("TB", TiB), ("GB", GiB), ("MB", MiB), ("KB", 1024)):
        if n >= fator:
            return f"{_num(n / fator)} {unidade}"
    return f"{int(n)} B"


def fmt_count(n: float | None) -> str:
    if n is None:
        return "—"
    for nome, fator in (("bi", 1e9), ("mi", 1e6), ("mil", 1e3)):
        if n >= fator:
            return f"{_num(n / fator)} {nome}"
    return str(int(n))


def fmt_duration(s: float | None) -> str:
    if s is None:
        return "—"
    if s < 90:
        return f"{max(1, round(s))} s"
    minutos = s / 60
    return f"~{round(minutos)} min" if minutos < 15 else f"~{round(minutos / 5) * 5} min"


# --------------------------------------------------------------------------------------------
# Filtro de partição: no formato do Glue (GetPartitions) ou avaliado aqui, partição por partição
# --------------------------------------------------------------------------------------------
class _Unsupported(Exception):
    """O filtro tem algo que a conversão/avaliação simples não cobre (função, subquery, outra coluna...)."""


_OPS = {"EQ": "=", "NEQ": "<>", "GT": ">", "GTE": ">=", "LT": "<", "LTE": "<="}
_FLIP = {"EQ": "EQ", "NEQ": "NEQ", "GT": "LT", "GTE": "LTE", "LT": "GT", "LTE": "GTE"}


def _comparison_op(node) -> str | None:
    for nome in _OPS:
        if type(node) is getattr(exp, nome):
            return nome
    return None


def _is_numeric(tipo: str) -> bool:
    return (tipo or "").strip().lower().startswith(_NUMERIC_TYPES)


def _number(valor) -> Decimal | None:
    try:
        return Decimal(str(valor).strip())
    except (InvalidOperation, ValueError):
        return None


def _literal(node) -> tuple[str, bool]:
    """(valor, é texto). Aceita CAST('2026-10-01' AS DATE) (o DATE '...' do Athena) e número negativo."""
    if isinstance(node, exp.Paren):
        return _literal(node.this)
    if isinstance(node, exp.Cast) and isinstance(node.this, (exp.Literal, exp.Neg)):
        return _literal(node.this)[0], True
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal) and not node.this.is_string:
        return "-" + str(node.this.this), False
    if isinstance(node, exp.Literal):
        return str(node.this), bool(node.is_string)
    raise _Unsupported(type(node).__name__)


def _column(node, types: dict[str, str]) -> str:
    if isinstance(node, exp.Column) and not isinstance(node.this, exp.Star) and node.name.lower() in types:
        return node.name.lower()
    raise _Unsupported(type(node).__name__)


def _comparison(node, op: str, types: dict[str, str]) -> tuple[str, str, tuple[str, bool]]:
    """(coluna, operador, literal), com a coluna à esquerda."""
    try:
        return _column(node.this, types), op, _literal(node.expression)
    except _Unsupported:
        return _column(node.expression, types), _FLIP[op], _literal(node.this)


def _parse_filter(filtro: str):
    if sqlglot is None or not (filtro or "").strip():
        raise _Unsupported("vazio")
    try:
        return sqlglot.parse_one(filtro, read="spark")
    except sqlglot.errors.SqlglotError as e:
        raise _Unsupported(str(e)) from None


def glue_expression(filtro: str, types: dict[str, str]) -> str | None:
    """O filtro de partição da orquestração no formato do `Expression` do GetPartitions do Glue,
    ou None se ele usar algo que esse formato não tem (função, subquery...)."""
    try:
        return _glue(_parse_filter(filtro), types)
    except _Unsupported:
        return None


def _glue_value(valor: str, texto: bool, tipo: str) -> str:
    if _is_numeric(tipo) and _number(valor) is not None:
        return valor.strip()
    return "'" + valor.replace("'", "''") + "'"


def _glue(node, types: dict[str, str]) -> str:
    if isinstance(node, exp.Paren):
        return "(" + _glue(node.this, types) + ")"
    if isinstance(node, exp.And):
        return f"({_glue(node.this, types)}) AND ({_glue(node.expression, types)})"
    if isinstance(node, exp.Or):
        return f"({_glue(node.this, types)}) OR ({_glue(node.expression, types)})"
    if isinstance(node, exp.Not):
        return f"NOT ({_glue(node.this, types)})"
    op = _comparison_op(node)
    if op:
        col, op, (valor, texto) = _comparison(node, op, types)
        return f"{col} {_OPS[op]} {_glue_value(valor, texto, types[col])}"
    if isinstance(node, exp.Between):
        col = _column(node.this, types)
        lo, hi = _literal(node.args["low"]), _literal(node.args["high"])
        return f"{col} BETWEEN {_glue_value(*lo, types[col])} AND {_glue_value(*hi, types[col])}"
    if isinstance(node, exp.In) and not node.args.get("query") and not node.args.get("unnest"):
        col = _column(node.this, types)
        return f"{col} IN ({', '.join(_glue_value(*_literal(e), types[col]) for e in node.expressions)})"
    if type(node) is exp.Like:
        col = _column(node.this, types)
        return f"{col} LIKE {_glue_value(_literal(node.expression)[0], True, 'string')}"
    raise _Unsupported(type(node).__name__)


def _compare(valor: str, literal: str, texto: bool, tipo: str) -> int:
    """-1/0/1 comparando o valor da partição (texto, como o Glue guarda) com o literal do filtro."""
    if _is_numeric(tipo) or not texto:
        a, b = _number(valor), _number(literal)
        if a is not None and b is not None:
            return (a > b) - (a < b)
    return (valor > literal) - (valor < literal)


def _like(valor: str, padrao: str, flags=0) -> bool:
    regex = "".join(".*" if c == "%" else "." if c == "_" else re.escape(c) for c in padrao)
    return re.fullmatch(regex, valor, flags | re.S) is not None


def partition_matches(node, values: dict[str, str], types: dict[str, str]) -> bool:
    """Avalia o filtro (árvore do sqlglot) para os valores de uma partição. _Unsupported se não souber."""
    if isinstance(node, exp.Paren):
        return partition_matches(node.this, values, types)
    if isinstance(node, exp.And):
        return partition_matches(node.this, values, types) and partition_matches(node.expression, values, types)
    if isinstance(node, exp.Or):
        return partition_matches(node.this, values, types) or partition_matches(node.expression, values, types)
    if isinstance(node, exp.Not):
        return not partition_matches(node.this, values, types)
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    op = _comparison_op(node)
    if op:
        col, op, (literal, texto) = _comparison(node, op, types)
        c = _compare(values.get(col, ""), literal, texto, types[col])
        return {"EQ": c == 0, "NEQ": c != 0, "GT": c > 0, "GTE": c >= 0, "LT": c < 0, "LTE": c <= 0}[op]
    if isinstance(node, exp.Between):
        col = _column(node.this, types)
        v = values.get(col, "")
        return (_compare(v, *_literal(node.args["low"]), types[col]) >= 0
                and _compare(v, *_literal(node.args["high"]), types[col]) <= 0)
    if isinstance(node, exp.In) and not node.args.get("query") and not node.args.get("unnest"):
        col = _column(node.this, types)
        return any(_compare(values.get(col, ""), *_literal(e), types[col]) == 0 for e in node.expressions)
    if type(node) in (exp.Like, exp.ILike):
        col = _column(node.this, types)
        return _like(values.get(col, ""), _literal(node.expression)[0], re.I if type(node) is exp.ILike else 0)
    raise _Unsupported(type(node).__name__)


# --------------------------------------------------------------------------------------------
# Glue Data Catalog e S3
# --------------------------------------------------------------------------------------------
def _error_code(e: Exception) -> str:
    resposta = getattr(e, "response", None)
    if isinstance(resposta, dict):
        codigo = (resposta.get("Error") or {}).get("Code")
        if codigo:
            return str(codigo)
    return type(e).__name__


def _access_denied(e: Exception) -> bool:
    return "accessdenied" in _error_code(e).lower()


def _param_int(params: dict, keys) -> int | None:
    for k in keys:
        try:
            n = int(float(params[k]))
        except (KeyError, TypeError, ValueError):
            continue
        if n >= 0:
            return n
    return None


def table_format(table: dict) -> str:
    params = {str(k).lower(): str(v).lower() for k, v in (table.get("Parameters") or {}).items()}
    if params.get("table_type") == "iceberg":
        return "iceberg"
    provedor = params.get("spark.sql.sources.provider", "")
    if provedor in ("delta", "iceberg", "hudi"):
        return provedor
    sd = table.get("StorageDescriptor") or {}
    texto = " ".join([sd.get("InputFormat") or "", (sd.get("SerdeInfo") or {}).get("SerializationLibrary") or "",
                      params.get("classification", ""), provedor]).lower()
    if "hoodie" in texto or "hudi" in texto:
        return "hudi"
    for fmt in ("parquet", "orc", "avro", "json", "csv"):
        if fmt in texto:
            return fmt
    if "lazysimpleserde" in texto or "textinputformat" in texto:
        return "texto"
    return "desconhecido"


def _type_weight(tipo: str) -> float:
    t = (tipo or "").strip().lower()
    for prefixo, peso in _TYPE_WEIGHTS:
        if t.startswith(prefixo):
            return peso
    return 1.0


def column_share(columns: list[tuple[str, str]], profile: QueryProfile | None, fmt: str) -> tuple[int | None, float]:
    """(colunas usadas, parte dos bytes que a leitura nativa lê). Em Parquet/ORC só as colunas citadas na query
    são lidas; o peso de cada coluna vem do tipo (texto pesa mais que número). Em formatos de texto a linha
    inteira é lida."""
    if not columns:
        return None, 1.0
    if profile is None or profile.star or fmt not in COLUMNAR:
        return len(columns), 1.0
    usadas = [(n, t) for n, t in columns if n in profile.columns]
    total = sum(_type_weight(t) for _, t in columns)
    parte = sum(_type_weight(t) for _, t in usadas)
    return len(usadas), (max(parte / total, 0.02) if total else 1.0)


def split_s3(location: str) -> tuple[str, str]:
    """s3://bucket/caminho -> (bucket, "caminho/"). A barra no fim evita que dt=2026-10-1 pegue dt=2026-10-10."""
    m = re.match(r"^s3[an]?://([^/]+)/?(.*)$", (location or "").strip())
    if not m:
        return "", ""
    prefixo = m.group(2)
    if prefixo and not prefixo.endswith("/"):
        prefixo += "/"
    return m.group(1), prefixo


class S3Meter:
    """Soma o tamanho dos arquivos no S3, só em buckets da própria conta.

    Os buckets da conta vêm do ListBuckets (que só devolve os da própria conta). Um caminho em bucket
    de outra conta é ignorado sem nenhuma chamada a ele.
    """

    def __init__(self, s3_client):
        self.s3 = s3_client
        self._own: set[str] | None = None
        self._own_error = ""               # erro do ListBuckets (sem permissão, por exemplo)
        self.problem = ""                  # por que a última medição não aconteceu

    def own_buckets(self) -> set[str]:
        if self._own is None:
            try:
                nomes, token = set(), None
                while True:
                    resp = self.s3.list_buckets(**({"ContinuationToken": token} if token else {}))
                    nomes |= {b["Name"] for b in resp.get("Buckets") or []}
                    token = resp.get("ContinuationToken")
                    if not token:
                        break
                self._own = nomes
            except Exception as e:
                self._own = set()
                self._own_error = _error_code(e)
        return self._own

    def measure(self, location: str, deadline: float, max_pages: int) -> tuple[int, int, bool] | None:
        """(bytes, arquivos, cortado) ou None (bucket de outra conta, sem permissão...; motivo em `problem`)."""
        bucket, prefixo = split_s3(location)
        if not bucket:
            self.problem = f"local `{location}` não é do S3"
            return None
        if bucket not in self.own_buckets():
            erro = self._own_error
            self.problem = (f"não consegui listar os buckets da sua conta ({erro}), então o tamanho no S3 não foi "
                            "medido" if erro else
                            f"os arquivos estão no bucket `{bucket}`, que não é da sua conta: o app não consulta "
                            "buckets de outras contas")
            return None
        total = arquivos = paginas = 0
        token = None
        while True:
            params = {"Bucket": bucket, "Prefix": prefixo}
            if token:
                params["ContinuationToken"] = token
            try:
                resp = self.s3.list_objects_v2(**params)
            except Exception as e:
                self.problem = f"não consegui listar `s3://{bucket}/{prefixo}` ({_error_code(e)})"
                return None
            for obj in resp.get("Contents") or []:
                nome = str(obj.get("Key", "")).rsplit("/", 1)[-1]
                if not nome or nome.startswith(("_", ".")) or nome.endswith("$folder$"):
                    continue                 # _SUCCESS, .crc, marcadores de pasta
                total += int(obj.get("Size") or 0)
                arquivos += 1
            paginas += 1
            token = resp.get("NextContinuationToken") if resp.get("IsTruncated") else None
            if not token:
                return total, arquivos, False
            if paginas >= max_pages or time.monotonic() > deadline:
                return total, arquivos, True


def _list_partitions(glue, ident: dict, expression: str | None, deadline: float,
                     limit: int = MAX_PARTITIONS) -> tuple[list[dict], bool]:
    """Partições da tabela (com o Expression, só as escolhidas). Devolve (partições, lista completa?)."""
    out: list[dict] = []
    token = None
    while True:
        params = dict(ident, MaxResults=1000, ExcludeColumnSchema=True)
        if expression:
            params["Expression"] = expression
        if token:
            params["NextToken"] = token
        resp = glue.get_partitions(**params)
        out.extend(resp.get("Partitions") or [])
        token = resp.get("NextToken")
        if not token:
            return out, True
        if len(out) >= limit or time.monotonic() > deadline:
            return out, False


def _chosen_partitions(glue, ident: dict, filtro: str, keys: list[tuple[str, str]], deadline: float,
                       notes: list[str]) -> tuple[list[dict] | None, int | None, bool]:
    """(partições escolhidas pelo filtro, total de partições se conhecido, lista completa?).
    None quando não dá para saber quais partições o filtro escolhe."""
    types = dict(keys)
    if not filtro:
        todas, completo = _list_partitions(glue, ident, None, deadline)
        return todas, (len(todas) if completo else None), completo
    expressao = glue_expression(filtro, types)
    if expressao:
        try:
            escolhidas, completo = _list_partitions(glue, ident, expressao, deadline)
            return escolhidas, None, completo
        except Exception as e:
            if _access_denied(e):
                raise
            # o Glue não aceitou o Expression: o filtro é avaliado aqui, partição por partição
    try:
        arvore = _parse_filter(filtro)
        todas, completo = _list_partitions(glue, ident, None, deadline)
        nomes = [k for k, _ in keys]
        escolhidas = [p for p in todas
                      if partition_matches(arvore, dict(zip(nomes, p.get("Values") or [], strict=False)), types)]
    except _Unsupported:
        notes.append(f"o filtro `{filtro}` usa algo que não consigo avaliar aqui (uma função, por exemplo): "
                     "não sei quantas partições ele escolhe")
        return None, None, False
    return escolhidas, (len(todas) if completo else None), completo


def _sample(items: list, n: int) -> list:
    if len(items) <= n:
        return list(items)
    passo = len(items) / n
    return [items[int(i * passo)] for i in range(n)]


def _extrapolated(values: list[int | None], total: int) -> int | None:
    conhecidos = [v for v in values if v is not None]
    if not conhecidos or len(conhecidos) * 2 < total:
        return None
    return int(sum(conhecidos) * total / len(conhecidos))


def _size_from_partitions(ts: TableSize, parts: list[dict]):
    params = [p.get("Parameters") or {} for p in parts]
    tamanhos = [_param_int(p, _SIZE_KEYS) for p in params]
    total = _extrapolated(tamanhos, len(parts))
    if not total:                            # sem estatística (ou o catálogo diz 0 bytes para tudo)
        return
    conhecidos = sum(1 for t in tamanhos if t is not None)
    ts.bytes = total
    ts.bytes_source = "catálogo" if conhecidos == len(parts) else f"catálogo ({conhecidos} de {len(parts)} partições)"
    ts.rows = _extrapolated([_param_int(p, _ROW_KEYS) for p in params], len(parts))
    ts.files = _extrapolated([_param_int(p, _FILE_KEYS) for p in params], len(parts))


def _measure_partitions(ts: TableSize, s3: S3Meter, parts: list[dict], deadline: float):
    total = arquivos = medidas = 0
    cortado = False
    for p in _sample(parts, S3_SAMPLE):
        if time.monotonic() > deadline:
            break
        r = s3.measure((p.get("StorageDescriptor") or {}).get("Location") or "", deadline, S3_PAGES)
        if r is None:
            ts.notes.append(s3.problem)
            break
        total, arquivos, medidas, cortado = total + r[0], arquivos + r[1], medidas + 1, cortado or r[2]
    if not medidas:
        return
    fator = len(parts) / medidas
    ts.bytes, ts.files = int(total * fator), int(arquivos * fator)
    ts.bytes_source = "S3" if medidas == len(parts) else f"S3 (amostra de {medidas} de {len(parts)} partições)"
    if cortado:
        ts.notes.append("alguma partição tem arquivos demais para listar inteira: o tamanho é o mínimo")


def _size_from_table_average(ts: TableSize, glue, ident: dict, params: dict, deadline: float):
    tamanho = _param_int(params, _SIZE_KEYS)
    if not tamanho:
        return
    total = ts.partitions_total
    if total is None:
        try:
            todas, completo = _list_partitions(glue, ident, None, deadline)
        except Exception:
            return
        if not completo or not todas:
            return
        total = ts.partitions_total = len(todas)
    if not total:
        return
    ts.bytes = int(tamanho * ts.partitions / total)
    ts.bytes_source = "média das partições da tabela"
    linhas = _param_int(params, _ROW_KEYS)
    ts.rows = int(linhas * ts.partitions / total) if linhas else None


def collect_table(glue, ref, profile: QueryProfile, s3: S3Meter | None, deadline: float) -> TableSize:
    """Tamanho das partições que o filtro da entrada escolhe (ref: alias, database, table, filter)."""
    ts = TableSize(alias=ref.alias, database=ref.database, table=ref.table, filter=getattr(ref, "filter", "") or "")
    if not ref.database:
        ts.notes.append("sem database: não consultei o Glue")
        return ts
    if time.monotonic() > deadline:
        ts.notes.append("acabou o tempo da estimativa antes de consultar esta tabela")
        return ts
    try:
        tabela = glue.get_table(DatabaseName=ref.database, Name=ref.table)["Table"]
    except Exception as e:
        ts.notes.append(f"não consegui ler a tabela no Glue ({_error_code(e)})")
        return ts
    sd = tabela.get("StorageDescriptor") or {}
    params = tabela.get("Parameters") or {}
    ts.format = table_format(tabela)
    colunas = [(str(c.get("Name", "")).lower(), c.get("Type", "")) for c in sd.get("Columns") or []]
    chaves = [(str(k.get("Name", "")).lower(), k.get("Type", "string")) for k in tabela.get("PartitionKeys") or []]
    ts.partitioned = bool(chaves)
    ts.columns_total = len(colunas) or None
    ts.columns_used, ts.read_fraction = column_share(colunas, profile, ts.format)

    if ts.format in ("iceberg", "delta", "hudi"):
        ts.notes.append(f"tabela {ts.format}: o Glue Data Catalog não guarda o tamanho dela")
        return ts
    if not chaves:
        ts.bytes = _param_int(params, _SIZE_KEYS) or None
        if ts.bytes:
            ts.bytes_source = "catálogo"
            ts.rows, ts.files = _param_int(params, _ROW_KEYS), _param_int(params, _FILE_KEYS)
        elif s3 is not None and sd.get("Location"):
            r = s3.measure(sd["Location"], deadline, S3_PAGES_TABLE)
            if r is None:
                ts.notes.append(s3.problem)
            else:
                ts.bytes, ts.files, ts.bytes_source = r[0], r[1], "S3"
                if r[2]:
                    ts.notes.append("a tabela tem arquivos demais para listar inteira: o tamanho é o mínimo")
        return ts

    ident = {"DatabaseName": ref.database, "TableName": ref.table}
    try:
        parts, total, completo = _chosen_partitions(glue, ident, ts.filter, chaves, deadline, ts.notes)
        alvo = tabela.get("TargetTable") or {}
        if parts == [] and alvo.get("Name"):
            # link do Lake Formation que não devolveu partições: tenta direto na tabela de origem
            ident = {k: v for k, v in (("CatalogId", alvo.get("CatalogId")), ("DatabaseName", alvo.get("DatabaseName")),
                                       ("TableName", alvo.get("Name"))) if v}
            parts, total, completo = _chosen_partitions(glue, ident, ts.filter, chaves, deadline, ts.notes)
    except Exception as e:
        ts.notes.append(f"não consegui listar as partições no Glue ({_error_code(e)})")
        return ts
    if parts is None:
        return ts
    ts.partitions, ts.partitions_total = len(parts), total
    if not completo:
        ts.notes.append("a tabela tem partições demais para listar todas a tempo: os números são o mínimo")
    if not parts:
        ts.bytes, ts.bytes_source = 0, "nenhuma partição"
        ts.notes.append("o filtro não escolhe nenhuma partição: confira o filtro (formato da data, por exemplo)")
        return ts
    _size_from_partitions(ts, parts)
    if ts.bytes is None and s3 is not None:
        _measure_partitions(ts, s3, parts, deadline)
    if ts.bytes is None:
        _size_from_table_average(ts, glue, ident, params, deadline)
    if ts.bytes is None:
        ts.notes.append("o catálogo não guarda o tamanho das partições")
    return ts


# --------------------------------------------------------------------------------------------
# A query
# --------------------------------------------------------------------------------------------
def _from_key() -> str:
    return "from_" if "from_" in exp.Select.arg_types else "from"


def _final_query(stmt):
    """O SELECT que produz o resultado (INSERT ... SELECT e CREATE TABLE ... AS SELECT incluídos)."""
    if isinstance(stmt, (exp.Insert, exp.Create)):
        q = stmt.args.get("expression")
        return q if isinstance(q, exp.Query) else None
    return stmt if isinstance(stmt, exp.Query) else None


def _final_selects(q) -> list:
    if isinstance(q, exp.Subquery):
        return _final_selects(q.this)
    if isinstance(q, exp.SetOperation):
        return _final_selects(q.this) + _final_selects(q.expression)
    return [q] if isinstance(q, exp.Select) else []


def _sources(select) -> list:
    out = []
    frm = select.args.get(_from_key())
    if frm is not None:
        out.append(frm.this)
    out += [j.this for j in select.args.get("joins") or []]
    return out


def _has_star(select) -> bool:
    return any(isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star))
               for e in select.expressions)


def _star_reaches_table(select, ctes: dict, depth: int = 0) -> bool:
    """O * do SELECT chega até uma base (e não só a uma CTE que escolhe as colunas)?"""
    if depth > 20 or not _has_star(select):
        return False
    for src in _sources(select):
        if isinstance(src, exp.Subquery):
            if any(_star_reaches_table(s, ctes, depth + 1) for s in _final_selects(src.this)):
                return True
        elif isinstance(src, exp.Table):
            nome = src.name.lower()
            if not src.db and nome in ctes:
                if any(_star_reaches_table(s, ctes, depth + 1) for s in _final_selects(ctes[nome])):
                    return True
            else:
                return True
    return False


def profile_query(sql: str) -> QueryProfile:
    """JOINs, janelas, DISTINCT... e as colunas citadas (para saber quanto da tabela é lido)."""
    p = QueryProfile()
    if sqlglot is None or not (sql or "").strip():
        return p
    try:
        statements = [s for s in sqlglot.parse(sql, read="athena") if s is not None]
    except sqlglot.errors.SqlglotError:
        return p
    colunas: set[str] = set()
    for stmt in statements:
        final = _final_query(stmt)
        if final is None:
            continue
        p.joins += sum(1 for j in stmt.find_all(exp.Join) if not isinstance(j.this, (exp.Unnest, exp.Lateral)))
        p.windows += sum(1 for _ in stmt.find_all(exp.Window))
        for d in stmt.find_all(exp.Distinct):
            if isinstance(d.parent, exp.Select):
                p.select_distinct += 1
            elif isinstance(d.parent, exp.AggFunc):
                p.count_distinct += 1
        p.group_by += sum(1 for _ in stmt.find_all(exp.Group))
        p.unions += sum(1 for _ in stmt.find_all(exp.SetOperation))
        if final.args.get("order") is not None and final.args.get("limit") is None:
            p.global_order = True
        ctes = {c.alias_or_name.lower(): c.this for c in stmt.find_all(exp.CTE)}
        if any(_star_reaches_table(s, ctes) for s in _final_selects(final)):
            p.star = True
        colunas |= {c.name.lower() for c in stmt.find_all(exp.Column) if not isinstance(c.this, exp.Star)}
    p.columns = frozenset(colunas)
    return p


# --------------------------------------------------------------------------------------------
# Histórico do Athena (só leitura: nada é executado)
# --------------------------------------------------------------------------------------------
def normalize_sql(sql: str) -> str:
    """Sem comentários, espaços extras, ; no fim e diferença de maiúsculas."""
    texto = re.sub(r"\s+", " ", _COMMENTS.sub(" ", sql or "")).strip()
    return texto.rstrip(";").strip().lower()


def sql_shape(sql: str) -> str:
    """A query com os valores trocados por ? (a mesma query rodada para outra data tem a mesma forma)."""
    texto = re.sub(r"'(?:[^']|'')*'", "?", normalize_sql(sql))
    return re.sub(r"\b\d+(?:\.\d+)?\b", "0", texto)


def _run_from(qe: dict, match: str) -> AthenaRun:
    st = qe.get("Status") or {}
    stats = qe.get("Statistics") or {}
    estado = st.get("State", "")
    erro = (st.get("AthenaError") or {}).get("ErrorMessage") or st.get("StateChangeReason") or ""
    return AthenaRun(execution_id=qe.get("QueryExecutionId", ""), workgroup=qe.get("WorkGroup", ""), state=estado,
                     submitted=st.get("SubmissionDateTime"), match=match,
                     scanned_bytes=stats.get("DataScannedInBytes"), engine_ms=stats.get("EngineExecutionTimeInMillis"),
                     reused=bool((stats.get("ResultReuseInformation") or {}).get("ReusedPreviousResult")),
                     error="" if estado == "SUCCEEDED" else erro)


def _runtime_stats(athena, run: AthenaRun, notes: list[str]):
    """Linhas lidas e quanto as etapas passaram umas para as outras (≈ o shuffle no Spark)."""
    try:
        resp = athena.get_query_runtime_statistics(QueryExecutionId=run.execution_id)
    except Exception as e:
        notes.append(f"Sem as estatísticas detalhadas da execução no Athena ({_error_code(e)}): o shuffle foi "
                     "estimado pela query.")
        return
    rs = resp.get("QueryRuntimeStatistics") or {}
    linhas = rs.get("Rows") or {}
    run.input_rows, run.output_rows = linhas.get("InputRows"), linhas.get("OutputRows")
    raiz = rs.get("OutputStage")
    if raiz:
        total, etapas, pilha = 0, 0, list(raiz.get("SubStages") or [])
        while pilha:
            etapa = pilha.pop()
            total += int(etapa.get("OutputBytes") or 0)
            etapas += 1
            pilha.extend(etapa.get("SubStages") or [])
        if etapas:
            run.exchange_bytes = total


def _workgroups(athena, notes: list[str]) -> list[str]:
    try:
        nomes, token = [], None
        while len(nomes) < ATHENA_MAX_WORKGROUPS:
            resp = athena.list_work_groups(**({"NextToken": token} if token else {}))
            nomes += [w["Name"] for w in resp.get("WorkGroups") or [] if w.get("State", "ENABLED") == "ENABLED"]
            token = resp.get("NextToken")
            if not token:
                break
        return nomes[:ATHENA_MAX_WORKGROUPS] or ["primary"]
    except Exception as e:
        notes.append(f"Não consegui listar os workgroups do Athena ({_error_code(e)}): procurei só no `primary`. "
                     "Informe o workgroup na estimativa.")
        return ["primary"]


_RANK = {("exata", "SUCCEEDED"): 0, ("parecida", "SUCCEEDED"): 1, ("exata", "FAILED"): 2, ("parecida", "FAILED"): 3}


def _rank(run: AthenaRun) -> tuple:
    quando = run.submitted.timestamp() if hasattr(run.submitted, "timestamp") else 0
    return _RANK.get((run.match, run.state), 9), -quando


def _candidate(qe: dict, alvo: str, forma: str) -> AthenaRun | None:
    if qe.get("StatementType") not in (None, "DML"):
        return None
    if (qe.get("Status") or {}).get("State") not in ("SUCCEEDED", "FAILED"):
        return None
    texto = qe.get("Query") or ""
    if normalize_sql(texto) == alvo:
        match = "exata"
    elif sql_shape(texto) == forma:
        match = "parecida"
    else:
        return None
    run = _run_from(qe, match)
    return None if run.reused else run


def find_athena_run(athena, sql: str, workgroup: str = "", execution_id: str = "",
                    budget_s: float = ATHENA_BUDGET_S,
                    max_per_workgroup: int = ATHENA_MAX_PER_WORKGROUP) -> tuple[AthenaRun | None, list[str]]:
    """A execução mais útil desta query no histórico do Athena (ou a do ID informado), e observações.

    Usa só ListWorkGroups, ListQueryExecutions, BatchGetQueryExecution, GetQueryExecution e
    GetQueryRuntimeStatistics: leitura do histórico, sem rodar nada no Athena."""
    notes: list[str] = []
    execution_id = (execution_id or "").strip()
    if execution_id:
        if not _EXECUTION_ID.match(execution_id):
            notes.append(f"`{execution_id}` não parece um ID de execução do Athena (formato 1a2b3c4d-...).")
            return None, notes
        try:
            qe = athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]
        except Exception as e:
            notes.append(f"Não consegui ler a execução `{execution_id}` no Athena ({_error_code(e)}).")
            return None, notes
        run = _run_from(qe, "id")
        if not run.reused:
            _runtime_stats(athena, run, notes)
        return run, notes

    alvo, forma = normalize_sql(sql), sql_shape(sql)
    if not alvo:
        return None, notes
    deadline = time.monotonic() + budget_s
    grupos = [workgroup.strip()] if (workgroup or "").strip() else _workgroups(athena, notes)
    melhor: AthenaRun | None = None
    vistas = 0
    for wg in grupos:
        token, lidas = None, 0
        while lidas < max_per_workgroup and time.monotonic() < deadline:
            params = {"WorkGroup": wg, "MaxResults": 50}
            if token:
                params["NextToken"] = token
            try:
                resp = athena.list_query_executions(**params)
                ids = resp.get("QueryExecutionIds") or []
                execucoes = athena.batch_get_query_execution(QueryExecutionIds=ids)["QueryExecutions"] if ids else []
            except Exception as e:
                notes.append(f"Não consegui ler o histórico do workgroup `{wg}` ({_error_code(e)}).")
                break
            for qe in execucoes:
                run = _candidate(qe, alvo, forma)
                if run is not None and (melhor is None or _rank(run) < _rank(melhor)):
                    melhor = run
            lidas += len(ids)
            vistas += len(ids)
            token = resp.get("NextToken")
            if not token or not ids or (melhor is not None and _rank(melhor)[0] == 0 and melhor.workgroup == wg):
                break                        # as execuções vêm da mais nova para a mais antiga
    if time.monotonic() >= deadline:
        notes.append("A busca no histórico do Athena parou no limite de tempo.")
    if melhor is None:
        onde = ", ".join(f"`{g}`" for g in grupos)
        notes.append(f"Não achei esta query nas últimas {vistas} execuções do Athena (workgroup {onde}). Se ela já "
                     "rodou, cole o ID da execução (console do Athena → Recent queries).")
        return None, notes
    _runtime_stats(athena, melhor, notes)
    return melhor, notes


# --------------------------------------------------------------------------------------------
# Coleta e conta
# --------------------------------------------------------------------------------------------
def collect(refs, sql_athena: str, *, glue=None, s3=None, athena=None, workgroup: str = "", execution_id: str = "",
            budget_s: float = GLUE_BUDGET_S, athena_budget_s: float = ATHENA_BUDGET_S) -> SizingData:
    """Junta o que se sabe das bases (Glue/S3) e da execução no Athena. O histórico do Athena é lido em
    paralelo com o catálogo. refs: objetos com alias, database, table e filter (ex.: TableInfo da importação)."""
    profile = profile_query(sql_athena)
    notes: list[str] = []
    with ThreadPoolExecutor(max_workers=1) as pool:
        futuro = (pool.submit(find_athena_run, athena, sql_athena, workgroup, execution_id, athena_budget_s)
                  if athena is not None else None)
        deadline = time.monotonic() + budget_s
        medidor = S3Meter(s3) if s3 is not None else None
        tabelas = []
        for ref in refs:
            if glue is None:
                tabelas.append(TableSize(alias=ref.alias, database=ref.database, table=ref.table,
                                         filter=getattr(ref, "filter", "") or ""))
            else:
                tabelas.append(collect_table(glue, ref, profile, medidor, deadline))
        run = None
        if futuro is not None:
            try:
                run, notas = futuro.result()
                notes.extend(notas)
            except Exception as e:                # find_athena_run já trata os erros da AWS
                notes.append(f"Não consegui ler o histórico do Athena ({_error_code(e)}).")
    return SizingData(tabelas, run, profile, notes)


def estimate(data: SizingData, reader: str = "spark", target_s: float = DEFAULT_TARGET_S) -> Estimate:
    """Tipo e número de workers para processar a query em ~target_s segundos.

    reader: "spark" (leitura nativa: só as colunas usadas), "glue_context" (Glue 4.0 com Lake Formation:
    tenta a leitura nativa e, se ela falhar, o DynamicFrame; a estimativa do DynamicFrame vem em
    `alternative`) ou "dynamic_frame" (todas as colunas, mais devagar)."""
    est = Estimate()
    tabelas, run, perfil = data.tables, data.athena, data.profile
    dyf = reader == "dynamic_frame"
    conhecidas = [t for t in tabelas if t.bytes is not None]
    sem_tamanho = [t for t in tabelas if t.bytes is None]
    completo = bool(tabelas) and not sem_tamanho
    cheio_conhecido = sum(t.bytes for t in conhecidas)          # todas as colunas
    nativo_conhecido = sum(t.read_bytes for t in conhecidas)    # só as colunas usadas
    run_ok = run is not None and run.usable
    run_exato = run_ok and run.match in ("id", "exata")

    # 1) leitura nativa (só as colunas usadas)
    nativo, fonte = None, ""
    if run_exato:
        nativo, fonte = run.scanned_bytes, "a mesma query no Athena"
    elif completo:
        nativo, fonte = nativo_conhecido, "partições escolhidas × colunas usadas"
    elif run_ok:
        nativo, fonte = run.scanned_bytes, "a mesma query no Athena com outros valores"
    elif conhecidas:
        nativo, fonte = nativo_conhecido, "só as bases com tamanho conhecido"
    elif run is not None and run.scanned_bytes:
        nativo, fonte = run.scanned_bytes, "execução que falhou no Athena: parcial"

    # tamanho inteiro dos arquivos (todas as colunas): é o que o DynamicFrame lê e o que define os pedaços em que o
    # Spark divide a leitura. O que o Athena leu além das bases com tamanho conhecido é das bases sem tamanho; para
    # elas, o tamanho inteiro sai das colunas que a query usa.
    if completo or nativo is None:
        cheio = cheio_conhecido if conhecidas else None
    else:
        resto = max(0, nativo - nativo_conhecido)
        fracoes = [t.read_fraction for t in sem_tamanho if t.columns_total]
        if fracoes:
            cheio = cheio_conhecido + int(resto / (sum(fracoes) / len(fracoes)))
        else:
            cheio = cheio_conhecido + resto if conhecidas else None

    if dyf:
        lido = cheio
        fonte = ("partições escolhidas, todas as colunas" if completo else
                 "Athena e colunas das tabelas, todas as colunas" if run_ok else
                 "só as bases com tamanho conhecido, todas as colunas")
    else:
        lido = nativo
    if lido is None:
        est.reasons.append("Não encontrei o tamanho de nenhuma base, então não dá para estimar.")
        est.notes.append(HOW_TO_IMPROVE)
        return est

    # 2) quanto as etapas trocam entre si (shuffle)
    if run_ok and run.exchange_bytes is not None and (run_exato or not completo):
        shuffle, fonte_shuffle = run.exchange_bytes, "Athena"
    else:
        shuffle = int((nativo if nativo is not None else lido) * EXPANSION * SHUFFLE_FACTOR[perfil.weight])
        fonte_shuffle = f"estimado pela query {perfil.weight}"

    # 3) trabalho e núcleos
    tarefas = max(1, math.ceil(max(cheio or 0, lido) / SPLIT_BYTES))
    trabalho = (lido / (READ_RATE_DYNAMIC_FRAME if dyf else READ_RATE) + tarefas * TASK_COST_S
                + shuffle / SHUFFLE_RATE)
    paralelismo = max(tarefas, math.ceil(shuffle / SPLIT_BYTES), 4)
    pedidos = math.ceil(trabalho / target_s)
    nucleos = max(1, min(pedidos, paralelismo))

    # 4) tipo de worker
    inicio, motivos = 0, []
    particoes = sum(t.partitions or 0 for t in tabelas)
    arquivos = sum(t.files or 0 for t in tabelas)
    if perfil.weight == "pesada" and lido > 20 * GiB:
        inicio = 1
        motivos.append("query pesada com bastante dado: o G.2X tem o dobro de memória por executor e no driver")
    if particoes > 10_000 or arquivos > 100_000:
        inicio = max(inicio, 1)
        motivos.append(f"{fmt_count(max(particoes, arquivos))} partições/arquivos: o driver precisa de mais memória "
                       "para listar e planejar")
    if shuffle > 2 * TiB or lido > 4 * TiB:
        inicio = max(inicio, 2)
        motivos.append("volume muito grande: o G.4X tem mais disco por núcleo para o shuffle")
    escolha = None
    for tipo in TYPES[inicio:]:
        vcpu = GLUE_WORKER_TYPES[tipo].vcpu
        por_cpu = math.ceil(nucleos / vcpu)
        por_disco = math.ceil(SHUFFLE_DISK_RATIO * shuffle / (DISK_USE * FREE_DISK_GB[tipo] * GiB))
        workers = max(MIN_WORKERS, max(por_cpu, por_disco) + 1)
        if workers <= MAX_WORKERS:
            escolha = (tipo, workers, por_disco > por_cpu)
            break
        motivos.append(f"com {tipo} seriam mais de {MAX_WORKERS} workers")
    if escolha is None:
        escolha = (TYPES[-1], MAX_WORKERS, False)
        est.notes.append(f"Mesmo com {TYPES[-1]} × {MAX_WORKERS} a query deve passar do tempo desejado: filtre menos "
                         "partições ou rode como job do Glue.")
    tipo, workers, pelo_disco = escolha
    vcpu = GLUE_WORKER_TYPES[tipo].vcpu
    executores = workers - 1

    est.worker_type, est.num_workers = tipo, workers
    est.read_bytes, est.read_source = lido, fonte
    est.shuffle_bytes, est.shuffle_source = shuffle, fonte_shuffle
    est.tasks = tarefas
    est.seconds = trabalho / (executores * vcpu) + OVERHEAD_S

    # 5) partições do shuffle: com o padrão (200), um shuffle grande vira tarefas enormes que vão para o disco
    if shuffle > DEFAULT_SHUFFLE_PARTITIONS * SPLIT_BYTES:
        nucleos_total = executores * vcpu
        n = math.ceil(math.ceil(shuffle / SPLIT_BYTES) / nucleos_total) * nucleos_total
        est.shuffle_partitions = min(n, MAX_SHUFFLE_PARTITIONS)

    # 6) confiança
    if dyf:
        est.confidence = "média" if completo else "baixa"
    elif run_exato:
        est.confidence = "alta"
    elif completo:
        est.confidence = "média"
    else:
        est.confidence = "baixa"

    linha_workers = f"{workers} workers = {executores} executor(es) de {vcpu} núcleos + 1 worker para o driver"
    if pelo_disco:
        linha_workers += "; o shuffle precisa caber no disco dos workers"
    if pedidos > paralelismo and workers > MIN_WORKERS:
        linha_workers += "; mais workers não aceleram, porque os dados não rendem mais tarefas em paralelo"
    est.reasons = [
        f"Leitura de **~{fmt_bytes(lido)}** ({fonte})"
        + (f", em ~{fmt_count(tarefas)} pedaços de arquivo" if tarefas > 1 else "") + ".",
        f"Troca entre etapas (shuffle) de **~{fmt_bytes(shuffle)}** ({fonte_shuffle}).",
        f"Query {perfil.weight}" + (f": {perfil.describe()}" if perfil.describe() else "") + ".",
        *[m[0].upper() + m[1:] + "." for m in motivos],
        linha_workers + ".",
    ]
    if sem_tamanho and not run_exato:
        nomes = ", ".join(f"`{t.alias}`" for t in sem_tamanho)
        est.notes.append(f"Sem o tamanho de {nomes}: a estimativa pode ficar abaixo do necessário.")
    if run_ok and run.match == "parecida" and not completo and not dyf:
        est.notes.append("A execução achada no Athena tem outros valores (outra data, por exemplo): o volume desta "
                         "pode ser diferente.")
    if reader == "glue_context":
        alternativa = estimate(data, "dynamic_frame", target_s)
        est.alternative = alternativa if alternativa.ok else None
    return est
