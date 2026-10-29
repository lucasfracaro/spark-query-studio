"""
Gera o código PySpark que será executado no motor (Glue Interactive Session ou Spark local).

- O código é legível: cada comando da query aparece como texto Python normal, então dá para
  copiar e rodar num notebook do Glue. Só quando o código passaria do limite do Glue
  (68 mil caracteres por comando) a query e as configurações vão comprimidas (zlib + base64).
- Cada execução usa uma sessão Spark nova (spark.newSession()): configurações, database
  atual e views de execuções anteriores não passam para a próxima, como num job novo.
- O resultado volta como uma linha marcada com RESULT_MARKER, com o JSON comprimido em base64.
"""
from __future__ import annotations

import base64
import binascii
import json
import pprint
import re
import zlib
from dataclasses import asdict, dataclass, field

RESULT_MARKER = "__SQS_RESULT__"
DEFAULT_LIMIT = 100
GLUE_CODE_LIMIT = 68_000   # tamanho máximo do campo Code do RunStatement (API do Glue)


@dataclass
class InputSource:
    """Uma entrada de dados que vira uma temp view no Spark."""
    alias: str                 # nome da view usada na query
    kind: str = "catalog"      # "catalog" (Glue Data Catalog / Athena) ou "s3"
    source: str = ""           # "database.tabela" ou "s3://bucket/caminho/"
    fmt: str = "parquet"       # parquet | csv | json | orc | delta | iceberg
    options: str = ""          # "header=true;sep=;" (só para s3)
    filter: str = ""           # filtro opcional aplicado antes de registrar (ex: "dt >= '2026-10-01'")
    cache: bool = False        # cachear a view

    def is_valid(self) -> bool:
        return bool(self.alias and self.alias.strip() and self.source and self.source.strip())


@dataclass
class Orchestration:
    inputs: list[InputSource] = field(default_factory=list)
    spark_conf: dict[str, str] = field(default_factory=dict)
    default_database: str = ""
    limit: int = DEFAULT_LIMIT


# --------------------------------------------------------------------------------------------
# Divisão da query em comandos
# --------------------------------------------------------------------------------------------
def split_statements(sql: str) -> list[str]:
    """Separa a query em comandos pelos ';' que estão fora de strings e comentários.

    O texto de cada comando é mantido como foi escrito, inclusive comentários e hints do Spark
    (/*+ BROADCAST(t) */). Trechos que só têm comentários ou espaços são descartados.
    """
    return [texto for _linha, texto in split_statements_with_lines(sql)]


def split_statements_with_lines(sql: str) -> list[tuple[int, str]]:
    """Como split_statements, mas devolve também a linha (1, 2, ...) onde cada comando começa."""
    statements: list[tuple[int, str]] = []

    def add(a: int, b: int):
        trecho = sql[a:b]
        inicio = a + (len(trecho) - len(trecho.lstrip()))
        statements.append((sql.count("\n", 0, inicio) + 1, trecho.strip()))

    start, i, n = 0, 0, len(sql)
    has_code = False
    in_single = in_double = in_backtick = False
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if in_single or in_double:
            if ch == "\\" and nxt:                       # Spark aceita escape com barra: 'it\'s'
                i += 2
                continue
            if (ch == "'" and in_single) or (ch == '"' and in_double):
                in_single = in_double = False            # 'it''s' = fecha e reabre: funciona igual
            i += 1
            continue
        if in_backtick:
            if ch == "`":
                in_backtick = False
            i += 1
            continue
        if ch == "-" and nxt == "-":                     # comentário de linha
            end = sql.find("\n", i)
            i = n if end == -1 else end
            continue
        if ch == "/" and nxt == "*":                     # comentário de bloco (e hints /*+ */)
            end = sql.find("*/", i + 2)
            i = n if end == -1 else end + 2
            continue
        if ch == ";":
            if has_code:
                add(start, i)
            start, has_code = i + 1, False
            i += 1
            continue
        if ch == "'":
            in_single = True
        elif ch == '"':
            in_double = True
        elif ch == "`":
            in_backtick = True
        if not ch.isspace():
            has_code = True
        i += 1
    if has_code:
        add(start, n)
    return statements


# --------------------------------------------------------------------------------------------
# Opções de leitura e validação da orquestração
# --------------------------------------------------------------------------------------------
def parse_options(texto: str) -> dict[str, str]:
    """Converte "chave=valor;chave2=valor2" em dict.

    O próprio ";" pode ser valor: "header=true;sep=;" -> {"header": "true", "sep": ";"}.
    Regra: um trecho sem "=" logo depois de uma chave com valor vazio vira o valor ";".
    """
    opcoes: dict[str, str] = {}
    ultima = None
    for trecho in (texto or "").split(";"):
        if "=" in trecho:
            k, v = trecho.split("=", 1)
            ultima = k.strip()
            if ultima:
                opcoes[ultima] = v.strip()
        elif ultima and opcoes.get(ultima) == "":
            opcoes[ultima] = ";" + trecho.strip()
            ultima = None
    return {k: v for k, v in opcoes.items() if k}


_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_orchestration(orch: Orchestration) -> list[str]:
    errors = []
    seen = set()
    for inp in orch.inputs:
        if not inp.is_valid():
            continue
        if not _ALIAS_RE.match(inp.alias.strip()):
            errors.append(f"Alias inválido '{inp.alias}': use letras, números e _ (sem começar com número).")
        if inp.alias.lower() in seen:
            errors.append(f"Alias duplicado: '{inp.alias}'.")
        seen.add(inp.alias.lower())
        if inp.kind == "s3" and not inp.source.strip().startswith(("s3://", "s3a://", "file:/", "/")):
            errors.append(f"Entrada '{inp.alias}': caminho deve começar com s3:// (ou ser um caminho local).")
        if inp.kind == "catalog" and "." not in inp.source and not orch.default_database:
            errors.append(f"Entrada '{inp.alias}': use 'database.tabela' ou defina um database padrão.")
    if not (1 <= int(orch.limit) <= 10000):
        errors.append("Limite de linhas deve estar entre 1 e 10000.")
    return errors


# --------------------------------------------------------------------------------------------
# Geração do código
# --------------------------------------------------------------------------------------------
def py_literal(text: str) -> str:
    """Literal Python para um texto: string bruta de três aspas (legível) quando é seguro, senão repr()."""
    if "'''" not in text and not text.endswith(("'", "\\")) and "\r" not in text and "\x00" not in text:
        return "r'''" + text + "'''"
    return repr(text)


def _payload_readable(statements: list[str], payload: dict) -> str:
    comandos = "".join(f"    {py_literal(s)},\n" for s in statements)
    return ("# Comandos da query, na ordem em que rodam\n"
            f"_statements = [\n{comandos}]\n\n"
            "# Orquestração: entradas, configurações Spark, database padrão e opções\n"
            f"_p = {pprint.pformat(payload, width=110, sort_dicts=False)}\n")


def _payload_compressed(statements: list[str], payload: dict) -> str:
    raw = json.dumps(dict(payload, statements=statements), ensure_ascii=False).encode("utf-8")
    blob = base64.b64encode(zlib.compress(raw, 9)).decode("ascii")
    return ("# A query e a orquestração vão comprimidas porque o código passaria do limite do Glue\n"
            f'_p = _json.loads(_zlib.decompress(_b64.b64decode("{blob}")).decode("utf-8"))\n'
            '_statements = _p.pop("statements")\n')


_TEMPLATE = r'''
import base64 as _b64, json as _json, time as _time, traceback as _tb, zlib as _zlib

#__PAYLOAD__
_t0 = _time.time()
_log = []


def _emit(obj):
    _raw = _json.dumps(obj, default=str).encode("utf-8")
    print(_p["marker"] + _b64.b64encode(_zlib.compress(_raw, 6)).decode("ascii"))


try:
    try:
        spark                       # no Glue (e no modo local) a sessão já existe
    except NameError:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

    # Sessão Spark nova a cada execução: configurações, database atual e views de execuções
    # anteriores não passam para esta, como num job novo. O cache em memória também é limpo.
    _s = spark.newSession()
    _s.catalog.clearCache()
    for _row in spark.sql("SET").collect():      # configurações iniciais (ex.: as do Glue)
        try:
            if _s.conf.get(_row[0], None) != _row[1]:
                _s.conf.set(_row[0], _row[1])
        except Exception:
            pass

    for _k, _v in _p["conf"].items():
        try:
            _s.conf.set(_k, _v)
            _log.append("conf " + _k + "=" + str(_v))
        except Exception as _e:
            _log.append("AVISO: conf " + _k + " não aplicada (" + str(_e).splitlines()[0] + ")")

    if _p["db"]:
        _s.sql("USE " + _p["db"])
        _log.append("USE " + _p["db"])

    for _inp in _p["inputs"]:
        _alias, _src = _inp["alias"].strip(), _inp["source"].strip()
        if _inp["kind"] == "catalog":
            _df = _s.table(_src)
        else:
            _r = _s.read.format((_inp.get("fmt") or "parquet").lower())
            for _ok, _ov in (_inp.get("options") or {}).items():
                _r = _r.option(_ok, _ov)
            _df = _r.load(_src)
        if (_inp.get("filter") or "").strip():
            _df = _df.where(_inp["filter"])
        _df.createOrReplaceTempView(_alias)
        if _inp.get("cache"):
            _s.catalog.cacheTable(_alias)
        _log.append("view " + _alias + " <- " + _src)

    # comandos (CREATE VIEW, SET, CACHE...) rodam na hora; um SELECT só roda quando é o último
    _df = None
    for _cmd in _statements:
        _df = _s.sql(_cmd)

    _fields = _df.schema.fields
    _cols = [{"name": _f.name, "type": _f.dataType.simpleString(), "nullable": bool(getattr(_f, "nullable", True))}
             for _f in _fields]
    _rows = []
    for _row in _df.limit(_p["limit"]).collect():
        _rows.append([None if _x is None else (_x if isinstance(_x, (int, float, bool, str)) else str(_x)) for _x in _row])
    _out = {"ok": True, "columns": _cols, "rows": _rows, "elapsed": round(_time.time() - _t0, 2), "log": _log}

    if _p["profile"] and _fields:
        # estatísticas no resultado COMPLETO (uma agregação só, colunas renomeadas por posição)
        from pyspark.sql import functions as _F
        _simples = ("string", "int", "bigint", "smallint", "tinyint", "double", "float", "boolean",
                    "date", "timestamp", "timestamp_ntz", "decimal", "varchar", "char")
        _dfp = _df.toDF(*["c%d" % _i for _i in range(len(_fields))])
        _aggs = [_F.count(_F.lit(1)).alias("__n")]
        for _i, _f in enumerate(_fields):
            _c = _F.col("c%d" % _i)
            _aggs.append(_F.count(_c).alias("n%d" % _i))
            if _f.dataType.simpleString().split("(")[0] in _simples:
                _aggs += [_F.approx_count_distinct(_c).alias("d%d" % _i), _F.min(_c).alias("mn%d" % _i),
                          _F.max(_c).alias("mx%d" % _i)]
        _a = _dfp.agg(*_aggs).collect()[0].asDict()

        def _txt(_v):
            return None if _v is None else (_v if isinstance(_v, (int, float, bool)) else str(_v))

        _out["profile"] = {"rows": _a["__n"], "columns": [
            {"nulls": _a["__n"] - _a["n%d" % _i], "distinct": _a.get("d%d" % _i),
             "min": _txt(_a.get("mn%d" % _i)), "max": _txt(_a.get("mx%d" % _i))}
            for _i in range(len(_fields))]}
        _log.append("estatísticas calculadas em " + str(_a["__n"]) + " linhas")

    _emit(_out)
except Exception as _e:
    _emit({"ok": False, "error": type(_e).__name__ + ": " + str(_e), "traceback": _tb.format_exc(),
           "elapsed": round(_time.time() - _t0, 2), "log": _log})
'''.strip()


def build_spark_code(sql: str, orch: Orchestration, profile: bool = False) -> str:
    """Monta o script PySpark que cria as entradas, roda a query e imprime o resultado.

    O código mostrado na tela é exatamente o que roda. Se a versão legível passar do limite do
    Glue, a query vai comprimida; se nem assim couber, levanta ValueError.
    """
    statements = split_statements((sql or "").replace("\r\n", "\n"))
    if not statements:
        raise ValueError("A query está vazia.")

    payload = {
        "inputs": [dict(asdict(i), options=parse_options(i.options)) for i in orch.inputs if i.is_valid()],
        "conf": {k: v for k, v in orch.spark_conf.items() if k and str(k).strip()},
        "db": orch.default_database.strip(),
        "limit": int(orch.limit),
        "profile": bool(profile),
        "marker": RESULT_MARKER,
    }
    code = _TEMPLATE.replace("#__PAYLOAD__", _payload_readable(statements, payload))
    if len(code) > GLUE_CODE_LIMIT:
        code = _TEMPLATE.replace("#__PAYLOAD__", _payload_compressed(statements, payload))
    if len(code) > GLUE_CODE_LIMIT:
        raise ValueError(f"A query é grande demais para o Glue: mesmo comprimido, o código tem "
                         f"{len(code):,} caracteres e o limite é {GLUE_CODE_LIMIT:,}.".replace(",", "."))
    return code


def parse_result(output_text: str) -> dict | None:
    """Encontra a última linha marcada no stdout e decodifica o JSON (comprimido ou não)."""
    if not output_text:
        return None
    for line in reversed(output_text.splitlines()):
        idx = line.find(RESULT_MARKER)
        if idx != -1:
            raw = line[idx + len(RESULT_MARKER):].strip()
            try:
                data = base64.b64decode(raw)
                try:
                    data = zlib.decompress(data)
                except zlib.error:
                    pass                                   # formato antigo, sem compressão
                return json.loads(data.decode("utf-8"))
            except (ValueError, UnicodeDecodeError, binascii.Error):
                # saída truncada ou corrompida: o chamador trata como "sem resultado"
                return None
    return None
