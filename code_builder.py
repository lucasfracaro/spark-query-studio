"""
Gera o código PySpark que será executado no motor (Glue Interactive Session ou Spark local).

- O código é legível: cada comando da query aparece como texto Python normal, então dá para
  copiar e rodar num notebook do Glue. Só quando o código passaria do limite do Glue
  (68 mil caracteres por comando) a query e as configurações vão comprimidas (zlib + base64).
- Cada execução usa uma sessão Spark nova (spark.newSession()): configurações, database
  atual e views de execuções anteriores não passam para a próxima, como num job novo.
- O cache continua entre as execuções: entradas marcadas com "cache" (e CACHE TABLE da própria
  query) não são relidas do S3 enquanto a definição não mudar. O que sai da orquestração é liberado.
- reader="glue_context" (Glue 4.0 com Lake Formation): as entradas do catálogo são lidas pelo GlueContext,
  porque no Glue 4.0 só essa leitura usa as permissões do Lake Formation (dados no S3 de outra conta).
  Primeiro a leitura nativa do Spark (create_data_frame.from_catalog com useSparkDataSource, que lê só as
  colunas usadas); se ela não funcionar, o DynamicFrame (create_dynamic_frame.from_catalog), mais lento.
- O resultado volta como uma linha marcada com RESULT_MARKER, com o JSON comprimido em base64.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import pprint
import re
import zlib
from dataclasses import asdict, dataclass, field

RESULT_MARKER = "__SQS_RESULT__"
DEFAULT_LIMIT = 100
GLUE_CODE_LIMIT = 68_000   # tamanho máximo do campo Code do RunStatement (API do Glue)
READERS = ("spark", "glue_context")


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


_TEMPLATE = r"""
import base64 as _b64, json as _json, time as _time, traceback as _tb, zlib as _zlib
from decimal import Decimal as _Decimal

#__PAYLOAD__
_t0 = _time.time()
_log = []


def _emit(obj):
    _raw = _json.dumps(obj, default=str).encode("utf-8")
    print(_p["marker"] + _b64.b64encode(_zlib.compress(_raw, 6)).decode("ascii"))


def _valor(_x):
    if _x is None or isinstance(_x, (int, float, bool, str)):
        return _x
    if isinstance(_x, _Decimal):
        return format(_x, "f")              # "1.50" (e zero com casas decimais não vira "0E-10")
    return str(_x)


try:
    try:
        spark                       # no Glue (e no modo local) a sessão já existe
    except NameError:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

    # Sessão Spark nova a cada execução: configurações, database atual e views de execuções
    # anteriores não passam para esta, como num job novo.
    _s = spark.newSession()
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

    # Cache entre execuções: o que foi marcado com "cache" continua em memória na sessão e não é relido
    # do S3 enquanto a definição não mudar. O que saiu da orquestração (ou da query) é liberado.
    _cache = globals().setdefault("_sqs_cache", {})        # {chave: [DataFrame, hora da leitura, nome]}
    for _chave in [_c for _c in _cache if _c not in _p["keep_cached"]]:
        try:
            _cache[_chave][0].unpersist()
            spark.catalog.dropGlobalTempView("sqs_" + _chave[2:])
        except Exception:
            pass
        _log.append("cache liberado: " + _cache.pop(_chave)[2])

    def _ler_pelo_glue_context(_src, _filtro, _partes, _alias):
        # Glue 4.0 com Lake Formation: só as leituras pelo GlueContext usam as permissões do Lake Formation.
        # 1º) leitura nativa do Spark (useSparkDataSource): lê só as colunas que a query usa e filtra dentro
        #     dos arquivos. 2º) se ela não funcionar, DynamicFrame, como nos jobs do Glue (lê todas as
        #     colunas das partições escolhidas: bem mais lento).
        # O push_down_predicate só aceita colunas de partição: as partições da tabela são conferidas no
        # catálogo e só a parte do filtro que usa apenas elas vai para a leitura; o filtro inteiro é aplicado
        # depois, então o resultado é o mesmo.
        _gc = globals().get("_sqs_gc")
        if _gc is None:
            from awsglue.context import GlueContext
            _gc = globals()["_sqs_gc"] = GlueContext(spark.sparkContext)
        _db, _tabela = _src.split(".", 1) if "." in _src else ((_p["db"] or "default"), _src)
        _t, _nativa_falhou, _depois = _time.time(), "", []
        _push = _filtro
        if _filtro and _partes and all(_x["cols"] is not None for _x in _partes):
            try:
                _part = {_c.name.lower() for _c in spark.catalog.listColumns(_tabela, _db) if _c.isPartition}
            except Exception:
                _part = None
            if _part is not None:
                _usar = [_x["sql"] for _x in _partes if _x["cols"] and set(_x["cols"]) <= _part]
                _depois = [_x["sql"] for _x in _partes if not (_x["cols"] and set(_x["cols"]) <= _part)]
                _push = _usar[0] if len(_usar) == 1 else " AND ".join("(" + _x + ")" for _x in _usar)
        try:
            _d = _gc.create_data_frame.from_catalog(database=_db, table_name=_tabela, push_down_predicate=_push,
                                                    additional_options={"useSparkDataSource": True})
            _d.limit(1).collect()              # a leitura é preguiçosa: confere agora o acesso aos arquivos
        except Exception as _e:
            _d, _nativa_falhou = None, (str(_e).strip().splitlines() or [""])[0][:200]
        if _d is None:
            try:
                _d = _gc.create_dynamic_frame.from_catalog(database=_db, table_name=_tabela,
                                                           push_down_predicate=_push).toDF()
            except Exception as _e:
                if not _push:
                    raise
                raise RuntimeError(
                    "O filtro da entrada " + _alias + " (" + _push + ") não pôde ser usado para escolher as "
                    "partições de " + _src + ". Na leitura pelo GlueContext, a parte do filtro que vai para a "
                    "leitura deve usar só colunas de partição; as outras condições vão na query. Erro do Glue: "
                    + (str(_e).strip().splitlines() or [""])[0][:300]) from _e
        try:
            _rdd = "LogicalRDD" in _d._jdf.queryExecution().analyzed().toString()
        except Exception:
            _rdd = _nativa_falhou != ""
        _como = ("DynamicFrame: lê todas as colunas, mais lento" if _rdd
                 else "leitura nativa do Spark: só as colunas usadas")
        if _push:
            _onde = " (partições: " + _push + ")"
        else:
            _onde = " (SEM filtro de partição: a tabela inteira)"
        if _depois:
            _onde += " (aplicado depois da leitura: " + " AND ".join(_depois) + ")"
        _log.append("leitura de " + _alias + " pelo GlueContext (" + _como + ") preparada em "
                    + str(round(_time.time() - _t, 1)) + "s" + _onde)
        if _nativa_falhou:
            _log.append("a leitura nativa não funcionou para " + _alias + " (" + _nativa_falhou + ")")
        return _d.where(_filtro) if _filtro else _d

    def _para_esta_sessao(_d, _nome):
        # o GlueContext cria o DataFrame em outra sessão do Spark; uma view global deixa a sessão desta
        # execução usar o mesmo plano, sem perder a leitura só das colunas e partições necessárias
        _d.createOrReplaceGlobalTempView(_nome)
        return _s.table(_s.conf.get("spark.sql.globalTempDatabase", "global_temp") + "." + _nome)

    for _inp in _p["inputs"]:
        _alias, _src = _inp["alias"].strip(), _inp["source"].strip()
        _filtro = (_inp.get("filter") or "").strip()
        _chave, _cachear, _reaproveitado = _inp["key"], bool(_inp.get("cache")), False
        if _inp["kind"] == "catalog" and _p["reader"] == "glue_context":
            if _cachear and _chave in _cache:
                _base, _reaproveitado = _cache[_chave][0], True
            else:
                _base = _ler_pelo_glue_context(_src, _filtro, _inp.get("filter_parts") or [], _alias)
                if _cachear:
                    _base.cache()
                    _cache[_chave] = [_base, _time.strftime("%H:%M:%S"), _alias]
            _df = _para_esta_sessao(_base, "sqs_" + _chave[2:])
        else:
            if _inp["kind"] == "catalog":
                _df = _s.table(_src)
            else:
                _r = _s.read.format((_inp.get("fmt") or "parquet").lower())
                for _ok, _ov in (_inp.get("options") or {}).items():
                    _r = _r.option(_ok, _ov)
                _df = _r.load(_src)
            if _filtro:
                _df = _df.where(_filtro)
            if _cachear:
                # o cache do Spark vale para todas as sessões: se esta leitura é igual à de uma execução
                # anterior, os dados já estão em memória
                _nivel = _df.storageLevel
                _reaproveitado = bool(_nivel.useMemory or _nivel.useDisk or _nivel.useOffHeap)
                if not _reaproveitado:
                    if _chave in _cache:
                        try:
                            _cache.pop(_chave)[0].unpersist()
                        except Exception:
                            pass
                    _df.cache()
                if _chave not in _cache:
                    _cache[_chave] = [_df, _time.strftime("%H:%M:%S") if not _reaproveitado else "antes", _alias]
        _df.createOrReplaceTempView(_alias)
        _msg = "view " + _alias + " <- " + _src
        if _cachear:
            _msg += (" (cache reaproveitado: dados lidos " + ("às " if _cache[_chave][1] != "antes" else "")
                     + _cache[_chave][1] + "; para reler, use Limpar cache)" if _reaproveitado
                     else " (cache: lido agora e mantido para as próximas execuções)")
        elif _inp["kind"] == "catalog" and _p["reader"] == "glue_context":
            _msg += " (dica: marque Cache para esta leitura acontecer uma vez só por sessão)"
        _log.append(_msg)

    # comandos (CREATE VIEW, SET, CACHE...) rodam na hora; um SELECT só roda quando é o último
    _df = None
    for _cmd in _statements:
        _df = _s.sql(_cmd)

    # CACHE TABLE da própria query: os dados ficam em memória enquanto o comando continuar na query
    for _c in _p["sql_caches"]:
        if _c["key"] not in _cache:
            try:
                _cache[_c["key"]] = [_s.table(_c["name"]), _time.strftime("%H:%M:%S"), _c["name"]]
            except Exception:
                pass

    _fields = _df.schema.fields
    _cols = [{"name": _f.name, "type": _f.dataType.simpleString(), "nullable": bool(getattr(_f, "nullable", True))}
             for _f in _fields]
    _rows = [[_valor(_x) for _x in _row] for _row in _df.limit(_p["limit"]).collect()]
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
        _out["profile"] = {"rows": _a["__n"], "columns": [
            {"nulls": _a["__n"] - _a["n%d" % _i], "distinct": _a.get("d%d" % _i),
             "min": _valor(_a.get("mn%d" % _i)), "max": _valor(_a.get("mx%d" % _i))}
            for _i in range(len(_fields))]}
        _log.append("estatísticas calculadas em " + str(_a["__n"]) + " linhas")

    _emit(_out)
except Exception as _e:
    _emit({"ok": False, "error": type(_e).__name__ + ": " + str(_e), "traceback": _tb.format_exc(),
           "elapsed": round(_time.time() - _t0, 2), "log": _log})
""".strip()


def _key(*partes) -> str:
    texto = json.dumps(partes, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(texto.encode("utf-8")).hexdigest()[:12]


def input_cache_key(inp: InputSource, default_database: str = "", reader: str = "spark") -> str:
    """Identifica o que é lido por uma entrada: se não mudar, o cache da execução anterior serve.
    O alias não entra: renomear a view não obriga a reler os dados."""
    if inp.kind == "s3":
        ident = ("s3", inp.source.strip(), (inp.fmt or "parquet").lower(), parse_options(inp.options))
    else:
        db = "" if "." in inp.source else default_database.strip()
        ident = ("catalog", inp.source.strip().lower(), db.lower(), reader)
    return "e:" + _key(*ident, inp.filter.strip())


_LEADING_COMMENTS_RE = re.compile(r"^(\s+|--[^\n]*(\n|$)|/\*.*?\*/)+", re.S)
_CACHE_RE = re.compile(r"CACHE\s+(?:LAZY\s+)?TABLE\s+((?:`[^`]+`|[\w$]+)(?:\.(?:`[^`]+`|[\w$]+))*)", re.I)


def sql_cache_targets(statements: list[str]) -> list[dict]:
    """Tabelas/views que a própria query coloca em cache (CACHE TABLE ...)."""
    alvos = []
    for comando in statements:
        texto = _LEADING_COMMENTS_RE.sub("", comando)
        m = _CACHE_RE.match(texto)
        if m:
            alvos.append({"key": "q:" + _key(" ".join(texto.split())), "name": m.group(1)})
    return alvos


def filter_parts(texto: str) -> list[dict]:
    """Separa o filtro de uma entrada pelos AND de primeiro nível: [{"sql": trecho, "cols": colunas}].

    Na leitura pelo GlueContext, só os trechos cujas colunas são todas de partição vão para o
    push_down_predicate (a lista de partições é conferida no catálogo, na hora). cols=None quando não
    dá para analisar o trecho: aí o filtro inteiro é tentado como está.
    """
    texto = (texto or "").strip()
    if not texto:
        return []
    try:
        import sqlglot
        from sqlglot import exp

        from sql_refs import quiet_sqlglot
        with quiet_sqlglot():
            arvore = sqlglot.parse_one(texto, read="spark")
    except Exception:
        return [{"sql": texto, "cols": None}]
    partes = list(arvore.flatten()) if isinstance(arvore, exp.And) else [arvore]   # flatten tira os parênteses
    saida = []
    for parte in partes:
        while isinstance(parte, exp.Paren):
            parte = parte.this
        analisavel = parte.find(exp.Select) is None
        cols = sorted({c.name.lower() for c in parte.find_all(exp.Column)}) if analisavel else None
        trecho = parte.sql(dialect="spark")
        saida.append({"sql": f"({trecho})" if isinstance(parte, exp.Or) else trecho, "cols": cols})
    return saida


def build_spark_code(sql: str, orch: Orchestration, profile: bool = False, reader: str = "spark") -> str:
    """Monta o script PySpark que cria as entradas, roda a query e imprime o resultado.

    O código mostrado na tela é exatamente o que roda. Se a versão legível passar do limite do
    Glue, a query vai comprimida; se nem assim couber, levanta ValueError.
    reader="glue_context": entradas do catálogo lidas pelo GlueContext (Glue 4.0 com Lake Formation).
    """
    if reader not in READERS:
        raise ValueError(f"Leitor desconhecido: {reader}")
    statements = split_statements((sql or "").replace("\r\n", "\n"))
    if not statements:
        raise ValueError("A query está vazia.")

    db = orch.default_database.strip()
    inputs = [dict(asdict(i), options=parse_options(i.options), key=input_cache_key(i, db, reader))
              for i in orch.inputs if i.is_valid()]
    if reader == "glue_context":
        for i in inputs:
            if i["kind"] == "catalog" and i["filter"].strip():
                i["filter_parts"] = filter_parts(i["filter"])
    caches_sql = sql_cache_targets(statements)
    payload = {
        "inputs": inputs,
        "conf": {k: v for k, v in orch.spark_conf.items() if k and str(k).strip()},
        "db": db,
        "limit": int(orch.limit),
        "profile": bool(profile),
        "reader": reader,
        "keep_cached": [i["key"] for i in inputs if i["cache"]] + [c["key"] for c in caches_sql],
        "sql_caches": caches_sql,
        "marker": RESULT_MARKER,
    }
    code = _TEMPLATE.replace("#__PAYLOAD__", _payload_readable(statements, payload))
    if len(code) > GLUE_CODE_LIMIT:
        code = _TEMPLATE.replace("#__PAYLOAD__", _payload_compressed(statements, payload))
    if len(code) > GLUE_CODE_LIMIT:
        raise ValueError(f"A query é grande demais para o Glue: mesmo comprimido, o código tem "
                         f"{len(code):,} caracteres e o limite é {GLUE_CODE_LIMIT:,}.".replace(",", "."))
    return code


_CLEAR_CACHE = r"""
import base64 as _b64, json as _json, zlib as _zlib
try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
_n = len(globals().get("_sqs_cache", {}))
spark.catalog.clearCache()
globals()["_sqs_cache"] = {}
_raw = _json.dumps({"ok": True, "columns": [], "rows": [],
                    "log": ["cache da sessão limpo (%d entrada(s) guardada(s) pela ferramenta)" % _n]})
print("#__MARKER__" + _b64.b64encode(_zlib.compress(_raw.encode("utf-8"), 6)).decode("ascii"))
""".strip()


def build_clear_cache_code() -> str:
    """Código que tira tudo do cache da sessão (para reler dados que mudaram no S3)."""
    return _CLEAR_CACHE.replace("#__MARKER__", RESULT_MARKER)


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
