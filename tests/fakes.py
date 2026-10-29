"""Dublês de teste: um Spark mínimo (para executar o código gerado sem PySpark)
e um cliente Glue em memória (para testar o motor sem AWS)."""
from __future__ import annotations

import io
from contextlib import redirect_stdout

from code_builder import parse_result


# --------------------------------------------------------------------------- Spark falso
class _Type:
    def __init__(self, name):
        self._name = name

    def simpleString(self):
        return self._name


class _Field:
    def __init__(self, name, type_name):
        self.name = name
        self.dataType = _Type(type_name)


class _Schema:
    def __init__(self, fields):
        self.fields = fields


class _StorageLevel:
    def __init__(self, cached: bool):
        self.useMemory = self.useDisk = cached
        self.useOffHeap = False


class _FakeJdf:
    """O lado "Java" de um DataFrame, só com o que o código gerado usa para trocá-lo de sessão."""

    def __init__(self, df):
        self.df = df

    def rdd(self):
        return ("rdd", self.df)

    def schema(self):
        return self.df.columns

    def queryExecution(self):            # .analyzed().toString(): o "plano" em texto
        plano = str(self.df.plan)

        class _Analisado:
            def toString(self):
                return plano

        class _QE:
            def analyzed(self):
                return _Analisado()
        return _QE()


class FakeDataFrame:
    """plan identifica o que o DataFrame lê (como o plano lógico do Spark): é a chave do cache, que,
    como no Spark, vale para todas as sessões."""

    def __init__(self, rows, columns=None, spark=None, plan=()):
        if isinstance(rows, _FakeJdf):                  # DataFrame(jdf, sessão), como no PySpark
            origem, spark = rows.df, columns
            rows, columns, plan = origem.rows, origem.columns, ("rdd",) + origem.plan
        self.rows = rows
        self.columns = columns          # [(nome, tipo)]
        self.schema = _Schema([_Field(n, t) for n, t in columns])
        self.spark = spark
        self.plan = tuple(plan)
        self._jdf = _FakeJdf(self)

    def where(self, cond):
        if self.spark is not None:
            self.spark.calls.append(("where", cond))
        return FakeDataFrame(self.rows, self.columns, self.spark, self.plan + (("where", cond),))

    def createOrReplaceTempView(self, alias):
        self.spark.views[alias] = self

    def createOrReplaceGlobalTempView(self, nome):
        self.spark.global_views[nome] = self

    def collect(self):
        if self.spark is not None:
            self.spark.calls.append(("collect",))
        return list(self.rows)

    def limit(self, n):
        return FakeDataFrame(self.rows[:n], self.columns, self.spark, self.plan + (("limit", n),))

    @property
    def storageLevel(self):
        return _StorageLevel(self.plan in self.spark.shared_cache)

    def cache(self):
        self.spark.shared_cache.add(self.plan)
        self.spark.cached.append(self.plan)
        return self

    def unpersist(self):
        self.spark.shared_cache.discard(self.plan)
        self.spark.uncached.append(self.plan)
        return self


class _Reader:
    def __init__(self, spark):
        self.spark = spark
        self.fmt = None
        self.options = {}

    def format(self, fmt):
        self.fmt = fmt
        return self

    def option(self, k, v):
        self.options[k] = v
        return self

    def load(self, path):
        self.spark.calls.append(("load", self.fmt, path, dict(self.options)))
        return self.spark.default_df(("load", self.fmt, path, tuple(sorted(self.options.items()))))


class _Conf:
    def __init__(self, spark):
        self.spark = spark

    def set(self, k, v):
        if k in self.spark.bad_conf:
            raise ValueError(f"Cannot modify the value of a static config: {k}")
        self.spark.conf_values[k] = v

    def get(self, k, default=None):
        return self.spark.conf_values.get(k, self.spark.settings.get(k, default))


class _Catalog:
    def __init__(self, spark):
        self.spark = spark

    def clearCache(self):
        self.spark.shared_cache.clear()
        self.spark.cache_cleared += 1

    def dropGlobalTempView(self, nome):
        return self.spark.global_views.pop(nome, None) is not None


class _JvmSession:
    def __init__(self, spark):
        self.spark = spark

    def createDataFrame(self, rdd, schema):
        assert rdd[0] == "rdd"
        return _FakeJdf(rdd[1])


_COMPARTILHADOS = ("shared_cache", "cached", "uncached", "cache_cleared", "global_views")


class FakeSpark:
    """Imita o pedaço da API do SparkSession que o código gerado usa."""

    def __init__(self, rows=None, columns=None, fail_on=None, bad_conf=(), settings=None, parent=None):
        self.rows = rows if rows is not None else [(1, "a", None), (2, "b", 3.5), (3, "c", 1.0)]
        self.columns = columns or [("id", "int"), ("nome", "string"), ("valor", "double")]
        self.fail_on = fail_on            # texto que, se aparecer na query, gera erro
        self.bad_conf = set(bad_conf)
        # configurações "iniciais" da sessão (o que o comando SET devolve)
        self.settings = dict(settings if settings is not None else {"spark.sql.catalogImplementation": "hive"})
        self.calls, self.sql_log = [], []
        self.views, self.conf_values = {}, {}
        self.sessions: list[FakeSpark] = []   # sessões criadas com newSession()
        self.root = parent.root if parent is not None else self
        if parent is None:                    # como no Spark, o cache e as views globais valem para todas as sessões
            self.shared_cache, self.cached, self.uncached = set(), [], []
            self.cache_cleared = 0
            self.global_views = {}
        self.conf = _Conf(self)
        self.catalog = _Catalog(self)
        self._jsparkSession = _JvmSession(self)
        self.sparkContext = ("sparkContext", self.root)

    # cache e contadores ficam na sessão principal
    def __getattr__(self, nome):
        if nome in _COMPARTILHADOS:
            return getattr(self.root, nome)
        raise AttributeError(nome)

    def __setattr__(self, nome, valor):
        if nome in _COMPARTILHADOS and self.__dict__.get("root") not in (None, self):
            setattr(self.root, nome, valor)
        else:
            object.__setattr__(self, nome, valor)

    def newSession(self):
        """Sessão nova: começa sem views, sem configurações alteradas e no database padrão."""
        child = FakeSpark(rows=self.rows, columns=self.columns, fail_on=self.fail_on,
                          bad_conf=self.bad_conf, settings={}, parent=self)
        self.sessions.append(child)
        return child

    @property
    def last_session(self) -> FakeSpark:
        return self.sessions[-1]

    @property
    def read(self):
        return _Reader(self)

    def default_df(self, plan=()):
        return FakeDataFrame(self.rows, self.columns, self, plan)

    def table(self, name):
        self.calls.append(("table", name))
        if name.startswith("global_temp."):
            origem = self.global_views[name.split(".", 1)[1]]
            return FakeDataFrame(origem.rows, origem.columns, self, ("global",) + origem.plan)
        return self.default_df(("table", name.lower()))

    def sql(self, query):
        self.sql_log.append(query)
        if query.strip().upper() == "SET":
            return FakeDataFrame(list(self.settings.items()), [("key", "string"), ("value", "string")], self)
        if self.fail_on and self.fail_on in query:
            raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] {self.fail_on}")
        return self.default_df(("sql", query))


class FakeGlueContext:
    """awsglue.context.GlueContext mínimo. As leituras acontecem na sessão principal, como no Glue.

    create_data_frame.from_catalog (leitura nativa, useSparkDataSource) e create_dynamic_frame.from_catalog
    (DynamicFrame). nativa_falha simula um Glue em que a leitura nativa não funciona."""
    instancias: list[FakeGlueContext] = []

    def __init__(self, spark_context):
        self.spark = spark_context[1]
        self.reads, self.native_reads = [], []
        self.falhar_com_filtro = False
        self.nativa_falha = False
        contexto = self

        class _Dinamico:
            def from_catalog(self, database, table_name, push_down_predicate=""):
                contexto._conferir_filtro(push_down_predicate)
                contexto.reads.append((database, table_name, push_down_predicate))
                df = contexto.spark.default_df(("LogicalRDD", "glue", database, table_name, push_down_predicate))

                class _Dyf:
                    def toDF(self):
                        return df
                return _Dyf()

        class _Nativo:
            def from_catalog(self, database, table_name, push_down_predicate="", additional_options=None):
                if contexto.nativa_falha:
                    raise RuntimeError("useSparkDataSource is not supported for this table")
                contexto._conferir_filtro(push_down_predicate)
                contexto.native_reads.append((database, table_name, push_down_predicate, dict(additional_options or {})))
                return contexto.spark.default_df(("Relation", "glue", database, table_name, push_down_predicate))

        self.create_dynamic_frame, self.create_data_frame = _Dinamico(), _Nativo()
        FakeGlueContext.instancias.append(self)

    def _conferir_filtro(self, filtro):
        if filtro and self.falhar_com_filtro:
            raise RuntimeError("cannot resolve column used in push_down_predicate")


def run_generated(code: str, spark: FakeSpark, namespace: dict | None = None) -> dict:
    """Executa o código gerado como o motor faz e devolve o resultado decodificado.
    namespace: variáveis que sobrevivem entre execuções (no Glue, a sessão guarda as variáveis)."""
    buf = io.StringIO()
    ns = namespace if namespace is not None else {}
    ns["spark"] = spark
    with redirect_stdout(buf):
        exec(code, ns)
    return parse_result(buf.getvalue())


# --------------------------------------------------------------------------- Glue falso
class EntityNotFoundException(Exception):
    pass


class IllegalSessionStateException(Exception):
    """A sessão não aceita comandos (encerrou por tempo sem uso, por exemplo)."""


class SessionBusyException(Exception):
    """A sessão já está rodando outro comando."""


class FakeGlueClient:
    """Cliente Glue em memória.

    session_states: estados devolvidos em sequência por get_session (o último se repete).
    statement_states: idem para get_statement; o resultado final vem de `output`.
    """

    def __init__(self, session_states=("PROVISIONING", "READY"), statement_states=("RUNNING", "AVAILABLE"),
                 output=None, listed_sessions=(), run_errors=(), get_statement_errors=()):
        self.session_states = list(session_states)
        self.statement_states = list(statement_states)
        self.output = output if output is not None else {"Status": "OK", "Data": {"TextPlain": ""}}
        self.listed_sessions = list(listed_sessions)     # o que list_sessions devolve (com "Tags")
        self.run_errors = list(run_errors)               # exceções que os próximos run_statement levantam
        self.get_statement_errors = list(get_statement_errors)
        self.created, self.statements, self.stopped, self.cancelled = [], [], [], []
        self.list_calls, self.get_session_calls = [], []

    @staticmethod
    def _next(states):
        return states.pop(0) if len(states) > 1 else states[0]

    def create_session(self, **params):
        self.created.append(params)
        return {"Session": {"Id": params["Id"], "Status": "PROVISIONING"}}

    def get_session(self, Id):
        self.get_session_calls.append(Id)
        status = self._next(self.session_states)
        if status is None:
            raise EntityNotFoundException(Id)
        return {"Session": {"Id": Id, "Status": status, "ErrorMessage": "falhou de propósito"}}

    def run_statement(self, SessionId, Code):
        if self.run_errors:
            raise self.run_errors.pop(0)
        self.statements.append((SessionId, Code))
        return {"Id": len(self.statements)}

    def get_statement(self, SessionId, Id):
        if self.get_statement_errors:
            raise self.get_statement_errors.pop(0)
        state = self._next(self.statement_states)
        st = {"Id": Id, "State": state, "Progress": 0.5}
        if state == "AVAILABLE":
            st["Output"] = self.output
        return {"Statement": st}

    def cancel_statement(self, SessionId, Id):
        self.cancelled.append(Id)

    def stop_session(self, Id):
        self.stopped.append(Id)

    def list_sessions(self, Tags=None, MaxResults=None, NextToken=None):
        self.list_calls.append(Tags)
        filtradas = [{k: v for k, v in sessao.items() if k != "Tags"} for sessao in self.listed_sessions
                     if all(sessao.get("Tags", {}).get(k) == v for k, v in (Tags or {}).items())]
        # devolve em duas páginas, para exercitar a paginação
        if NextToken is None and len(filtradas) > 1:
            return {"Sessions": filtradas[:1], "NextToken": "pagina2"}
        return {"Sessions": filtradas[1:] if NextToken else filtradas}


class FakeSts:
    def __init__(self, arn="arn:aws:sts::123456789012:assumed-role/Dev/lucas", falha=False):
        self.arn, self.falha, self.calls = arn, falha, 0

    def get_caller_identity(self):
        self.calls += 1
        if self.falha:
            raise PermissionError("AccessDenied")
        return {"Arn": self.arn, "Account": "123456789012", "UserId": "X"}
