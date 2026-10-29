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


class FakeDataFrame:
    def __init__(self, rows, columns, spark=None):
        self.rows = rows
        self.columns = columns          # [(nome, tipo)]
        self.schema = _Schema([_Field(n, t) for n, t in columns])
        self.spark = spark

    def where(self, cond):
        if self.spark is not None:
            self.spark.calls.append(("where", cond))
        return self

    def createOrReplaceTempView(self, alias):
        self.spark.views[alias] = self

    def collect(self):
        if self.spark is not None:
            self.spark.calls.append(("collect",))
        return list(self.rows)

    def limit(self, n):
        return FakeDataFrame(self.rows[:n], self.columns, self.spark)


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
        return self.spark.default_df()


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

    def cacheTable(self, name):
        self.spark.cached.append(name)

    def clearCache(self):
        self.spark.cache_cleared += 1


class FakeSpark:
    """Imita o pedaço da API do SparkSession que o código gerado usa."""

    def __init__(self, rows=None, columns=None, fail_on=None, bad_conf=(), settings=None):
        self.rows = rows if rows is not None else [(1, "a", None), (2, "b", 3.5), (3, "c", 1.0)]
        self.columns = columns or [("id", "int"), ("nome", "string"), ("valor", "double")]
        self.fail_on = fail_on            # texto que, se aparecer na query, gera erro
        self.bad_conf = set(bad_conf)
        # configurações "iniciais" da sessão (o que o comando SET devolve)
        self.settings = dict(settings if settings is not None else {"spark.sql.catalogImplementation": "hive"})
        self.calls, self.sql_log, self.cached = [], [], []
        self.views, self.conf_values = {}, {}
        self.cache_cleared = 0
        self.sessions: list[FakeSpark] = []   # sessões criadas com newSession()
        self.conf = _Conf(self)
        self.catalog = _Catalog(self)

    def newSession(self):
        """Sessão nova: começa sem views, sem configurações alteradas e no database padrão."""
        child = FakeSpark(rows=self.rows, columns=self.columns, fail_on=self.fail_on,
                          bad_conf=self.bad_conf, settings={})
        self.sessions.append(child)
        return child

    @property
    def last_session(self) -> "FakeSpark":
        return self.sessions[-1]

    @property
    def read(self):
        return _Reader(self)

    def default_df(self):
        return FakeDataFrame(self.rows, self.columns, self)

    def table(self, name):
        self.calls.append(("table", name))
        return self.default_df()

    def sql(self, query):
        self.sql_log.append(query)
        if query.strip().upper() == "SET":
            return FakeDataFrame(list(self.settings.items()), [("key", "string"), ("value", "string")], self)
        if self.fail_on and self.fail_on in query:
            raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] {self.fail_on}")
        return self.default_df()


def run_generated(code: str, spark: FakeSpark) -> dict:
    """Executa o código gerado como o motor local faz e devolve o resultado decodificado."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        exec(code, {"spark": spark})
    return parse_result(buf.getvalue())


# --------------------------------------------------------------------------- Glue falso
class EntityNotFoundException(Exception):
    pass


class FakeGlueClient:
    """Cliente Glue em memória.

    session_states: estados devolvidos em sequência por get_session (o último se repete).
    statement_states: idem para get_statement; o resultado final vem de `output`.
    """

    def __init__(self, session_states=("PROVISIONING", "READY"), statement_states=("RUNNING", "AVAILABLE"),
                 output=None):
        self.session_states = list(session_states)
        self.statement_states = list(statement_states)
        self.output = output if output is not None else {"Status": "OK", "Data": {"TextPlain": ""}}
        self.created, self.statements, self.stopped, self.cancelled = [], [], [], []

    @staticmethod
    def _next(states):
        return states.pop(0) if len(states) > 1 else states[0]

    def create_session(self, **params):
        self.created.append(params)
        return {"Session": {"Id": params["Id"], "Status": "PROVISIONING"}}

    def get_session(self, Id):
        status = self._next(self.session_states)
        if status is None:
            raise EntityNotFoundException(Id)
        return {"Session": {"Id": Id, "Status": status, "ErrorMessage": "falhou de propósito"}}

    def run_statement(self, SessionId, Code):
        self.statements.append(Code)
        return {"Id": len(self.statements)}

    def get_statement(self, SessionId, Id):
        state = self._next(self.statement_states)
        st = {"Id": Id, "State": state, "Progress": 0.5}
        if state == "AVAILABLE":
            st["Output"] = self.output
        return {"Statement": st}

    def cancel_statement(self, SessionId, Id):
        self.cancelled.append(Id)

    def stop_session(self, Id):
        self.stopped.append(Id)
