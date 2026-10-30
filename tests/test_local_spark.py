"""Testes de integração com um Spark de verdade (modo local). Pulados se o PySpark não estiver instalado."""
import pytest

pytest.importorskip("pyspark")

from code_builder import InputSource, Orchestration, build_spark_code  # noqa: E402
from engines import LocalSparkEngine  # noqa: E402


@pytest.fixture(scope="module")
def engine():
    eng = LocalSparkEngine()
    eng.start()
    yield eng
    eng.stop()


@pytest.fixture
def csv_vendas(tmp_path):
    arq = tmp_path / "vendas.csv"
    arq.write_text("id;cliente;valor;dt\n1;Ana;10.5;2026-10-01\n2;Bruno;20;2026-10-02\n3;Ana;5;2026-10-03\n",
                   encoding="utf-8")
    return arq


def _run(engine, sql, **orch):
    return engine.run(build_spark_code(sql, Orchestration(**orch)))


def test_select_simples(engine):
    res = _run(engine, "SELECT 1 AS um, 'ç' AS letra, CAST(NULL AS INT) AS nada")
    assert res["ok"], res
    assert [c["name"] for c in res["columns"]] == ["um", "letra", "nada"]
    assert res["rows"] == [[1, "ç", None]]


def test_entrada_csv_com_opcoes_filtro_e_agregacao(engine, csv_vendas):
    inp = InputSource("vendas", "s3", str(csv_vendas), "csv", "header=true;sep=;;inferSchema=true",
                      filter="dt >= '2026-10-02'")
    res = _run(engine, "SELECT cliente, SUM(valor) AS total FROM vendas GROUP BY cliente ORDER BY cliente",
               inputs=[inp])
    assert res["ok"], res
    assert res["rows"] == [["Ana", 5.0], ["Bruno", 20.0]]


def test_varios_comandos_e_limite(engine):
    sql = ("CREATE OR REPLACE TEMP VIEW numeros AS SELECT id FROM range(1000);\n"
           "SELECT id FROM numeros ORDER BY id")
    res = _run(engine, sql, limit=100)
    assert res["ok"], res
    assert len(res["rows"]) == 100 and res["rows"][-1] == [99]


def test_tipos_decimal_e_data(engine):
    res = _run(engine, "SELECT CAST(1.5 AS DECIMAL(10,2)) AS d, DATE'2026-10-26' AS dt, "
                       "TIMESTAMP'2026-10-26 09:00:00' AS ts, array(1,2) AS arr")
    assert res["ok"], res
    d, dt, ts, arr = res["rows"][0]
    assert d == "1.50" and dt == "2026-10-26" and ts.startswith("2026-10-26 09:00") and arr == "[1, 2]"


def test_spark_conf_aplicada(engine):
    res = _run(engine, "SET spark.sql.shuffle.partitions",
               spark_conf={"spark.sql.shuffle.partitions": "7"})
    assert res["ok"], res
    assert res["rows"] == [["spark.sql.shuffle.partitions", "7"]]


def test_erro_de_analise(engine):
    res = _run(engine, "SELECT * FROM tabela_que_nao_existe")
    assert res["ok"] is False
    assert "tabela_que_nao_existe" in res["error"]


def test_nullable_e_estatisticas(engine):
    sql = ("SELECT * FROM VALUES (1, 'a', CAST(1.5 AS DECIMAL(5,2))), (2, NULL, NULL), (3, 'a', 2.0), "
           "(3, 'b', 2.0) AS t(id, letra, valor)")
    code = build_spark_code(sql, Orchestration(limit=2), profile=True)
    res = engine.run(code)
    assert res["ok"], res
    assert len(res["rows"]) == 2                                   # limite vale só para as linhas exibidas
    assert [c["nullable"] for c in res["columns"]] == [False, True, True]
    prof = res["profile"]
    assert prof["rows"] == 4                                       # estatísticas no resultado completo
    id_, letra, valor = prof["columns"]
    assert id_ == {"nulls": 0, "distinct": 3, "min": 1, "max": 3}
    assert letra["nulls"] == 1 and letra["distinct"] == 2 and letra["min"] == "a" and letra["max"] == "b"
    assert valor["nulls"] == 1 and valor["min"] == "1.50" and valor["max"] == "2.00"


def test_estatisticas_com_nomes_repetidos_e_tipos_complexos(engine):
    sql = "SELECT 1 AS x, 2 AS x, array(1, 2) AS arr, map('k', 1) AS m"
    res = engine.run(build_spark_code(sql, Orchestration(), profile=True))
    assert res["ok"], res
    cols = res["profile"]["columns"]
    assert cols[0]["max"] == 1 and cols[1]["max"] == 2              # renomeadas por posição: sem ambiguidade
    assert cols[2]["distinct"] is None and cols[2]["nulls"] == 0    # complexos: só contagem de nulos


def test_execucoes_nao_compartilham_configuracoes_nem_views(engine):
    r1 = _run(engine, "CREATE OR REPLACE TEMP VIEW sobra AS SELECT 1 AS x; SELECT * FROM sobra",
              spark_conf={"spark.sql.shuffle.partitions": "7"})
    assert r1["ok"], r1
    r2 = _run(engine, "SET spark.sql.shuffle.partitions")
    assert r2["ok"], r2
    assert r2["rows"] == [["spark.sql.shuffle.partitions", "200"]]      # voltou ao padrão
    r3 = _run(engine, "SELECT * FROM sobra")
    assert r3["ok"] is False and "sobra" in r3["error"]                 # a view da execução anterior sumiu


def test_hints_e_comentarios_chegam_ao_spark(engine):
    sql = ("-- comentário no topo\n"
           "SELECT /*+ BROADCAST(b) */ a.id, b.v/*sem espaço*/AS valor\n"
           "FROM range(3) a JOIN (SELECT id, id * 10 AS v FROM range(3)) b ON a.id = b.id\n"
           "ORDER BY a.id")
    res = _run(engine, sql)
    assert res["ok"], res
    assert [c["name"] for c in res["columns"]] == ["id", "valor"]
    assert res["rows"] == [[0, 0], [1, 10], [2, 20]]


def test_query_grande_vai_comprimida(engine):
    sql = "SELECT " + ", ".join(f"{i} AS coluna_numero_{i:05d}" for i in range(3000))
    code = build_spark_code(sql, Orchestration())
    assert "_zlib.decompress" in code
    res = engine.run(code)
    assert res["ok"], res
    assert len(res["columns"]) == 3000 and res["rows"][0][2999] == 2999


# --------------------------------------------------------------------------- cache entre execuções
def _vendas_csv(caminho, valores):
    linhas = "\n".join(f"{i};{v}" for i, v in enumerate(valores, start=1))
    caminho.write_text("id;valor\n" + linhas + "\n", encoding="utf-8")


def test_cache_vale_entre_execucoes_ate_limpar(engine, tmp_path):
    from code_builder import build_clear_cache_code
    arq = tmp_path / "v.csv"
    _vendas_csv(arq, [10, 20])
    entrada = InputSource("v", "s3", str(arq), "csv", "header=true;sep=;;inferSchema=true", cache=True)
    r1 = _run(engine, "SELECT SUM(valor) AS total FROM v", inputs=[entrada])
    assert r1["ok"], r1
    assert r1["rows"] == [[30]] and any("lido agora" in linha for linha in r1["log"])

    _vendas_csv(arq, [1, 2])          # os dados mudam no "S3"
    r2 = _run(engine, "SELECT SUM(valor) AS total, COUNT(*) AS n FROM v", inputs=[entrada])
    assert r2["ok"], r2
    assert r2["rows"] == [[30, 2]]                                     # veio do cache: não releu
    assert any("cache reaproveitado" in linha for linha in r2["log"])

    assert engine.run(build_clear_cache_code())["ok"]
    r3 = _run(engine, "SELECT SUM(valor) AS total FROM v", inputs=[entrada])
    assert r3["rows"] == [[3]]                                         # depois de limpar, relê


def test_entrada_sem_cache_e_sempre_relida(engine, tmp_path):
    arq = tmp_path / "v.csv"
    _vendas_csv(arq, [5])
    entrada = InputSource("v", "s3", str(arq), "csv", "header=true;sep=;;inferSchema=true")
    assert _run(engine, "SELECT SUM(valor) FROM v", inputs=[entrada])["rows"] == [[5]]
    _vendas_csv(arq, [7])
    assert _run(engine, "SELECT SUM(valor) FROM v", inputs=[entrada])["rows"] == [[7]]


def test_cache_table_da_query_vale_entre_execucoes(engine, tmp_path):
    arq = tmp_path / "v.csv"
    _vendas_csv(arq, [1, 1])
    entrada = InputSource("v", "s3", str(arq), "csv", "header=true;sep=;;inferSchema=true")
    sql = "CACHE TABLE base AS SELECT * FROM v WHERE valor > 0; SELECT SUM(valor) FROM base"
    assert _run(engine, sql, inputs=[entrada])["rows"] == [[2]]
    _vendas_csv(arq, [5, 5])
    r2 = _run(engine, sql, inputs=[entrada])
    assert r2["ok"], r2
    assert r2["rows"] == [[2]]                                         # o CACHE TABLE foi reaproveitado
    r3 = _run(engine, "SELECT SUM(valor) FROM v", inputs=[entrada])    # sem o CACHE TABLE: liberado
    assert r3["rows"] == [[10]] and "cache liberado: base" in r3["log"]


# --------------------------------------------------------------------------- Glue 4.0 + Lake Formation
class _GlueContextLocal:
    """Imita o awsglue: o DataFrame nasce na sessão principal, não na sessão da execução.
    create_data_frame é a leitura nativa (useSparkDataSource); create_dynamic_frame, o DynamicFrame (RDD)."""
    tabelas: dict = {}
    leituras: list = []
    nativa_falha = False

    def __init__(self, sc):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        def ler(database, table_name, push_down_predicate):
            nome = f"{database}.{table_name}"
            if nome in _GlueContextLocal.tabelas:
                df = (spark.read.option("header", "true").option("sep", ";").option("inferSchema", "true")
                      .csv(_GlueContextLocal.tabelas[nome]))
            else:
                df = spark.table(nome)                  # tabela de verdade no catálogo local
            return df.where(push_down_predicate) if push_down_predicate else df

        class _Nativo:
            def from_catalog(self, database, table_name, push_down_predicate="", additional_options=None):
                if _GlueContextLocal.nativa_falha:
                    raise RuntimeError("useSparkDataSource is not supported")
                _GlueContextLocal.leituras.append(("nativa", database, table_name, push_down_predicate))
                return ler(database, table_name, push_down_predicate)

        class _Dinamico:
            def from_catalog(self, database, table_name, push_down_predicate=""):
                _GlueContextLocal.leituras.append(("dynamic", database, table_name, push_down_predicate))
                df = ler(database, table_name, push_down_predicate)
                rdd_df = spark.createDataFrame(df.rdd, df.schema)       # como o DynamicFrame: baseado em RDD

                class _Dyf:
                    def toDF(self):
                        return rdd_df
                return _Dyf()

        self.create_data_frame, self.create_dynamic_frame = _Nativo(), _Dinamico()


@pytest.fixture
def awsglue_local(monkeypatch):
    import sys
    import types
    pacote, modulo = types.ModuleType("awsglue"), types.ModuleType("awsglue.context")
    modulo.GlueContext = _GlueContextLocal
    pacote.context = modulo
    monkeypatch.setitem(sys.modules, "awsglue", pacote)
    monkeypatch.setitem(sys.modules, "awsglue.context", modulo)
    _GlueContextLocal.leituras.clear()
    monkeypatch.setattr(_GlueContextLocal, "nativa_falha", False)
    LocalSparkEngine._namespace.pop("_sqs_gc", None)
    return _GlueContextLocal


def _vendas_glue(engine, tmp_path, awsglue_local, cache=True):
    arq = tmp_path / "vendas.csv"
    _vendas_csv(arq, [10, 20, 30])
    awsglue_local.tabelas["db.vendas"] = str(arq)
    entrada = InputSource("vendas", "catalog", "db.vendas", filter="id >= 2", cache=cache)
    return build_spark_code("SELECT SUM(valor) AS total FROM vendas", Orchestration(inputs=[entrada]),
                            reader="glue_context")


def test_glue_context_leitura_nativa_na_sessao_da_execucao(engine, tmp_path, awsglue_local):
    code = _vendas_glue(engine, tmp_path, awsglue_local)
    r1 = engine.run(code)
    assert r1["ok"], r1
    assert r1["rows"] == [[50]]
    assert awsglue_local.leituras == [("nativa", "db", "vendas", "id >= 2")]
    assert any("leitura nativa do Spark" in linha for linha in r1["log"])
    r2 = engine.run(code)
    assert r2["rows"] == [[50]] and len(awsglue_local.leituras) == 1      # cache: não leu de novo
    assert any("cache reaproveitado" in linha for linha in r2["log"])


def test_glue_context_sem_leitura_nativa_usa_o_dynamic_frame(engine, tmp_path, awsglue_local, monkeypatch):
    monkeypatch.setattr(_GlueContextLocal, "nativa_falha", True)
    r = engine.run(_vendas_glue(engine, tmp_path, awsglue_local, cache=False))
    assert r["ok"], r
    assert r["rows"] == [[50]]
    assert awsglue_local.leituras == [("dynamic", "db", "vendas", "id >= 2")]
    assert any("DynamicFrame" in linha for linha in r["log"])


def test_glue_context_separa_o_filtro_pelas_particoes_do_catalogo(engine, tmp_path, awsglue_local):
    pytest.importorskip("sqlglot")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS db_sqs LOCATION '{(tmp_path / 'db').as_posix()}'")
    (spark.createDataFrame([(1, 10, "2026-10-01"), (2, 20, "2026-10-02"), (3, 30, "2026-10-02")], ["id", "valor", "dt"])
     .write.mode("overwrite").partitionBy("dt").option("path", (tmp_path / "vendas").as_posix())
     .saveAsTable("db_sqs.vendas"))
    entrada = InputSource("vendas", "catalog", "db_sqs.vendas", filter="dt = '2026-10-02' AND valor > 20")
    r = engine.run(build_spark_code("SELECT SUM(valor) AS total FROM vendas", Orchestration(inputs=[entrada]),
                                    reader="glue_context"))
    assert r["ok"], r
    assert r["rows"] == [[30]]
    # só a parte de partição foi para a leitura; a outra foi aplicada depois
    assert awsglue_local.leituras == [("nativa", "db_sqs", "vendas", "dt = '2026-10-02'")]
    assert any("aplicado depois da leitura: valor > 20" in linha for linha in r["log"])
