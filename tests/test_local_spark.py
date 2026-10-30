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


def test_resultado_completo_traz_todas_as_linhas(engine):
    from export import fetch_full_result, rows_to_csv_bytes
    sql = "SELECT id, CAST(id / 2 AS DECIMAL(10,1)) AS metade, IF(id % 3 = 0, NULL, 'x;' || id) AS t FROM range(5000)"
    orch = Orchestration(limit=10)
    tela = engine.run(build_spark_code(sql, orch))
    assert tela["ok"] and len(tela["rows"]) == 10
    dados, info = fetch_full_result(lambda codigo, on_status: engine.run(codigo), sql, orch, chunk_bytes=4096)
    assert info["rows"] == 5000 and not info["truncated"] and info["chunks"] > 1
    linhas = dados.decode("utf-8-sig").splitlines()
    assert len(linhas) == 5001 and linhas[0] == "id;metade;t"
    # as primeiras linhas são as mesmas do CSV da tela, no mesmo formato
    inicio = rows_to_csv_bytes(["id", "metade", "t"], tela["rows"], [c["type"] for c in tela["columns"]])
    assert dados.startswith(inicio)


def test_database_padrao_que_nao_existe_so_avisa(engine):
    res = _run(engine, "SELECT 1 AS um", default_database="nao_existe_aqui")
    assert res["ok"], res
    assert any(linha.startswith("AVISO: o database padrão nao_existe_aqui") for linha in res["log"])


def test_total_de_linhas_e_csv_do_resultado_guardado(engine):
    from export import fetch_full_result
    sql = "SELECT id % 7 AS grupo, count(*) AS n FROM range(10000) GROUP BY id % 7 ORDER BY grupo"
    orch = Orchestration(limit=3)
    res = engine.run(build_spark_code(sql, orch, count=True))
    assert res["ok"], res
    assert res["total_rows"] == 7 and len(res["rows"]) == 3
    assert any("guardado em memória" in linha for linha in res["log"])      # agregação: fica em memória
    dados, info = fetch_full_result(lambda codigo, on_status: engine.run(codigo), sql, orch,
                                    result_key=res["result_key"], chunk_bytes=64)
    assert info["rerun"] is False and info["rows"] == 7                    # sem rodar a query de novo
    linhas = dados.decode("utf-8-sig").splitlines()
    assert linhas[0] == "grupo;n" and linhas[1:3] == ["0;1429", "1;1429"] and len(linhas) == 8


# --------------------------------------------------------------------------- erro de coluna e linha da query
def test_coluna_que_nao_existe_aponta_a_linha_e_os_nomes_parecidos(engine, csv_vendas):
    import column_errors
    from spark_errors import locate_error
    entrada = InputSource("vendas", "s3", str(csv_vendas), "csv", "header=true;sep=;;inferSchema=true")
    sql = "SET x = 1;\nSELECT id,\n  clinte\nFROM vendas"
    res = _run(engine, sql, inputs=[entrada])
    assert res["ok"] is False and res["statement"] == 1, res
    assert res["inputs"] == [{"alias": "vendas", "kind": "s3", "source": str(csv_vendas), "read": "s3",
                              "columns": ["id", "cliente", "valor", "dt"]}]
    local = locate_error(res["error"], sql, res["statement"])
    assert (local.line, local.column, local.statement) == (3, 2, 2)
    d = column_errors.diagnose(res["error"], "SELECT id,\n  clinte\nFROM vendas", sql, res["inputs"], None, [entrada])
    assert d.kind == "nao_existe" and d.similar[0] == "cliente"


def test_erro_de_sintaxe_aponta_a_linha(engine):
    from spark_errors import locate_error
    sql = "SELECT 1;\n\nSELECT id,\n  ) FROM range(3)"
    res = _run(engine, sql)
    assert res["ok"] is False and res["statement"] == 1, res
    local = locate_error(res["error"], sql, res["statement"])
    assert (local.line, local.column) == (4, 2), res["error"]


def test_glue_context_coluna_do_catalogo_que_a_leitura_nao_trouxe(engine, tmp_path, awsglue_local):
    from pyspark.sql import SparkSession

    import column_errors
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS db_diag LOCATION '{(tmp_path / 'db').as_posix()}'")
    (spark.createDataFrame([(1, "123", "123")], ["id", "doc", "doc_str"])
     .write.mode("overwrite").option("path", (tmp_path / "clientes").as_posix()).saveAsTable("db_diag.clientes"))
    arquivo = tmp_path / "clientes.csv"                  # a leitura pelo GlueContext não traz a coluna nova
    arquivo.write_text("id;doc\n1;123\n", encoding="utf-8")
    awsglue_local.tabelas["db_diag.clientes"] = str(arquivo)
    entrada = InputSource("clientes", "catalog", "db_diag.clientes")
    sql = "SELECT id, doc_str FROM clientes"
    res = engine.run(build_spark_code(sql, Orchestration(inputs=[entrada]), reader="glue_context"))
    assert res["ok"] is False, res
    [info] = res["inputs"]
    assert info["read"] == "nativa" and info["columns"] == ["id", "doc"]
    assert info["catalog"] == ["id", "doc", "doc_str"]               # o que a "role" vê no catálogo
    d = column_errors.diagnose(res["error"], sql, sql, res["inputs"], None, [entrada], glue=True)
    assert d.kind == "arquivos" and d.title == "A leitura de `clientes` não trouxe a coluna `doc_str`"


def test_coluna_que_falta_numa_cte_que_a_query_nao_usa(engine, csv_vendas):
    # Glue 4.0 (Spark 3.3): "unresolved operator 'Project [...]", sem linha; do Spark 3.4 em diante, a coluna e a linha
    import column_errors
    from spark_errors import locate, locate_error
    entrada = InputSource("vendas", "s3", str(csv_vendas), "csv", "header=true;sep=;;inferSchema=true")
    sql = "WITH base AS (\n  SELECT id,\n    regiao\n  FROM vendas\n)\nSELECT count(*) AS n FROM vendas"
    res = _run(engine, sql, inputs=[entrada])
    assert res["ok"] is False and res["inputs"][0]["columns"] == ["id", "cliente", "valor", "dt"], res
    d = column_errors.diagnose(res["error"], sql, sql, res["inputs"], None, [entrada])
    assert d.kind == "nao_existe" and "`regiao`" in d.title, res["error"]
    local = locate_error(res["error"], sql, 0) or locate(sql, 0, *d.position)
    assert (local.line, local.column) == (3, 4)
    if "unresolved operator" in res["error"]:
        assert d.cte == "base" and "que a query final não usa" in d.cause
