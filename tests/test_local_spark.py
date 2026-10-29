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
