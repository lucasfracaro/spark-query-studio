"""Tabelas `$partitions` do Athena e subqueries de partição mais recente na importação."""
import pytest

sqlglot = pytest.importorskip("sqlglot")

import latest_partition as lp  # noqa: E402
from athena_import import import_athena_query  # noqa: E402
from code_builder import InputSource  # noqa: E402
from sql_refs import check_query_against_orchestration  # noqa: E402
from tests.fakes import AccessDeniedException, FakeCatalog, glue_partition, glue_table  # noqa: E402

PARTICOES = {
    ("db", "w"): ([("anomes", "string")], [["202607"], ["202609"], ["202608"]]),
    ("db", "s"): ([("year", "int"), ("month", "int"), ("day", "int")],
                  [["2026", "9", "30"], ["2026", "10", "29"], ["2026", "10", "1"]]),
    ("db", "f"): ([("dt", "date")], [["2026-09-30"], ["2026-10-29"]]),
    ("db", "c"): ([("anomesdia", "string")], [["20260901"]]),
}


def valores(db, tabela, filtro=None):
    if (db, tabela) not in PARTICOES:
        raise AccessDeniedException(f"{db}.{tabela}")
    return PARTICOES[(db, tabela)]


def lookup(db, tabela):
    return [k for k, _ in PARTICOES[(db, tabela)][0]] if (db, tabela) in PARTICOES else []


def _importar(sql, **kw):
    r = import_athena_query(sql, lookup, "", partition_values=kw.pop("partition_values", valores), **kw)
    return {i.alias: i.filter for i in r.orchestration.inputs}, " ".join(r.spark_sql.split()), \
        " | ".join(n.text for n in r.notes if n.level == "atencao")


def _calcular(select_sql, tabela=("db", "s")):
    select = sqlglot.parse_one(select_sql, read="athena")
    nomes, linhas = lp.evaluate(select, *PARTICOES[tabela])
    return nomes, [[lp._text(v) for v in linha] for linha in linhas]


# --------------------------------------------------------------------------- cálculo pela lista de partições
def test_maximo_minimo_e_partição_mais_recente():
    assert _calcular("SELECT max(day) FROM x WHERE year = 2026 AND month = 10") == (["day"], [["29"]])
    assert _calcular("SELECT min(month) AS m FROM x") == (["m"], [["9"]])            # número, não texto ("10" < "9")
    assert _calcular("SELECT * FROM x ORDER BY year DESC, month DESC, day DESC LIMIT 1") == \
        (["year", "month", "day"], [["2026", "10", "29"]])
    assert _calcular("SELECT year, month FROM x ORDER BY 2 DESC LIMIT 1")[1] == [["2026", "10"]]
    assert _calcular("SELECT DISTINCT month FROM x ORDER BY month")[1] == [["9"], ["10"]]
    assert _calcular("SELECT max(concat(cast(year AS varchar), lpad(cast(month AS varchar), 2, '0'))) FROM x")[1] \
        == [["202610"]]


def test_funcoes_de_data_em_volta_do_maximo():
    f = ("db", "f")
    assert _calcular("SELECT date_format(date_add('month', -1, max(dt)), '%Y%m') FROM x", f)[1] == [["202609"]]
    assert _calcular("SELECT format_datetime(max(dt) - interval '1' day, 'yyyy-MM-dd') FROM x", f)[1] == \
        [["2026-10-28"]]
    assert _calcular("SELECT year(max(dt)), date_trunc('month', max(dt)) FROM x", f)[1] == [["2026", "2026-10-01"]]
    w = ("db", "w")
    assert _calcular("SELECT date_format(date_add('month', 1, date_parse(max(anomes), '%Y%m')), '%Y%m') FROM x",
                     w)[1] == [["202610"]]
    assert _calcular("SELECT substr(max(anomes), 1, 4) FROM x", w)[1] == [["2026"]]


def test_o_que_nao_da_para_calcular_pela_lista_de_partições():
    for sql in ("SELECT max(valor) FROM x", "SELECT max(day), month FROM x", "SELECT year FROM x GROUP BY year",
                "SELECT max(day) FROM x WHERE day > (SELECT 1)", "SELECT regexp_extract(max(day), '1') FROM x"):
        with pytest.raises(lp.Unresolvable):
            _calcular(sql)


# --------------------------------------------------------------------------- tabela$partitions na importação
def test_partitions_vira_o_valor_atual_e_o_filtro_da_orquestracao():
    filtros, spark, avisos = _importar(
        'SELECT * FROM db.w WHERE anomes = (SELECT anomes FROM "db"."w$partitions" ORDER BY anomes DESC LIMIT 1)')
    assert filtros == {"w": "anomes = '202609'"}                       # $partitions não vira entrada
    assert spark == "SELECT * FROM w WHERE anomes = '202609'"
    assert "`db.w$partitions` é uma tabela do Athena" in avisos and "(anomes = 202609)" in avisos


def test_partitions_com_varias_colunas_e_com_in():
    filtros, spark, _ = _importar(
        'SELECT * FROM db.s WHERE (year, month, day) = (SELECT year, month, day FROM "db"."s$partitions" '
        "ORDER BY year DESC, month DESC, day DESC LIMIT 1) AND tipo = 'x'")
    assert filtros == {"s": "year = 2026 AND month = 10 AND day = 29"} and "tipo = 'x'" in spark
    filtros, spark, _ = _importar('SELECT * FROM db.w WHERE anomes IN (SELECT anomes FROM "db"."w$partitions" '
                                  "ORDER BY anomes DESC LIMIT 2)")
    assert filtros == {"w": "anomes IN ('202609', '202608')"}


def test_partitions_numa_cte_entra_no_join_e_no_filtro():
    filtros, spark, _ = _importar('WITH u AS (SELECT max(anomes) AS anomes FROM "db"."w$partitions") '
                                  "SELECT * FROM db.w v JOIN u ON v.anomes = u.anomes")
    assert filtros == {"w": "anomes = '202609'"} and "v.anomes = '202609'" in spark
    filtros, spark, _ = _importar('WITH u AS (SELECT max(anomes) AS anomes FROM "db"."w$partitions") '
                                  "SELECT * FROM db.w WHERE anomes = (SELECT anomes FROM u)")
    assert filtros == {"w": "anomes = '202609'"}


def test_consulta_direta_a_partitions_vira_values():
    filtros, spark, _ = _importar('SELECT * FROM "db"."s$partitions" ORDER BY year DESC, month DESC, day DESC LIMIT 2')
    assert filtros == {} and spark == "SELECT * FROM VALUES (2026, 10, 29), (2026, 10, 1) AS _p(year, month, day)"


def test_sem_glue_partitions_vira_aviso_e_nao_entrada():
    sql = 'SELECT * FROM db.w WHERE anomes = (SELECT max(anomes) FROM "db"."w$partitions")'
    filtros, _, avisos = _importar(sql, partition_values=None)
    assert "w_partitions" not in filtros and "não consegui trocá-la por um valor" in avisos
    filtros, _, avisos = _importar(sql.replace("db.w", "db.outra").replace('"db"."w$', '"db"."outra$'))
    assert "não consegui ler as partições no Glue (AccessDeniedException)" in avisos
    # se a query rodar assim mesmo, a conferência explica o que é a tabela
    check = check_query_against_orchestration("SELECT * FROM w WHERE anomes = (SELECT max(anomes) FROM "
                                              "`db`.`w$partitions`)", [InputSource("w", "catalog", "db.w")])
    assert "tabela de metadados do Athena" in check.missing[0].message()


# --------------------------------------------------------------------------- max/min que lê a base inteira
def test_max_de_uma_base_avisa_que_ela_e_lida_inteira_com_o_valor_de_hoje():
    sql = "SELECT * FROM db.c WHERE anomesdia = (SELECT date_format(date_add('month', -1, max(dt)), '%Y%m') FROM db.f)"
    filtros, spark, avisos = _importar(sql)
    assert "o Spark lê a tabela `db.f` inteira" in avisos and "a subquery dá hoje 202609" in avisos
    assert "Trocar max/min de colunas de partição" in avisos and "mistura" not in avisos
    assert "MAX(dt)" in spark                                       # só avisa: a query não muda
    filtros, spark, avisos = _importar(sql, resolve_max=True)
    assert filtros == {"c": "anomesdia = '202609'"}                  # db.f nem precisa mais ser lida
    assert spark == "SELECT * FROM c WHERE anomesdia = '202609'" and "foi trocado pelo valor" in avisos


def test_max_de_coluna_que_nao_e_partição_so_avisa():
    _, spark, avisos = _importar("SELECT * FROM db.w WHERE anomes = (SELECT max(valor) FROM db.w)", resolve_max=True)
    assert "o Spark lê a tabela `db.w` inteira" in avisos and "Se der, troque" in avisos and "MAX(valor)" in spark


def test_on_de_join_interno_filtra_a_base_do_from_mas_o_de_left_join_nao():
    filtros, _, _ = _importar("SELECT * FROM db.w a JOIN db.s b ON a.anomes = '202609' AND b.year = 2026")
    assert filtros == {"w": "anomes = '202609'", "s": "year = 2026"}
    filtros, _, _ = _importar("SELECT * FROM db.w a LEFT JOIN db.s b ON a.anomes = '202609' AND b.year = 2026")
    assert filtros == {"w": "", "s": "year = 2026"}


# --------------------------------------------------------------------------- lista de partições pelo Glue
def test_lista_de_partições_pelo_glue_com_link_do_lake_formation():
    origem = {"CatalogId": "999999999999", "DatabaseName": "prod", "Name": "w"}
    particoes = [glue_partition(f"2026{m:02d}") for m in range(1, 13)]
    glue = FakeCatalog({("db", "w"): glue_table(keys=(("anomes", "string"),), target=origem),
                        ("prod", "w"): glue_table(keys=(("anomes", "string"),))}, {("prod", "w"): particoes})
    chaves, linhas = lp.glue_partition_values(glue)("db", "w")
    assert chaves == [("anomes", "string")] and len(linhas) == 12 and ["202612"] in linhas
    filtros, _, _ = _importar('SELECT * FROM db.w WHERE anomes = (SELECT max(anomes) FROM "db"."w$partitions")',
                              partition_values=lp.glue_partition_values(glue))
    assert filtros == {"w": "anomes = '202612'"}
