import pytest

pytest.importorskip("sqlglot")

from athena_import import TableNotFoundError, glue_partition_lookup, import_athena_query  # noqa: E402
from sql_refs import check_query_against_orchestration  # noqa: E402

PARTICOES = {
    ("db_vendas", "tb_vendas"): ["dt"],
    ("db_cad", "clientes"): ["anomes"],
    ("db_cad", "produtos"): [],
    ("db_log", "eventos"): ["ano", "mes", "dia"],
}


def lookup(db, tb):
    if (db, tb) not in PARTICOES:
        raise TableNotFoundError(f"{db}.{tb}")
    return PARTICOES[(db, tb)]


def _filtros(r):
    return {i.alias: i.filter for i in r.orchestration.inputs}


def _avisos(r):
    return " | ".join(n.text for n in r.notes if n.level == "atencao")


# --------------------------------------------------------------------------- bases e filtros
def test_filtro_de_partição_da_cte_vai_para_a_orquestracao():
    q = """
    WITH base AS (
      SELECT * FROM "db_vendas"."tb_vendas"
      WHERE dt BETWEEN '2026-10-01' AND '2026-10-31' AND valor > 0
    )
    SELECT * FROM base
    """
    r = import_athena_query(q, lookup)
    assert [(i.alias, i.source, i.filter) for i in r.orchestration.inputs] == [
        ("tb_vendas", "db_vendas.tb_vendas", "dt BETWEEN '2026-10-01' AND '2026-10-31'")]
    assert r.tables[0].partition_source == "glue"
    # a query continua com os filtros originais e passa a usar o alias
    assert "FROM tb_vendas" in r.spark_sql and "valor > 0" in r.spark_sql and "BETWEEN" in r.spark_sql


def test_varias_condicoes_de_particao_sao_combinadas_com_and():
    q = "SELECT * FROM db_log.eventos e WHERE e.ano = 2026 AND e.mes IN (9, 10) AND e.tipo = 'x' AND dia >= 1"
    assert _filtros(import_athena_query(q, lookup)) == {"eventos": "ano = 2026 AND mes IN (9, 10) AND dia >= 1"}


def test_mesma_base_com_filtros_diferentes_vira_or():
    q = """
    WITH hoje AS (SELECT * FROM db_vendas.tb_vendas WHERE dt = '2026-10-26'),
         ontem AS (SELECT * FROM db_vendas.tb_vendas WHERE dt = '2026-10-25')
    SELECT * FROM hoje UNION ALL SELECT * FROM ontem
    """
    assert _filtros(import_athena_query(q, lookup)) == {"tb_vendas": "(dt = '2026-10-26') OR (dt = '2026-10-25')"}


def test_mesma_base_lida_sem_filtro_em_algum_lugar_fica_sem_filtro():
    q = """
    WITH a AS (SELECT * FROM db_vendas.tb_vendas WHERE dt = '2026-10-26'),
         b AS (SELECT * FROM db_vendas.tb_vendas)
    SELECT * FROM a JOIN b ON a.id = b.id
    """
    r = import_athena_query(q, lookup)
    assert _filtros(r) == {"tb_vendas": ""}
    assert "sem filtro em outra" in _avisos(r)


def test_filtro_no_on_do_join():
    q = """
    SELECT * FROM db_vendas.tb_vendas v
    JOIN db_cad.clientes c ON c.id = v.id_cliente AND c.anomes = 202610
    WHERE v.dt = '2026-10-26'
    """
    assert _filtros(import_athena_query(q, lookup)) == {"tb_vendas": "dt = '2026-10-26'", "clientes": "anomes = 202610"}


def test_on_de_right_join_nao_filtra_a_base_juntada():
    q = "SELECT * FROM db_cad.produtos p RIGHT JOIN db_cad.clientes c ON c.id = p.id AND c.anomes = 202610"
    assert _filtros(import_athena_query(q, lookup))["clientes"] == ""


def test_coluna_sem_prefixo_com_varias_bases():
    q = "SELECT * FROM db_vendas.tb_vendas v JOIN db_cad.clientes c ON c.id = v.id WHERE dt = '2026-10-26'"
    # dt só é partição de tb_vendas: atribuída a ela
    assert _filtros(import_athena_query(q, lookup))["tb_vendas"] == "dt = '2026-10-26'"


@pytest.mark.parametrize("cond, motivo", [
    ("dt = (SELECT max(dt) FROM db_vendas.tb_vendas)", "subquery"),
    ("(dt = '2026-10-26' OR valor > 10)", "mistura"),
    ("(dt = '2026-10-26' OR dt IS NULL)", "nulos"),
])
def test_condicoes_que_nao_podem_ser_levadas(cond, motivo):
    r = import_athena_query(f"SELECT * FROM db_vendas.tb_vendas v WHERE {cond}", lookup)
    assert r.orchestration.inputs[0].filter == "" or "SELECT" not in r.orchestration.inputs[0].filter
    assert motivo in _avisos(r)


def test_base_particionada_sem_filtro_gera_aviso():
    r = import_athena_query("SELECT * FROM db_cad.clientes", lookup)
    assert "todas as partições serão lidas" in _avisos(r)


def test_base_nao_particionada_nao_gera_aviso_de_particao():
    r = import_athena_query("SELECT * FROM db_cad.produtos", lookup)
    assert _avisos(r) == ""
    assert any("sem pendências" in n.text for n in r.notes)


# --------------------------------------------------------------------------- nomes e aliases
def test_ctes_nao_viram_entradas_e_alias_nao_colide_com_cte():
    q = """
    WITH tb_vendas AS (SELECT * FROM db_vendas.tb_vendas WHERE dt = '2026-10-26')
    SELECT * FROM tb_vendas
    """
    r = import_athena_query(q, lookup)
    assert [i.alias for i in r.orchestration.inputs] == ["tb_vendas_base"]
    assert "FROM tb_vendas_base AS tb_vendas" in r.spark_sql.replace("\n", " ")


def test_mesmo_nome_em_databases_diferentes():
    def lk(db, tb):
        return []
    r = import_athena_query("SELECT * FROM db1.clientes a JOIN db2.clientes b ON a.id = b.id", lk)
    assert sorted(i.alias for i in r.orchestration.inputs) == ["db1_clientes", "db2_clientes"]


def test_tabela_sem_alias_ganha_o_nome_original_para_manter_as_colunas():
    def lk(db, tb):
        return []
    r = import_athena_query("SELECT clientes.id FROM db1.clientes JOIN db2.clientes c2 ON 1=1", lk)
    sql = r.spark_sql.replace("\n", " ")
    assert "FROM db1_clientes AS clientes" in sql and "clientes.id" in sql


def test_database_padrao_para_tabela_sem_database():
    r = import_athena_query("SELECT * FROM tb_vendas WHERE dt = '2026-10-26'", lookup, default_database="db_vendas")
    assert r.orchestration.inputs[0].source == "db_vendas.tb_vendas"
    assert r.orchestration.inputs[0].filter == "dt = '2026-10-26'"


def test_tabela_sem_database_e_sem_padrao():
    r = import_athena_query("SELECT * FROM solta", lookup)
    assert r.orchestration.inputs[0].source == "solta"
    assert "sem database" in _avisos(r)


# --------------------------------------------------------------------------- partições sem Glue
def test_sem_glue_supoe_pelo_nome():
    q = "SELECT * FROM db_x.qualquer WHERE anomes = 202610 AND status = 'A'"
    r = import_athena_query(q, None)
    assert _filtros(r) == {"qualquer": "anomes = 202610"}
    assert r.tables[0].partition_source == "suposicao"
    assert "supostas pelo nome" in _avisos(r)


def test_tabela_inexistente_no_glue():
    r = import_athena_query("SELECT * FROM db_x.nao_existe WHERE dt = '1'", lookup)
    assert "não existe no Glue" in _avisos(r)


def test_glue_fora_do_ar_cai_para_suposicao_uma_vez_so():
    def quebrado(db, tb):
        raise ConnectionError("sem rede")
    r = import_athena_query("SELECT * FROM db.a JOIN db.b ON 1=1 WHERE a.dt = '1'", quebrado)
    assert _avisos(r).count("Não consegui consultar o Glue") == 1
    assert _filtros(r)["a"] == "dt = '1'"


def test_glue_partition_lookup_usa_cache():
    class Glue:
        chamadas = 0

        def get_table(self, DatabaseName, Name):
            Glue.chamadas += 1
            return {"Table": {"PartitionKeys": [{"Name": "DT", "Type": "string"}]}}

    lk = glue_partition_lookup(Glue())
    assert lk("Db", "T") == ["dt"] and lk("db", "t") == ["dt"]
    assert Glue.chamadas == 1


def test_glue_partition_lookup_tabela_inexistente():
    class EntityNotFoundException(Exception):
        pass

    class Glue:
        def get_table(self, DatabaseName, Name):
            raise EntityNotFoundException("x")

    with pytest.raises(TableNotFoundError):
        glue_partition_lookup(Glue())("db", "t")


# --------------------------------------------------------------------------- tradução
def test_traducao_de_funcoes_do_athena():
    q = """SELECT date_add('day', -1, current_date) AS ontem,
                  date_format(ts, '%Y-%m-%d') AS d,
                  approx_distinct(id) AS n
           FROM db_cad.produtos"""
    r = import_athena_query(q, lookup)
    sql = r.spark_sql.upper()
    assert "DATE_ADD(CURRENT_DATE, -1)" in sql
    assert "'YYYY-MM-DD'" in sql
    assert "APPROX_COUNT_DISTINCT" in sql
    assert any("Formatos de data" in n.text for n in r.notes)


def test_funcao_desconhecida_e_sinalizada():
    r = import_athena_query("SELECT minha_udf(x) FROM db_cad.produtos", lookup)
    assert "minha_udf" in _avisos(r).lower()


def test_query_invalida():
    with pytest.raises(ValueError, match="Não consegui ler"):
        import_athena_query("SELECT FROM WHERE (", lookup)


def test_query_vazia():
    with pytest.raises(ValueError):
        import_athena_query("   ", lookup)


# --------------------------------------------------------------------------- integração
def test_resultado_passa_na_conferencia_da_orquestracao():
    q = """
    WITH v AS (SELECT * FROM db_vendas.tb_vendas WHERE dt = '2026-10-26')
    SELECT v.*, c.nome FROM v JOIN db_cad.clientes c ON c.id = v.id_cliente AND c.anomes = 202610
    """
    r = import_athena_query(q, lookup)
    check = check_query_against_orchestration(r.spark_sql, r.orchestration.inputs)
    assert check.ok and not check.warnings
