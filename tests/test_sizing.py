"""Estimativa do tamanho da sessão: catálogo do Glue, S3 (só buckets da conta), histórico do Athena e a conta."""
from datetime import datetime

import pytest

pytest.importorskip("sqlglot")

import sizing  # noqa: E402
from athena_import import TableInfo  # noqa: E402
from tests.fakes import (  # noqa: E402
    FakeAthena,
    FakeCatalog,
    FakeS3,
    athena_execution,
    glue_partition,
    glue_table,
)

GiB, MiB = sizing.GiB, sizing.MiB
COLUNAS = (("id", "bigint"), ("cliente", "bigint"), ("valor", "decimal(18,2)"), ("descricao", "string"))
SQL = "SELECT cliente, sum(valor) FROM db.vendas WHERE dt BETWEEN '2026-10-02' AND '2026-10-03' GROUP BY 1"
FILTRO = "dt BETWEEN '2026-10-02' AND '2026-10-03'"


def _ref(alias="vendas", db="db", tabela="vendas", filtro=FILTRO):
    return TableInfo(db, tabela, alias, ["dt"], "glue", filtro)


def _vendas(**kw):
    """5 partições diárias de 2 GB com estatísticas do Hive."""
    particoes = [glue_partition(f"2026-10-0{d}", f"s3://meu-bucket/vendas/dt=2026-10-0{d}",
                                totalSize=2 * GiB, numRows=10_000_000, numFiles=16) for d in range(1, 6)]
    return FakeCatalog({("db", "vendas"): glue_table(columns=COLUNAS)}, {("db", "vendas"): particoes}, **kw)


def _coletar(glue=None, refs=None, sql=SQL, **kw):
    return sizing.collect(refs or [_ref()], sql, glue=glue, **kw)


# --------------------------------------------------------------------------- filtro de partição
def test_filtro_vira_expression_do_glue():
    tipos = {"dt": "string", "anomes": "int"}
    assert sizing.glue_expression(FILTRO, tipos) == FILTRO
    # o tipo da coluna decide as aspas; o DATE '...' do Athena vira texto; a coluna fica à esquerda
    assert sizing.glue_expression("anomes = '202410'", tipos) == "anomes = 202410"
    assert sizing.glue_expression("dt = 20261001", tipos) == "dt = '20261001'"
    assert sizing.glue_expression("dt = CAST('2026-10-01' AS DATE)", tipos) == "dt = '2026-10-01'"
    assert sizing.glue_expression("'2026-10-01' <= dt", tipos) == "dt >= '2026-10-01'"
    assert sizing.glue_expression("(dt = 'a') OR (dt = 'b')", tipos) == "((dt = 'a')) OR ((dt = 'b'))"
    assert sizing.glue_expression("anomes IN (202409, 202410) AND NOT dt LIKE '2026%'", tipos) == \
        "(anomes IN (202409, 202410)) AND (NOT (dt LIKE '2026%'))"
    # o que o Expression do Glue não tem: função, coluna que não é partição, subquery
    assert sizing.glue_expression("dt >= date_format(current_date, 'yyyy-MM-dd')", tipos) is None
    assert sizing.glue_expression("valor > 0", tipos) is None
    assert sizing.glue_expression("dt IN (SELECT max(dt) FROM x)", tipos) is None


def test_filtro_avaliado_partição_por_partição():
    tipos = {"anomes": "int", "dt": "string", "ano": "string"}
    arvore = sizing._parse_filter("anomes BETWEEN 202401 AND 202406 AND dt LIKE '2026%' AND NOT ano IN ('2025')")
    assert sizing.partition_matches(arvore, {"anomes": "202403", "dt": "2026-01-01", "ano": "2026"}, tipos)
    assert not sizing.partition_matches(arvore, {"anomes": "202407", "dt": "2026-01-01", "ano": "2026"}, tipos)
    assert not sizing.partition_matches(arvore, {"anomes": "202403", "dt": "2026-01-01", "ano": "2025"}, tipos)
    # partição em texto comparada com número: compara como número (como o Spark faz)
    assert sizing.partition_matches(sizing._parse_filter("ano >= 2026"), {"ano": "2026"}, tipos)
    assert not sizing.partition_matches(sizing._parse_filter("ano >= 2026"), {"ano": "999"}, tipos)
    with pytest.raises(sizing._Unsupported):
        sizing.partition_matches(sizing._parse_filter("dt >= date_format(current_date, 'y')"), {"dt": "x"}, tipos)


# --------------------------------------------------------------------------- catálogo do Glue
def test_tamanho_vem_das_estatisticas_das_partições():
    glue = _vendas()
    t = _coletar(glue).tables[0]
    assert (t.partitions, t.bytes, t.rows, t.files, t.bytes_source) == (2, 4 * GiB, 20_000_000, 32, "catálogo")
    # o filtro foi pelo Expression do Glue: só as partições escolhidas vieram
    assert glue.calls == [("db", "vendas", FILTRO, None)]
    # leitura nativa: cliente e valor (peso 1 + 1) de 6 (descricao, texto, pesa 3)
    assert (t.columns_used, t.columns_total) == (2, 4)
    assert t.read_fraction == pytest.approx(2 / 6) and t.read_bytes == int(4 * GiB / 3)


def test_glue_recusa_o_expression_e_o_filtro_e_avaliado_aqui():
    glue = _vendas(reject_expressions=True)
    t = _coletar(glue).tables[0]
    assert (t.partitions, t.partitions_total, t.bytes) == (2, 5, 4 * GiB)
    assert glue.calls[-1] == ("db", "vendas", None, None)          # listou todas e filtrou aqui


def test_filtro_com_funcao_nao_inventa_tamanho():
    glue = _vendas()
    t = _coletar(glue, [_ref(filtro="dt >= date_format(current_date, 'yyyy-MM-dd')")]).tables[0]
    assert t.bytes is None and t.partitions is None
    assert any("não consigo avaliar" in n for n in t.notes)


def test_filtro_que_nao_escolhe_nada_avisa():
    t = _coletar(_vendas(), [_ref(filtro="dt = '2030-01-01'")]).tables[0]
    assert (t.partitions, t.bytes) == (0, 0)
    assert any("não escolhe nenhuma partição" in n for n in t.notes)


def test_paginacao_das_partições():
    particoes = [glue_partition(f"{i:05d}", totalSize=MiB) for i in range(1500)]
    glue = FakeCatalog({("db", "vendas"): glue_table(keys=(("lote", "string"),))}, {("db", "vendas"): particoes})
    t = _coletar(glue, [_ref(filtro="")]).tables[0]
    assert (t.partitions, t.partitions_total, t.bytes) == (1500, 1500, 1500 * MiB)
    assert len(glue.calls) == 2


def test_media_da_tabela_quando_as_partições_nao_tem_tamanho():
    particoes = [glue_partition(f"2026-10-0{d}") for d in range(1, 6)]
    glue = FakeCatalog({("db", "vendas"): glue_table(params={"sizeKey": 50 * GiB, "recordCount": 500})},
                       {("db", "vendas"): particoes})
    t = _coletar(glue).tables[0]
    assert (t.bytes, t.rows, t.bytes_source) == (20 * GiB, 200, "média das partições da tabela")


def test_link_do_lake_formation_busca_as_partições_na_origem():
    origem = {"CatalogId": "999999999999", "DatabaseName": "prod_db", "Name": "vendas"}
    particoes = [glue_partition(f"2026-10-0{d}", totalSize=GiB) for d in range(1, 6)]
    glue = FakeCatalog({("db", "vendas"): glue_table(target=origem), ("prod_db", "vendas"): glue_table()},
                       {("prod_db", "vendas"): particoes})
    t = _coletar(glue).tables[0]
    assert (t.partitions, t.bytes) == (2, 2 * GiB)
    assert glue.calls[-1] == ("prod_db", "vendas", FILTRO, "999999999999")


def test_sem_permissao_nas_partições_vira_observacao():
    glue = _vendas(deny=[("db", "vendas")])
    t = _coletar(glue).tables[0]
    assert t.bytes is None and any("AccessDeniedException" in n for n in t.notes)


def test_sem_tempo_nao_consulta_mais_tabelas():
    glue = _vendas()
    t = sizing.collect([_ref()], SQL, glue=glue, budget_s=-1).tables[0]
    assert t.bytes is None and glue.calls == [] and any("acabou o tempo" in n for n in t.notes)


def test_tabela_sem_partição_e_tabela_iceberg():
    glue = FakeCatalog({("db", "dim"): glue_table(keys=(), params={"totalSize": 300 * MiB, "numRows": 1000}),
                        ("db", "ice"): glue_table(params={"table_type": "ICEBERG"})})
    dados = _coletar(glue, [_ref("dim", "db", "dim", ""), _ref("ice", "db", "ice", "")])
    dim, ice = dados.tables
    assert (dim.partitioned, dim.bytes, dim.rows, dim.bytes_source) == (False, 300 * MiB, 1000, "catálogo")
    assert ice.format == "iceberg" and ice.bytes is None and any("iceberg" in n for n in ice.notes)


# --------------------------------------------------------------------------- S3: só buckets da própria conta
def test_s3_mede_só_buckets_da_conta():
    particoes = [glue_partition(f"2026-10-0{d}", f"s3://meu-bucket/eventos/dt=2026-10-0{d}") for d in range(1, 6)]
    compartilhada = [glue_partition(f"2026-10-0{d}", f"s3://bucket-de-outra-conta/x/dt=2026-10-0{d}")
                     for d in range(1, 6)]
    glue = FakeCatalog({("db", "eventos"): glue_table(), ("db", "compartilhada"): glue_table()},
                       {("db", "eventos"): particoes, ("db", "compartilhada"): compartilhada})
    s3 = FakeS3(buckets=["meu-bucket"], objects={"meu-bucket": [
        ("eventos/dt=2026-10-02/part-0.parquet", 100 * MiB), ("eventos/dt=2026-10-02/_SUCCESS", 0),
        ("eventos/dt=2026-10-03/part-0.parquet", 60 * MiB), ("eventos/dt=2026-10-03/part-1.parquet", 40 * MiB),
        ("eventos/dt=2026-10-30/part-0.parquet", 999 * MiB),      # dt=2026-10-3 não pode pegar dt=2026-10-30
    ]})
    eventos, outra = sizing.collect([_ref("eventos", "db", "eventos"), _ref("compartilhada", "db", "compartilhada")],
                                    SQL, glue=glue, s3=s3).tables
    assert (eventos.bytes, eventos.files, eventos.bytes_source) == (200 * MiB, 3, "S3")
    assert outra.bytes is None and any("não é da sua conta" in n for n in outra.notes)
    assert {b for b, _ in s3.listed} == {"meu-bucket"}               # o bucket da outra conta nunca é chamado


def test_s3_sem_permissao_para_listar_os_buckets_nao_chama_nenhum_bucket():
    glue = FakeCatalog({("db", "vendas"): glue_table()}, {("db", "vendas"): [glue_partition("2026-10-02")]})
    s3 = FakeS3(deny_list_buckets=True)
    t = sizing.collect([_ref()], SQL, glue=glue, s3=s3).tables[0]
    assert t.bytes is None and s3.listed == []
    assert any("listar os buckets da sua conta" in n for n in t.notes)


def test_s3_mede_uma_amostra_quando_ha_muitas_partições(monkeypatch):
    monkeypatch.setattr(sizing, "S3_SAMPLE", 10)
    particoes = [glue_partition(f"{i:03d}", f"s3://meu-bucket/t/lote={i:03d}") for i in range(100)]
    glue = FakeCatalog({("db", "vendas"): glue_table(keys=(("lote", "string"),))}, {("db", "vendas"): particoes})
    s3 = FakeS3(buckets=["meu-bucket"],
                objects={"meu-bucket": [(f"t/lote={i:03d}/p.parquet", 10 * MiB) for i in range(100)]})
    t = sizing.collect([_ref(filtro="")], SQL, glue=glue, s3=s3).tables[0]
    assert t.bytes == 1000 * MiB and t.bytes_source == "S3 (amostra de 10 de 100 partições)"
    assert len(s3.listed) == 10


# --------------------------------------------------------------------------- a query
def test_colunas_usadas_com_peso_pelo_tipo():
    perfil = sizing.profile_query(SQL)
    assert sizing.column_share(list(COLUNAS), perfil, "parquet") == (2, pytest.approx(2 / 6))
    assert sizing.column_share(list(COLUNAS), perfil, "csv") == (4, 1.0)          # texto: lê a linha inteira
    estrela = sizing.profile_query("SELECT * FROM db.vendas WHERE dt = '1'")
    assert sizing.column_share(list(COLUNAS), estrela, "parquet") == (4, 1.0)
    contagem = sizing.profile_query("SELECT count(*) FROM db.vendas WHERE dt = '1'")
    assert sizing.column_share(list(COLUNAS), contagem, "parquet") == (0, 0.02)


def test_perfil_da_query():
    leve = sizing.profile_query(SQL.replace(" GROUP BY 1", "").replace(", sum(valor)", ", valor"))
    assert leve.weight == "leve"
    media = sizing.profile_query("SELECT * FROM db.a JOIN db.b ON a.id = b.id CROSS JOIN UNNEST(a.itens) AS t(i)")
    assert (media.joins, media.weight) == (1, "média")                 # o UNNEST não é um JOIN de verdade
    pesada = sizing.profile_query("SELECT id, row_number() OVER (PARTITION BY id ORDER BY dt) FROM db.a")
    assert pesada.windows == 1 and pesada.weight == "pesada"
    assert sizing.profile_query("SELECT count(DISTINCT id) FROM db.a").weight == "pesada"
    assert sizing.profile_query("SELECT * FROM db.a ORDER BY dt").global_order
    assert not sizing.profile_query("SELECT * FROM db.a ORDER BY dt LIMIT 10").global_order
    # o * que só passa por uma CTE que escolhe as colunas não lê a tabela inteira
    assert not sizing.profile_query("WITH x AS (SELECT id, dt FROM db.a) SELECT * FROM x").star
    assert sizing.profile_query("WITH x AS (SELECT * FROM db.a) SELECT * FROM x").star
    insert = sizing.profile_query("INSERT INTO db.destino SELECT * FROM db.a")
    assert insert.star


# --------------------------------------------------------------------------- histórico do Athena
RUNTIME = {"Rows": {"InputRows": 1_200_000_000, "OutputRows": 10},
           "OutputStage": {"OutputBytes": 100, "SubStages": [
               {"OutputBytes": 3 * GiB, "SubStages": [{"OutputBytes": 2 * GiB, "SubStages": []}]}]}}


def test_historico_acha_a_mesma_query_mesmo_com_comentario_e_espacos():
    texto = "-- validação\nselect cliente,  sum(valor)\nFROM db.vendas\nWHERE dt BETWEEN '2026-10-02' AND '2026-10-03'\n" \
            "GROUP BY 1;"
    athena = FakeAthena([athena_execution("a1", "SELECT 1"), athena_execution("a2", texto, scanned=7 * GiB)],
                        runtime={"a2": RUNTIME})
    run, notas = sizing.find_athena_run(athena, SQL)
    assert (run.execution_id, run.match, run.scanned_bytes) == ("a2", "exata", 7 * GiB)
    assert run.exchange_bytes == 5 * GiB and run.input_rows == 1_200_000_000        # 3 GB + 2 GB entre as etapas
    assert set(athena.calls) <= {"ListWorkGroups", "ListQueryExecutions", "BatchGetQueryExecution",
                                 "GetQueryRuntimeStatistics"}
    assert "mesma query" in run.describe() and "7 GB lidos" in run.describe()


def test_historico_aceita_a_mesma_query_com_outra_data():
    outra = SQL.replace("2026-10-02", "2026-09-02").replace("2026-10-03", "2026-09-03")
    run, _ = sizing.find_athena_run(FakeAthena([athena_execution("b1", outra)]), SQL)
    assert run.match == "parecida" and "outros valores" in run.describe()


def test_historico_pula_resultado_reaproveitado_e_prefere_execucao_que_terminou():
    athena = FakeAthena([
        athena_execution("c1", SQL, reused=True, scanned=0),
        athena_execution("c2", SQL, state="FAILED", error="Query exhausted resources at this scale factor"),
        athena_execution("c3", SQL.replace("2026-10-02", "2026-10-01"), when=datetime(2026, 10, 1)),
    ])
    run, _ = sizing.find_athena_run(athena, SQL)
    assert run.execution_id == "c3"                     # parecida que terminou > exata que falhou
    falhou, _ = sizing.find_athena_run(FakeAthena([athena.executions[1]]), SQL)
    assert falhou.state == "FAILED" and not falhou.usable and "exhausted resources" in falhou.describe()


def test_historico_pelo_id_da_execucao():
    athena = FakeAthena([athena_execution("1a2b3c4d-0000-1111-2222-333344445555", "SELECT qualquer")],
                        runtime={"1a2b3c4d-0000-1111-2222-333344445555": RUNTIME})
    run, _ = sizing.find_athena_run(athena, SQL, execution_id="1a2b3c4d-0000-1111-2222-333344445555")
    assert run.match == "id" and run.exchange_bytes == 5 * GiB
    assert "ListQueryExecutions" not in athena.calls                  # com o ID não precisa procurar
    run, notas = sizing.find_athena_run(athena, SQL, execution_id="abc")
    assert run is None and "não parece um ID" in notas[0]


def test_historico_sem_permissao_nos_workgroups_procura_no_primary():
    athena = FakeAthena([athena_execution("d1", SQL)], deny_workgroups=True)
    run, notas = sizing.find_athena_run(athena, SQL)
    assert run.execution_id == "d1" and any("`primary`" in n for n in notas)
    assert any("estatísticas detalhadas" in n for n in notas)        # sem GetQueryRuntimeStatistics


def test_historico_procura_em_todos_os_workgroups_e_avisa_quando_nao_acha():
    athena = FakeAthena([athena_execution("e1", "SELECT 1", workgroup="analytics"), athena_execution("e2", SQL,
                         workgroup="analytics")], workgroups=("primary", "analytics"))
    run, _ = sizing.find_athena_run(athena, SQL)
    assert run.execution_id == "e2" and run.workgroup == "analytics"
    run, notas = sizing.find_athena_run(athena, "SELECT outra FROM db.x")
    assert run is None and "últimas 2 execuções" in notas[-1] and "`analytics`" in notas[-1]


# --------------------------------------------------------------------------- a conta
def _dados(gb_lidos=None, fracao=0.25, perfil=None, run=None, particoes=30, extra=()):
    tabelas = [sizing.TableSize("vendas", "db", "vendas", bytes=None if gb_lidos is None else int(gb_lidos * GiB / fracao),
                                read_fraction=fracao, partitions=particoes, columns_total=40), *extra]
    return sizing.SizingData(tabelas, run, perfil or sizing.QueryProfile())


def test_pouco_dado_fica_no_minimo():
    est = sizing.estimate(_dados(0.5))
    assert est.ok and est.label == "G.1X × 2" and est.dpu == 2 and est.seconds < 60
    assert est.confidence == "média" and est.shuffle_partitions is None


def test_mais_dado_pede_mais_workers_e_o_tempo_desejado_muda_a_conta():
    pequeno, grande = sizing.estimate(_dados(10)), sizing.estimate(_dados(100))
    assert grande.num_workers > pequeno.num_workers
    rapido = sizing.estimate(_dados(100), target_s=60)
    devagar = sizing.estimate(_dados(100), target_s=600)
    assert rapido.num_workers > grande.num_workers > devagar.num_workers
    assert devagar.seconds > rapido.seconds


def test_query_pesada_com_bastante_dado_usa_g2x():
    est = sizing.estimate(_dados(100, perfil=sizing.QueryProfile(windows=1)))
    assert est.worker_type == "G.2X" and any("pesada" in r for r in est.reasons)
    assert est.shuffle_partitions and est.shuffle_partitions % ((est.num_workers - 1) * 8) == 0


def test_nunca_passa_do_limite_de_workers():
    est = sizing.estimate(_dados(5000, perfil=sizing.QueryProfile(joins=1)))
    assert est.num_workers <= 100 and est.worker_type in ("G.4X", "G.8X")
    assert est.shuffle_partitions <= sizing.MAX_SHUFFLE_PARTITIONS


def test_muitas_partições_pede_driver_maior():
    est = sizing.estimate(_dados(5, particoes=20_000))
    assert est.worker_type == "G.2X" and any("driver" in r for r in est.reasons)


def test_execucao_no_athena_manda_na_leitura_e_no_shuffle():
    run = sizing.AthenaRun("x", state="SUCCEEDED", match="exata", scanned_bytes=50 * GiB, exchange_bytes=20 * GiB)
    est = sizing.estimate(_dados(2, run=run))
    assert est.read_bytes == 50 * GiB and est.shuffle_bytes == 20 * GiB and est.confidence == "alta"
    assert "a mesma query no Athena" in est.read_source
    # parecida (outra data) não passa na frente do tamanho das partições escolhidas
    run.match = "parecida"
    est = sizing.estimate(_dados(2, run=run))
    assert est.read_bytes == 2 * GiB and est.confidence == "média"


def test_so_com_o_athena_deduz_o_tamanho_cheio_pelas_colunas():
    run = sizing.AthenaRun("x", state="SUCCEEDED", match="exata", scanned_bytes=10 * GiB)
    est = sizing.estimate(_dados(None, fracao=0.1, run=run))
    assert est.read_bytes == 10 * GiB and est.tasks == 800           # 10 GB / 10% das colunas = 100 GB em arquivos


def test_athena_mais_catalogo_parcial_atribui_o_resto_as_bases_sem_tamanho():
    # vendas: 100 GB em arquivos, lê 2% (2 GB); clientes sem tamanho, lê metade das colunas. O Athena leu 12 GB:
    # 10 GB são de clientes, que então tem ~20 GB em arquivos. O DynamicFrame leria 100 + 20 GB.
    vendas = sizing.TableSize("vendas", "db", "vendas", bytes=100 * GiB, read_fraction=0.02, columns_total=60)
    clientes = sizing.TableSize("clientes", "db", "clientes", read_fraction=0.5, columns_total=3)
    run = sizing.AthenaRun("x", state="SUCCEEDED", match="exata", scanned_bytes=12 * GiB)
    dados = sizing.SizingData([vendas, clientes], run, sizing.QueryProfile(joins=1))
    est = sizing.estimate(dados, reader="glue_context")
    assert est.read_bytes == 12 * GiB and est.tasks == 960            # 120 GB / 128 MB
    assert est.alternative.read_bytes == 120 * GiB


def test_sem_tamanho_de_alguma_base_avisa_e_baixa_a_confianca():
    sem = sizing.TableSize("clientes", "db", "clientes")
    est = sizing.estimate(_dados(10, extra=[sem]))
    assert est.ok and est.confidence == "baixa" and any("`clientes`" in n for n in est.notes)


def test_sem_nenhum_tamanho_nao_estima():
    est = sizing.estimate(_dados(None))
    assert not est.ok and est.label == "—" and est.notes == [sizing.HOW_TO_IMPROVE]


def test_glue_4_com_lake_formation_mostra_a_alternativa_do_dynamicframe():
    est = sizing.estimate(_dados(10, fracao=0.1), reader="glue_context")
    alt = est.alternative
    assert alt is not None and alt.read_bytes == 100 * GiB and alt.num_workers > est.num_workers
    assert sizing.estimate(_dados(10), reader="spark").alternative is None


# --------------------------------------------------------------------------- de ponta a ponta
def test_coleta_junta_catalogo_e_athena():
    athena = FakeAthena([athena_execution("f1", SQL, scanned=GiB)], runtime={"f1": RUNTIME})
    dados = sizing.collect([_ref()], SQL, glue=_vendas(), s3=FakeS3(buckets=["meu-bucket"]), athena=athena)
    assert dados.tables[0].bytes == 4 * GiB and dados.athena.execution_id == "f1"
    est = sizing.estimate(dados)
    assert est.ok and est.confidence == "alta" and est.read_bytes == GiB


def test_formatacao():
    assert sizing.fmt_bytes(1536 * MiB) == "1,5 GB" and sizing.fmt_bytes(0) == "0 B"
    assert sizing.fmt_bytes(250 * GiB) == "250 GB" and sizing.fmt_bytes(None) == "—"
    assert sizing.fmt_count(1_200_000_000) == "1,2 bi" and sizing.fmt_count(950) == "950"
    assert sizing.fmt_duration(41) == "41 s" and sizing.fmt_duration(130) == "~2 min"
    assert sizing.fmt_duration(1500) == "~25 min"
