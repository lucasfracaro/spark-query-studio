"""Queries salvas no S3: busca pelo nome, arquivos do processo do Spark, navegação e leitura (S3 simulado)."""
from datetime import datetime, timezone

import pytest

import metadata
import s3_queries as sq
from fakes import FakeS3

B = "meu-bucket"
CONTEUDO = {
    (B, "queries/financeiro/athena/vendas.sql"): b"SELECT * FROM db.vendas",
    (B, "queries/financeiro/spark/vendas.sql"): b"SELECT * FROM vendas",
    (B, "queries/financeiro/orchestration/vendas.json"): b'{"entradas": []}',
    (B, "queries/financeiro/metadata/vendas.json"): b'{"colunas": [{"coluna": "id", "descricao": "Id"}]}',
    (B, "queries/legado/athena/vendas.sql"): b"SELECT 1",
    (B, "queries/legado/athena/orchestration/vendas.json"): b'{"motor": "athena"}',
    (B, "queries/financeiro/athena/clientes.sql"): b"SELECT * FROM db.clientes",
    (B, "queries/leia-me.md"): b"# queries",
}
NOVO = datetime(2026, 10, 31, 9, 0, tzinfo=timezone.utc)


def _s3(**kw):
    return FakeS3(buckets=[B, "outro-da-conta"], contents=CONTEUDO, **kw)


# --------------------------------------------------------------------------- nomes e caminhos
@pytest.mark.parametrize("texto, esperado", [
    ("s3://meu-bucket/queries", "s3://meu-bucket/queries/"), ("meu-bucket/queries/", "s3://meu-bucket/queries/"),
    ("s3a://meu-bucket", "s3://meu-bucket/"), (" S3://b/x/y ", "s3://b/x/y/"), ("", ""), ("s3://", ""),
])
def test_pasta_raiz_normalizada(texto, esperado):
    assert sq.normalize_uri(texto) == esperado


@pytest.mark.parametrize("texto, esperado", [
    ("vendas", "vendas"), ("db.vendas", "vendas"), ("`vendas`", "vendas"), ("vendas.sql", "vendas"),
    ("vendas.json", "vendas"), ('"db"."vendas"', "vendas"), ("  ", ""),
])
def test_nome_da_base(texto, esperado):
    assert sq.base_name(texto) == esperado
    assert sq.table_file_name(texto) == (f"{esperado}.sql" if esperado else "")


def test_motor_e_tipo_pelo_caminho():
    def arq(chave):
        return sq.S3File(B, chave)
    assert arq("q/financeiro/athena/vendas.sql").athena and arq("q/x/ATHENA/v.sql").engine == "athena"
    assert not arq("q/athena_antigo/v.sql").athena                       # a pasta precisa ser "athena"
    assert arq("q/spark/v.sql").engine == "spark" and arq("q/spark/athena/v.sql").engine == ""
    assert arq("q/spark/v.sql").kind == "query" and arq("q/orchestration/v.json").kind == "orquestracao"
    assert arq("q/orquestracao/v.json").kind == "orquestracao" and arq("q/metadata/v.json").kind == "metadados"
    assert arq("q/outros/v.json").kind == "" and arq("q/spark/v.txt").kind == ""


# --------------------------------------------------------------------------- busca
def test_busca_pelo_nome_da_tabela_com_o_athena_primeiro():
    s3 = _s3(modified={(B, "queries/legado/athena/vendas.sql"): NOVO})
    indice = sq.S3Queries(s3).index("s3://meu-bucket/queries")
    assert indice.complete and indice.scanned == len(CONTEUDO) and indice.root == "s3://meu-bucket/queries/"
    assert "queries/leia-me.md" not in [f.key for f in indice.files]
    achados = indice.find("db.VENDAS")
    assert [f.key for f in achados] == ["queries/legado/athena/vendas.sql",         # Athena, o mais novo primeiro
                                        "queries/financeiro/athena/vendas.sql",
                                        "queries/financeiro/spark/vendas.sql"]
    assert indice.find("nao_existe") == [] and indice.find("") == []


def test_arquivos_do_processo_do_spark():
    indice = sq.S3Queries(_s3()).index("s3://meu-bucket/queries/")
    arquivos = indice.find_process("vendas")
    assert [f.key for f in arquivos["query"]] == ["queries/financeiro/spark/vendas.sql"]   # a do Athena não entra
    assert [f.key for f in arquivos["orquestracao"]] == ["queries/financeiro/orchestration/vendas.json",
                                                         "queries/legado/athena/orchestration/vendas.json"]
    assert [f.key for f in arquivos["metadados"]] == ["queries/financeiro/metadata/vendas.json"]
    assert indice.find_process("clientes") == {"query": [], "orquestracao": [], "metadados": []}


def test_so_nos_buckets_da_conta():
    s3 = _s3()
    with pytest.raises(sq.S3QueryError, match="não é da conta logada"):
        sq.S3Queries(s3).index("s3://bucket-de-outra-conta/queries/")
    assert s3.listed == []                                               # nenhuma chamada ao bucket de fora
    with pytest.raises(sq.S3QueryError, match="ListAllMyBuckets"):
        sq.S3Queries(_s3(deny_list_buckets=True)).index("s3://meu-bucket/")


def test_pasta_grande_para_no_tempo_limite():
    s3 = _s3(page_size=2)
    indice = sq.S3Queries(s3).index("s3://meu-bucket/", budget_s=0)
    assert not indice.complete and indice.scanned == 2 and len(s3.listed) == 1
    completo = sq.S3Queries(_s3(page_size=2)).index("s3://meu-bucket/")
    assert completo.complete and completo.scanned == len(CONTEUDO)


def test_indice_vale_alguns_minutos():
    indice = sq.QueryIndex("s3://b/", [], 0, True, built_at=0)
    assert not indice.fresh() and sq.QueryIndex("s3://b/", [], 0, True, built_at=__import__("time").time()).fresh()


# --------------------------------------------------------------------------- navegação
def test_pasta_mostra_subpastas_e_queries():
    s3 = _s3()
    s3.objects[B].append(("queries/financeiro/", 0))                    # marcador de pasta do console
    cliente = sq.S3Queries(s3)
    raiz = cliente.folder(B, "queries/")
    assert raiz.folders == ["financeiro", "legado"] and [f.name for f in raiz.files] == []
    assert raiz.others == 1 and not raiz.truncated and raiz.uri == "s3://meu-bucket/queries/"
    pasta = cliente.folder(B, "queries/financeiro/athena/")
    assert [f.name for f in pasta.files] == ["clientes.sql", "vendas.sql"] and pasta.folders == []
    assert [f.name for f in cliente.folder(B, "queries/financeiro/athena/", "ven").files] == ["vendas.sql"]
    jsons = cliente.folder(B, "queries/financeiro/metadata/", extensions=sq.PROCESS_EXTENSIONS)
    assert [f.name for f in jsons.files] == ["vendas.json"]
    assert cliente.folder(B, "queries/financeiro/metadata/").others == 1     # sem .json, o JSON não é query


def test_pasta_com_itens_demais_e_cortada():
    cliente = sq.S3Queries(FakeS3(buckets=[B], objects={B: [(f"q/arquivo_{i:03}.sql", 10) for i in range(30)]},
                                  page_size=10))
    pasta = cliente.folder(B, "q/", max_items=15)
    assert pasta.truncated and len(pasta.files) == 15


# --------------------------------------------------------------------------- leitura
def test_le_o_arquivo_em_utf8_ou_windows():
    contents = {(B, "a.sql"): "\ufeffSELECT 'ação'".encode(), (B, "b.sql"): "SELECT 'ação'".encode("cp1252")}
    cliente = sq.S3Queries(FakeS3(buckets=[B], contents=contents))
    assert cliente.read(B, "a.sql") == "SELECT 'ação'" and cliente.read(B, "b.sql") == "SELECT 'ação'"


def test_leitura_recusa_arquivo_grande_negado_ou_de_fora():
    grande = {(B, "grande.sql"): b"x" * (sq.MAX_QUERY_BYTES + 1), (B, "negado.sql"): b"SELECT 1"}
    s3 = FakeS3(buckets=[B], contents=grande, deny_get={(B, "negado.sql")})
    cliente = sq.S3Queries(s3)
    with pytest.raises(sq.S3QueryError, match="grande demais"):
        cliente.read(B, "grande.sql")
    with pytest.raises(sq.S3QueryError, match="s3:GetObject"):
        cliente.read(B, "negado.sql")
    with pytest.raises(sq.S3QueryError, match="não é da conta logada"):
        cliente.read("de-fora", "x.sql")
    assert ("de-fora", "x.sql") not in s3.read


# --------------------------------------------------------------------------- descrições do JSON de metadados
def test_descricoes_das_colunas_em_formatos_diferentes():
    assert metadata.descriptions_from_json(metadata.dictionary_to_json(
        [{"coluna": "ID", "descricao": "Identificador"}, {"coluna": "valor", "descricao": ""}])) == {"id": "Identificador"}
    outro = '{"table": {"name": "vendas", "schema": {"fields": [{"name": "dt", "comment": "Data da venda"}]}}}'
    assert metadata.descriptions_from_json(outro) == {"dt": "Data da venda"}
    with pytest.raises(ValueError, match="JSON válido"):
        metadata.descriptions_from_json("{ isso não é json")
