"""Coluna não encontrada: a mensagem de cada versão do Spark, a linha da query e o motivo provável."""
import pytest

import column_errors as ce
from code_builder import InputSource, split_statements_with_positions
from fakes import AccessDeniedException, FakeCatalog, glue_table
from spark_errors import error_position, locate, locate_error

COLUNAS = ["id", "codigo9", "codigo14", "nome", "grupo", "subgrupo", "year", "month", "day"]
NOVAS = ["codigo9_str", "codigo14_str", "grupo_str", "subgrupo_str"]
LISTA_33 = ", ".join(f"clientes.{c}" for c in COLUNAS)
ERRO_33 = (f"AnalysisException: Column 'codigo9_str' does not exist. Did you mean one of the following? [{LISTA_33}]; "
           "line 3 pos 2;\n'Project ['id, 'codigo9_str]\n+- SubqueryAlias clientes\n")
ERRO_35 = ("AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with "
           "name `codigo9_str` cannot be resolved. Did you mean one of the following? [`clientes`.`codigo9`, "
           "`clientes`.`codigo14`, `clientes`.`nome`].; line 3 pos 2;\n'Project ['codigo9_str]\n")
ERRO_41 = ("AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with "
           "name `f`.`codigo9_str` cannot be resolved. Did you mean one of the following? [`f`.`codigo9`, `f`.`id`]. "
           "SQLSTATE: 42703; line 3 pos 2;\n'Project ['f.codigo9_str]\n")
QUERY = """SELECT
  id,
  codigo9_str,
  codigo14_str, grupo_str, -- comentário com subgrupo_str não conta
  nome
FROM clientes
WHERE year = 2026"""
ENTRADA = [InputSource("clientes", "catalog", "produtor.clientes")]


def _sessao(colunas=COLUNAS, catalogo=COLUNAS, leitura="nativa"):
    return [{"alias": "clientes", "kind": "catalog", "source": "produtor.clientes", "read": leitura,
             "columns": list(colunas), "catalog": None if catalogo is None else list(catalogo)}]


def _usuario(colunas):
    return {"clientes": {"source": "produtor.clientes", "columns": list(colunas)}}


def _diagnosticar(erro=ERRO_33, query=QUERY, **kw):
    kw.setdefault("orch_inputs", ENTRADA)
    return ce.diagnose(erro, query, query, **kw)


# --------------------------------------------------------------------------- a mensagem de cada versão do Spark
@pytest.mark.parametrize("erro, nome, qualificador, completa, candidatos", [
    (ERRO_33, "codigo9_str", "", True, len(COLUNAS)),                                       # Glue 4.0
    (ERRO_35, "codigo9_str", "", False, 3),                                                 # Glue 5.x
    (ERRO_41, "f.codigo9_str", "f", False, 2),                                              # Glue 6.0
    ("AnalysisException: cannot resolve '`t.x`' given input columns: [t.a, t.b]; line 1 pos 7;", "t.x", "t", True, 2),
    ("[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function parameter with name `x` cannot be "
     "resolved.  SQLSTATE: 42703; line 1 pos 7", "x", "", False, 0),
    ("Column 'letter.lower' does not exist. Did you mean one of the following? [letters.letter_json]; line 4 pos 19;",
     "letter.lower", "letter", True, 1),
    ("Column 'x' does not exist. Did you mean one of the following? []; line 1 pos 7;", "x", "", True, 0),
])
def test_mensagens_de_cada_versao_do_spark(erro, nome, qualificador, completa, candidatos):
    uc = ce.parse(erro)
    assert (uc.name, uc.qualifier, uc.full_list, len(uc.candidates)) == (nome, qualificador, completa, candidatos)
    assert uc.column == nome.split(".")[-1]


def test_nomes_com_crases_pontos_e_virgulas():
    uc = ce.parse("[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `t`.`a.b` cannot be "
                  "resolved. Did you mean one of the following? [`spark_catalog`.`db`.`t`.`a,c`, `t`.`x``y`].; "
                  "line 1 pos 7;")
    assert (uc.qualifier, uc.column) == ("t", "a.b")                   # `a.b` é um nome só
    assert uc.candidates == [("t", "a,c"), ("t", "x`y")]


def test_campo_de_struct():
    uc = ce.parse("[UNRESOLVED_FIELD.WITH_SUGGESTION] A field with name `cidade` cannot be resolved with the "
                  "struct-type column `c`.`endereco`. Did you mean one of the following? [`cidades`, `uf`].; "
                  "line 1 pos 7;")
    assert (uc.column, uc.struct, [c for _, c in uc.candidates]) == ("cidade", "c.endereco", ["cidades", "uf"])
    d = ce.diagnose("No such struct field cidad in cidade, uf; line 1 pos 7", "SELECT e.cidad FROM t")
    assert d.kind == "campo" and d.similar == ["cidade"]


def test_outros_erros_nao_sao_de_coluna():
    for erro in ("[TABLE_OR_VIEW_NOT_FOUND] The table or view `x` cannot be found.", "Path does not exist: s3://b/",
                 "cannot resolve 'x + y' due to data type mismatch", ""):
        assert ce.parse(erro) is None and ce.diagnose(erro) is None


# --------------------------------------------------------------------------- a linha da query
def test_posicao_nos_formatos_do_spark():
    assert error_position(ERRO_33) == (3, 2)
    assert error_position("Syntax error at or near 'FROM'(line 1, pos 9)\n\n== SQL ==") == (1, 9)
    assert error_position("[DIVIDE_BY_ZERO] ...\n== SQL (line 2, position 8) ==\nx / 0") == (2, 7)   # começa do 1
    assert error_position('File "<stdin>", line 245, in <module>') is None


def test_linha_do_editor_com_varios_comandos():
    sql = "-- teste\nSET a = 1;  SELECT 1;\n\nSELECT\n  id,\n  codigo9_str\nFROM t"
    # o comentário do início faz parte do 1º comando: o Spark conta as linhas a partir dele
    assert [(linha, coluna) for linha, coluna, _ in split_statements_with_positions(sql)] == [(1, 0), (2, 12), (4, 0)]
    local = locate(sql, 2, 3, 2)
    assert (local.line, local.column, local.statement, local.statements) == (6, 2, 3, 3)
    assert local.describe() == "linha 6, coluna 3 (comando 3 de 3)"
    assert local.snippet("codigo9_str") == "5 |   id,\n6 |   codigo9_str\n  |   " + "^" * len("codigo9_str")
    meio = locate(sql, 1, 1, 7)                                     # comando que começa no meio da linha
    assert (meio.line, meio.column) == (2, 19) and meio.text[19] == "1"
    assert locate(sql, None, 1, 0) is None and locate(sql, 7, 1, 0) is None and locate(sql, 0, 9, 0) is None


def test_trecho_marca_o_nome_inteiro_com_alias_e_crases():
    local = locate("SELECT f.id,\n\tF . `codigo9_str` FROM f", 0, 2, 1)
    assert local.snippet("f.codigo9_str").splitlines()[-1] == "  |     " + "^" * len("F . `codigo9_str`")   # tab: 4
    assert local.snippet("outro").splitlines()[-1].endswith("^") and local.snippet().count("^") == 1


def test_linha_muito_longa_mostra_o_pedaco_do_erro():
    sql = "SELECT " + ", ".join(f"coluna_{i}" for i in range(60)) + ", sumida FROM t"
    local = locate_error(f"Column 'sumida' does not exist. Did you mean one of the following? []; line 1 pos "
                         f"{sql.index('sumida')};", sql, 0)
    linha, marca = local.snippet("sumida").splitlines()
    assert len(linha) < 130 and linha.startswith("1 | …") and marca.index("^") == linha.index("sumida")
    assert marca.count("^") == len("sumida")


def test_sem_saber_o_comando_so_localiza_query_de_um_comando():
    assert locate_error(ERRO_33, QUERY, None).line == 3
    assert locate_error(ERRO_33, "SELECT 1; " + QUERY, None) is None
    assert locate_error("erro sem posição", QUERY, 0) is None


# --------------------------------------------------------------------------- o motivo
def test_role_sem_permissao_nas_colunas_lista_todas_que_a_query_usa():
    d = _diagnosticar(inputs=_sessao(), user_catalog=_usuario(COLUNAS + NOVAS + ["outra_str"]), role="SessaoGlue",
                      glue=True)
    assert d.kind == "role" and d.title == "A role da sessão não enxerga a coluna `codigo9_str`"
    assert "`SessaoGlue`" in d.cause and "peça SELECT no Lake Formation" in d.cause
    assert d.missing_too == ["codigo14_str", "grupo_str"]       # o subgrupo só aparece num comentário
    assert "A query usa 3 colunas que a role não vê" in d.cause and "Ao todo, a role não vê 5 colunas" in d.cause
    assert d.scope == "Nesse ponto da query, o Spark enxerga: `clientes` (`produtor.clientes`, 9 colunas)."


def test_role_sem_permissao_sem_saber_o_catalogo_da_role():
    d = _diagnosticar(inputs=_sessao(catalogo=None), user_catalog=_usuario(COLUNAS + NOVAS), glue=True)
    assert d.kind == "role" and "provavelmente é falta de permissão" in d.cause


def test_coluna_no_catalogo_que_a_leitura_nao_trouxe():
    d = _diagnosticar(inputs=_sessao(catalogo=COLUNAS + NOVAS, leitura="dynamicframe"), glue=True)
    assert d.kind == "arquivos" and d.title == "A leitura de `clientes` não trouxe a coluna `codigo9_str`"
    assert "como DynamicFrame" in d.cause and "partições" in d.cause
    assert d.missing_too == ["codigo14_str", "grupo_str"] and "que faltam pelo mesmo motivo" in d.cause
    d = _diagnosticar(inputs=_sessao(catalogo=COLUNAS + ["codigo9_str"], leitura="spark"))
    assert d.kind == "arquivos" and "GlueContext" not in d.cause and d.missing_too == []


def test_nome_errado_mostra_os_parecidos():
    erro = ERRO_33.replace("codigo9_str", "codigo_9")
    d = _diagnosticar(erro, QUERY.replace("codigo9_str", "codigo_9"), inputs=_sessao(), user_catalog=_usuario(COLUNAS),
                      glue=True)
    assert d.kind == "nao_existe" and d.title == "A coluna `codigo_9` não existe"
    assert d.similar[0] == "codigo9" and "nem no catálogo com a sua permissão" in d.cause
    d = _diagnosticar(erro, inputs=_sessao(), glue=True)          # sem conferir o seu usuário: pode ser permissão
    assert d.title == "A sessão não encontrou a coluna `codigo_9`" and "o motivo provável é permissão" in d.cause
    negado = {"clientes": {"source": "produtor.clientes", "error": "AccessDeniedException"}}
    d = _diagnosticar(erro, inputs=_sessao(), user_catalog=negado, glue=True)
    assert "Não deu para conferir as colunas com a sua permissão (AccessDeniedException)" in d.cause
    d = _diagnosticar(erro, inputs=_sessao())                      # Spark local: sem Lake Formation
    assert d.title == "A coluna `codigo_9` não existe" and "permissão" not in d.cause


def test_sem_as_listas_da_sessao_usa_so_a_mensagem():
    d = _diagnosticar(ERRO_35, orch_inputs=[], glue=True)
    assert d.kind == "desconhecido" and d.similar[0] == "codigo9" and "permissão" in d.cause
    assert d.scope == ""                                            # no Spark 3.4+ a lista não é a de todas


def test_coluna_de_outra_entrada_ou_que_nao_chega_ao_trecho():
    entradas = [InputSource("a", "catalog", "db.a"), InputSource("b", "catalog", "db.b")]
    sessao = [{"alias": "a", "kind": "catalog", "source": "db.a", "columns": ["id", "x"], "catalog": ["id", "x"]},
              {"alias": "b", "kind": "catalog", "source": "db.b", "columns": ["id", "y"], "catalog": ["id", "y"]}]
    sql = "SELECT a.y FROM a JOIN b ON a.id = b.id"
    erro = "Column 'a.y' does not exist. Did you mean one of the following? [a.id, a.x, b.id, b.y]; line 1 pos 7;"
    d = ce.diagnose(erro, sql, sql, sessao, None, entradas)
    assert d.kind == "fora_do_alcance" and d.title == "A coluna `y` é de outra tabela" and "`b.y`" in d.cause
    sql = "WITH c AS (SELECT id FROM b) SELECT y FROM c"
    erro = "Column 'y' does not exist. Did you mean one of the following? [c.id]; line 1 pos 36;"
    d = ce.diagnose(erro, sql, sql, sessao, None, entradas)
    assert d.title == "A coluna `y` não chega a esse ponto da query" and "inclua a coluna no SELECT dela" in d.cause
    assert d.scope == "Nesse ponto da query, o Spark enxerga: `c` (CTE, 1 coluna)."


def test_alias_que_nao_existe_no_trecho():
    sql = "SELECT v.valor FROM vendas x"
    erro = "Column 'v.valor' does not exist. Did you mean one of the following? [x.id, x.valor]; line 1 pos 7;"
    sessao = [{"alias": "vendas", "kind": "s3", "source": "s3://b/v/", "columns": ["id", "valor"]}]
    d = ce.diagnose(erro, sql, sql, sessao, None, [InputSource("vendas", "s3", "s3://b/v/")])
    assert d.kind == "alias" and "`x`" in d.cause and "use o alias que esse trecho dá a ela" in d.cause


def test_nome_criado_no_proprio_select():
    sql = "SELECT valor * 2 AS dobro FROM vendas WHERE dobro > 10"
    erro = "Column 'dobro' does not exist. Did you mean one of the following? [vendas.valor]; line 1 pos 44;"
    d = ce.diagnose(erro, sql, sql, [{"alias": "vendas", "kind": "s3", "source": "s3://b/", "columns": ["valor"]}])
    assert d.kind == "alias_do_select" and "WHERE" in d.cause
    erro = ("Column 'letter.lower' does not exist. Did you mean one of the following? [letters.letter_json]; "
            "line 1 pos 50;")
    sql = "SELECT from_json(j, 'lower STRING') AS letter, letter.lower FROM letters"
    assert ce.diagnose(erro, sql, sql).kind == "alias_do_select"


# --------------------------------------------------------------------------- o que o seu usuário vê no catálogo
def test_colunas_do_catalogo_com_a_permissao_do_usuario():
    link = glue_table(columns=(), keys=(), target={"CatalogId": "999999999999", "DatabaseName": "prod", "Name": "f"})
    link.pop("StorageDescriptor")
    glue = FakeCatalog({("db", "vendas"): glue_table(), ("db", "link"): link,
                        ("prod", "f"): glue_table(columns=(("x", "int"),), keys=(("dt", "string"),))})
    entradas = [InputSource("vendas", "catalog", "db.vendas"), InputSource("f", "catalog", "link"),
                InputSource("arquivo", "s3", "s3://b/"), InputSource("sumida", "catalog", "db.sumida")]
    saida = ce.user_catalog_columns(glue, entradas, default_database="db")
    assert saida["vendas"] == {"source": "db.vendas", "columns": ["id", "valor", "descricao", "dt"]}
    assert saida["f"] == {"source": "db.link", "columns": ["x", "dt"]}       # link do Lake Formation: a origem
    assert saida["sumida"]["error"] == "EntityNotFoundException" and "arquivo" not in saida


def test_codigo_de_erro_da_aws():
    class Negado(AccessDeniedException):
        response = {"Error": {"Code": "AccessDeniedException", "Message": "Insufficient Lake Formation permission"}}

    class Glue:
        def get_table(self, **_):
            raise Negado("negado")
    saida = ce.user_catalog_columns(Glue(), [InputSource("v", "catalog", "db.v")])
    assert saida == {"v": {"source": "db.v", "error": "AccessDeniedException"}}


def test_so_confere_o_catalogo_quando_nenhuma_entrada_trouxe_a_coluna():
    assert ce.needs_user_catalog({"error": ERRO_33, "inputs": _sessao()})
    assert not ce.needs_user_catalog({"error": ERRO_33, "inputs": _sessao(COLUNAS + ["codigo9_str"])})
    assert not ce.needs_user_catalog({"error": "[TABLE_OR_VIEW_NOT_FOUND] x"})
