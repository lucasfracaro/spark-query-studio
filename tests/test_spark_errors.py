from spark_errors import summarize

PLANO = """AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `valr` cannot be resolved. Did you mean one of the following? [`v`.`valor`, `v`.`id`].; line 1 pos 7;
'Project ['valr]
+- SubqueryAlias v
   +- View (`v`, [id#1,valor#2])
      +- Relation [id#1,valor#2] csv
"""


def test_coluna_nao_encontrada_sem_o_plano():
    r = summarize(PLANO)
    assert r.title == "Coluna não encontrada" and r.error_class == "UNRESOLVED_COLUMN"
    assert "`valr` cannot be resolved" in r.detail and "SubqueryAlias" not in r.detail


def test_coluna_do_spark_3_3_com_centenas_de_sugestoes_vira_lista_curta():
    lista = ", ".join(f"t.c{i}" for i in range(140))
    r = summarize(f"AnalysisException: Column 'x' does not exist. Did you mean one of the following? [{lista}]; "
                  "line 1 pos 7;\n'Project ['x]\n")
    assert r.title == "Coluna não encontrada"
    assert "[t.c0, t.c1, t.c2, t.c3, t.c4, t.c5, t.c6, t.c7, … mais 132]; line 1 pos 7;" in r.detail
    assert "t.c8," not in r.detail


def test_erro_de_sintaxe_mostra_o_trecho():
    erro = ("ParseException: \n[PARSE_SYNTAX_ERROR] Syntax error at or near 'FRM'.(line 1, pos 9)\n\n"
            "== SQL ==\nSELECT * FRM t\n---------^^^\n")
    r = summarize(erro)
    assert r.title == "Erro de sintaxe no SQL"
    assert "== SQL ==" in r.detail and "^^^" in r.detail


def test_java_resume_a_pilha_e_mostra_a_causa():
    erro = ("Py4JJavaError: An error occurred while calling o85.collectToPython.\n"
            ": org.apache.spark.SparkException: Job aborted due to stage failure: Task 3 failed 4 times\n"
            "\tat org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2672)\n"
            "\tat scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)\n"
            "Caused by: java.lang.ArithmeticException: [DIVIDE_BY_ZERO] Division by zero.\n"
            "\tat org.apache.spark.sql.errors.QueryExecutionErrors$.divideByZeroError\n")
    r = summarize(erro)
    assert r.title == "Divisão por zero" and "try_divide" in r.hint
    assert "DAGScheduler" not in r.detail and "Caused by: java.lang.ArithmeticException" in r.detail


def test_s3_de_outra_conta_sugere_lake_formation():
    erro = ("Py4JJavaError: An error occurred while calling o120.showString.\n"
            ": java.nio.file.AccessDeniedException: s3://bucket-da-outra-conta/vendas/dt=2026-10-01/part-0.parquet: "
            "getFileStatus on s3://bucket-da-outra-conta/vendas: com.amazonaws.services.s3.model.AmazonS3Exception: "
            "Access Denied (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied)\n\tat x.y(z)\n")
    r = summarize(erro)
    assert r.title == "Acesso negado ao S3" and "Lake Formation" in r.hint


def test_sem_permissao_no_lake_formation():
    r = summarize("AnalysisException: Insufficient Lake Formation permission(s) on vendas")
    assert r.title == "Sem permissão no Lake Formation" and "GetDataAccess" in r.hint


def test_sem_permissao_no_catalogo():
    erro = ("AnalysisException: org.apache.hadoop.hive.ql.metadata.HiveException: Unable to fetch table vendas. "
            "User: arn:aws:sts::1:assumed-role/Glue/x is not authorized to perform: glue:GetTable on resource")
    assert summarize(erro).title == "Sem permissão no Glue Data Catalog"


def test_falta_de_memoria():
    r = summarize("Py4JJavaError: x\n: org.apache.spark.SparkException: ExecutorLostFailure (executor 3 exited "
                  "caused by one of the running tasks) Reason: Container killed by YARN for exceeding memory limits")
    assert r.title == "Faltou memória nos workers" and "G.2X" in r.hint


def test_erro_desconhecido_vira_a_primeira_linha():
    r = summarize("ValueError: alguma coisa estranha aconteceu\nmais detalhes")
    assert r.title == "alguma coisa estranha aconteceu" and r.hint is None
    assert summarize("").title == "Erro na execução"


def test_glue_context_fora_do_glue():
    r = summarize("ModuleNotFoundError: No module named 'awsglue'")
    assert r.title == "GlueContext indisponível"


def test_filtro_que_nao_escolhe_particoes():
    r = summarize("RuntimeError: O filtro da entrada v (valor > 10) não pôde ser usado para escolher as partições "
                  "de db.v. Na leitura pelo GlueContext...")
    assert r.title == "O filtro não serve para escolher partições" and "WHERE" in r.hint
