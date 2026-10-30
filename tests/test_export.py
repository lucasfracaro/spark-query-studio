import pandas as pd
import pytest

from export import FORMATS, to_csv_bytes


def _df():
    return pd.DataFrame([[1, 10.5, "1.50", "a.b", 1e-07, None], [2, None, "-0.0000000000", "c", 2.0, True]],
                        columns=["id", "valor", "preco", "texto", "pequeno", "flag"])


TIPOS = ["int", "double", "decimal(10,2)", "string", "double", "boolean"]


def test_excel_em_portugues_usa_virgula_decimal():
    texto = to_csv_bytes(_df(), TIPOS, "excel_br").decode("utf-8-sig")
    assert texto.splitlines() == [
        "id;valor;preco;texto;pequeno;flag",
        "1;10,5;1,50;a.b;0,0000001;",                     # texto não é mexido
        "2;;-0,0000000000;c;2,0;True",
    ]


def test_padrao_internacional_mantem_ponto():
    texto = to_csv_bytes(_df(), TIPOS, "padrao").decode("utf-8-sig")
    assert texto.splitlines()[1] == "1,10.5,1.50,a.b,1e-07,"


def test_tabulacao():
    assert to_csv_bytes(_df(), TIPOS, "tab").decode("utf-8-sig").splitlines()[0] == "id\tvalor\tpreco\ttexto\tpequeno\tflag"


def test_bom_para_o_excel_reconhecer_acentos():
    assert to_csv_bytes(pd.DataFrame({"ç": ["ã"]}), ["string"]).startswith("﻿".encode())


def test_colunas_com_nome_repetido():
    df = pd.DataFrame([[1.5, 2.5]], columns=["x", "x"])
    assert to_csv_bytes(df, ["double", "double"], "excel_br").decode("utf-8-sig").splitlines()[1] == "1,5;2,5"


def test_formato_desconhecido():
    with pytest.raises(ValueError):
        to_csv_bytes(_df(), TIPOS, "xls")
    assert set(FORMATS) == {"excel_br", "padrao", "tab"}


# --------------------------------------------------------------------------- resultado completo
from decimal import Decimal  # noqa: E402

from code_builder import InputSource, Orchestration  # noqa: E402
from export import ExportError, fetch_full_result, rows_to_csv_bytes  # noqa: E402
from tests.fakes import FakeSpark, run_generated  # noqa: E402

LINHAS = [(1, "a;b", None, Decimal("1.50"), True), (None, 'x "q"', 1e-07, Decimal("-0.0000000000"), False),
          (3, "ç", float("inf"), None, None), (4, "d", 2.0, Decimal("10"), True)]
COLUNAS = [("id", "int"), ("texto", "string"), ("valor", "double"), ("preco", "decimal(10,2)"), ("flag", "boolean")]
ORCH = Orchestration(inputs=[InputSource("v", "catalog", "db.v")], limit=2)


def _rodar_com(spark, chamadas=None):
    def run(codigo, on_status):
        if chamadas is not None:
            chamadas.append(codigo)
        return run_generated(codigo, spark)
    return run


def _como_na_tela(v):
    return format(v, "f") if isinstance(v, Decimal) else v        # a tela recebe Decimal como texto (JSON)


@pytest.mark.parametrize("formato", list(FORMATS))
def test_resultado_completo_sai_igual_ao_csv_da_tela(formato):
    spark = FakeSpark(rows=LINHAS, columns=COLUNAS)
    dados, info = fetch_full_result(_rodar_com(spark), "SELECT * FROM v", ORCH, formato=formato, chunk_bytes=16)
    assert info["rows"] == 4 and not info["truncated"] and info["chunks"] > 1     # veio em várias partes
    tela = rows_to_csv_bytes([n for n, _ in COLUNAS], [[_como_na_tela(v) for v in linha] for linha in LINHAS],
                             [t for _, t in COLUNAS], formato)
    assert dados == tela                       # todas as linhas (não só as 2 da tela), no mesmo formato e com BOM
    assert ("toLocalIterator",) in spark.last_session.calls


def test_csv_da_tela_mantem_inteiro_com_nulo_como_inteiro():
    texto = rows_to_csv_bytes(["id", "v"], [[1, None], [None, 2.5]], ["bigint", "double"]).decode("utf-8-sig")
    assert texto.splitlines() == ["id;v", "1;", ";2,5"]             # e não 1.0 (o pandas viraria float)


def test_resultado_completo_para_no_limite_do_excel():
    spark = FakeSpark(rows=LINHAS, columns=COLUNAS)
    dados, info = fetch_full_result(_rodar_com(spark), "SELECT * FROM v", ORCH, max_rows=2)
    assert info["rows"] == 2 and info["truncated"]
    assert len(dados.decode("utf-8-sig").splitlines()) == 3          # cabeçalho + 2 linhas


def test_resultado_completo_com_erro_na_query_ou_numa_parte():
    with pytest.raises(ExportError, match="AnalysisException"):
        fetch_full_result(lambda c, s: {"ok": False, "error": "AnalysisException: tabela"}, "SELECT 1", ORCH)
    spark = FakeSpark(rows=LINHAS, columns=COLUNAS)
    real = _rodar_com(spark)
    respostas = iter([None, {"ok": False, "error": "FileNotFoundError: sumiu"}])

    def run(codigo, on_status):
        resposta = next(respostas)
        return real(codigo, on_status) if resposta is None else resposta
    with pytest.raises(ExportError, match="sumiu"):
        fetch_full_result(run, "SELECT * FROM v", ORCH, chunk_bytes=16)


def test_csv_completo_sai_do_resultado_guardado_sem_rodar_a_query_de_novo():
    from code_builder import build_spark_code
    spark, ns = FakeSpark(rows=LINHAS, columns=COLUNAS), {}
    tela = run_generated(build_spark_code("SELECT * FROM v", ORCH, count=True), spark, ns)
    sessoes = len(spark.sessions)
    codigos = []

    def run(codigo, on_status):
        codigos.append(codigo)
        return run_generated(codigo, spark, ns)

    dados, info = fetch_full_result(run, "SELECT * FROM v", ORCH, result_key=tela["result_key"], chunk_bytes=16)
    assert info["rows"] == 4 and info["rerun"] is False
    assert len(spark.sessions) == sessoes                  # nenhuma execução nova da query
    assert "SELECT * FROM v" not in codigos[0]
    # o mesmo arquivo que uma execução nova geraria
    de_novo, info2 = fetch_full_result(_rodar_com(FakeSpark(rows=LINHAS, columns=COLUNAS)), "SELECT * FROM v", ORCH)
    assert dados == de_novo and info2["rerun"] is True


def test_csv_completo_roda_de_novo_quando_o_resultado_guardado_e_de_outra_execucao():
    from code_builder import build_spark_code
    spark, ns = FakeSpark(rows=LINHAS, columns=COLUNAS), {}
    run_generated(build_spark_code("SELECT * FROM v", ORCH, count=True), spark, ns)
    dados, info = fetch_full_result(lambda c, s: run_generated(c, spark, ns), "SELECT * FROM v", ORCH,
                                    result_key="outra_chave")
    assert info["rerun"] is True and info["rows"] == 4


def test_arquivo_do_driver_e_apagado_depois_da_ultima_parte():
    import glob
    import os
    import tempfile
    antes = set(glob.glob(os.path.join(tempfile.gettempdir(), "sqs_export_*.csv.gz")))
    spark = FakeSpark(rows=LINHAS, columns=COLUNAS)
    fetch_full_result(_rodar_com(spark), "SELECT * FROM v", ORCH, chunk_bytes=16)
    assert set(glob.glob(os.path.join(tempfile.gettempdir(), "sqs_export_*.csv.gz"))) == antes
