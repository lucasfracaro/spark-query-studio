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
