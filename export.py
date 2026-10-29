"""
Exportação do resultado em CSV.

"Excel em português" usa ; como separador e vírgula como separador decimal: é o que o Excel em
português espera ao abrir o arquivo com dois cliques. Com ponto decimal, o Excel em português lê
10.5 como texto (ou como data).
"""
from __future__ import annotations

import math

import pandas as pd

FORMATS = {
    "excel_br": "Excel em português (; e vírgula decimal)",
    "padrao": "Padrão internacional (, e ponto decimal)",
    "tab": "Separado por tabulação (ponto decimal)",
}
_SEPARADOR = {"excel_br": ";", "padrao": ",", "tab": "\t"}
_TIPOS_DECIMAIS = ("decimal", "double", "float", "real")


def _decimal_com_virgula(valor):
    if valor is None or (isinstance(valor, float) and math.isnan(valor)):
        return None
    if isinstance(valor, bool):
        return valor
    if isinstance(valor, float):
        if math.isinf(valor):
            return "Infinito" if valor > 0 else "-Infinito"
        texto = repr(valor)
        if "e" in texto or "E" in texto:                 # 1e-07 -> 0,0000001 (sem notação científica)
            texto = f"{valor:.20f}".rstrip("0").rstrip(".")
        return texto.replace(".", ",")
    if isinstance(valor, (int, str)):
        return str(valor).replace(".", ",")
    return valor


def to_csv_bytes(df: pd.DataFrame, spark_types: list[str], formato: str = "excel_br") -> bytes:
    """CSV em UTF-8 com BOM (o Excel reconhece os acentos). spark_types: tipo Spark de cada coluna."""
    if formato not in _SEPARADOR:
        raise ValueError(f"Formato desconhecido: {formato}")
    saida = df
    if formato == "excel_br":
        saida = df.copy()
        for posicao, tipo in enumerate(spark_types):
            if (tipo or "").split("(")[0].strip().lower() in _TIPOS_DECIMAIS:
                saida.isetitem(posicao, df.iloc[:, posicao].map(_decimal_com_virgula).astype(object))
    return saida.to_csv(index=False, sep=_SEPARADOR[formato]).encode("utf-8-sig")
