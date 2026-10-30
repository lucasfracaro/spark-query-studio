"""
Exportação do resultado em CSV.

"Excel em português" usa ; como separador e vírgula como separador decimal: é o que o Excel em
português espera ao abrir o arquivo com dois cliques. Com ponto decimal, o Excel em português lê
10.5 como texto (ou como data).

Dois caminhos, com o mesmo formato:
- to_csv_bytes: as linhas que já estão na tela;
- fetch_full_result: o resultado inteiro. Ele sai do resultado que a execução deixou guardado na sessão
  (sem rodar a query de novo) ou, se ele não estiver mais lá, de uma execução nova. Todas as linhas vão
  para um CSV comprimido no driver e o arquivo volta em partes pela própria sessão (sem bucket, sem Athena).
"""
from __future__ import annotations

import base64
import codecs
import csv
import gzip
import io
import math
from collections.abc import Callable

import pandas as pd

from code_builder import (
    EXPORT_CHUNK_BYTES,
    EXPORT_MAX_ROWS,
    Orchestration,
    build_export_chunk_code,
    build_export_result_code,
    build_spark_code,
)

FORMATS = {
    "excel_br": "Excel em português (; e vírgula decimal)",
    "padrao": "Padrão internacional (, e ponto decimal)",
    "tab": "Separado por tabulação (ponto decimal)",
}
_SEPARADOR = {"excel_br": ";", "padrao": ",", "tab": "\t"}
_TIPOS_DECIMAIS = ("decimal", "double", "float", "real")


def format_cell(valor, decimal: bool, virgula: bool) -> str:
    """Texto de uma célula do CSV. É a mesma regra do resultado completo (_celula, no código gerado):
    os testes conferem que os dois arquivos saem iguais.

    decimal: a coluna é decimal/double/float/real; virgula: formato "Excel em português"."""
    if valor is None or (isinstance(valor, float) and math.isnan(valor)):
        return ""
    if isinstance(valor, bool):
        return "True" if valor else "False"
    if isinstance(valor, float):
        if math.isinf(valor):
            return ("Infinito" if valor > 0 else "-Infinito") if virgula else ("inf" if valor > 0 else "-inf")
        texto = repr(valor)
        if virgula and ("e" in texto or "E" in texto):          # 1e-07 -> 0,0000001 (sem notação científica)
            texto = f"{valor:.20f}".rstrip("0").rstrip(".")
    else:
        texto = str(valor)
    return texto.replace(".", ",") if (virgula and decimal) else texto


def rows_to_csv_bytes(names: list[str], rows: list[list], spark_types: list[str], formato: str = "excel_br") -> bytes:
    """CSV em UTF-8 com BOM (o Excel reconhece os acentos). rows: valores como vêm da sessão (o número inteiro
    com nulos continua inteiro: 1, e não 1.0)."""
    if formato not in _SEPARADOR:
        raise ValueError(f"Formato desconhecido: {formato}")
    virgula = formato == "excel_br"
    decimais = [(tipo or "").split("(")[0].strip().lower() in _TIPOS_DECIMAIS for tipo in spark_types]
    saida = io.StringIO()
    escritor = csv.writer(saida, delimiter=_SEPARADOR[formato], lineterminator="\n")
    escritor.writerow(names)
    for linha in rows:
        escritor.writerow([format_cell(v, d, virgula) for v, d in zip(linha, decimais, strict=False)])
    return saida.getvalue().encode("utf-8-sig")


def to_csv_bytes(df: pd.DataFrame, spark_types: list[str], formato: str = "excel_br") -> bytes:
    """Como rows_to_csv_bytes, a partir de um DataFrame do pandas."""
    valores = df.astype(object).where(df.notna(), None).values.tolist()
    return rows_to_csv_bytes([str(c) for c in df.columns], valores, spark_types, formato)


class ExportError(RuntimeError):
    pass


def fetch_full_result(run: Callable[[str, Callable[[str], None]], dict], sql: str, orch: Orchestration,
                      reader: str = "spark", formato: str = "excel_br",
                      on_status: Callable[[str], None] = lambda m: None,
                      max_rows: int = EXPORT_MAX_ROWS, chunk_bytes: int = EXPORT_CHUNK_BYTES,
                      result_key: str | None = None) -> tuple[bytes, dict]:
    """O resultado inteiro em CSV (UTF-8 com BOM, como o da tela) e o resumo {rows, truncated, bytes, rerun}.

    run(código, on_status) executa um comando na sessão e devolve o resultado lido (ex.: session_hub.HUB.run).
    result_key: a chave do resultado que a execução deixou guardado na sessão (o "result_key" da resposta)."""
    if formato not in _SEPARADOR:
        raise ValueError(f"Formato desconhecido: {formato}")
    config = {"format": formato, "sep": _SEPARADOR[formato], "max_rows": int(max_rows), "chunk": int(chunk_bytes)}
    res = None
    if result_key:
        on_status("Gerando o CSV com o resultado que ficou na sessão…")
        res = run(build_export_result_code(result_key, config), on_status)
        if res and res.get("missing"):
            res = None                      # sessão nova ou outra query rodou depois: roda esta de novo
        elif not res or not res.get("ok"):
            raise ExportError((res or {}).get("error") or "A sessão não devolveu o resultado.")
    rodou_de_novo = res is None
    if res is None:
        on_status("Rodando a query de novo para trazer todas as linhas…")
        res = run(build_spark_code(sql, orch, reader=reader, export=config), on_status)
        if not res or not res.get("ok"):
            raise ExportError((res or {}).get("error") or "A sessão não devolveu o resultado.")
    info = res["export"]
    partes = []
    for i in range(info["chunks"]):
        on_status(f"Trazendo o arquivo ({info['rows']:,} linhas): parte {i + 1} de {info['chunks']}…".replace(",", "."))
        parte = run(build_export_chunk_code(info["file"], i, chunk_bytes, last=i == info["chunks"] - 1), on_status)
        if not parte or not parte.get("ok"):
            raise ExportError((parte or {}).get("error") or f"A parte {i + 1} do arquivo não voltou.")
        partes.append(base64.b64decode(parte["chunk"]))
    try:
        dados = gzip.decompress(b"".join(partes))
    except (OSError, EOFError) as e:
        raise ExportError(f"O arquivo voltou incompleto ({e}). Tente de novo.") from e
    return codecs.BOM_UTF8 + dados, dict(info, rerun=rodou_de_novo)
