"""Metadados do resultado de uma query: dicionário de dados, tipos para o Athena e DDL da tabela."""
from __future__ import annotations

import json
import re

# tipos do Spark que o Athena/Hive não tem (ou escreve diferente)
_TYPE_FIXES = [
    (re.compile(r"\btimestamp_ntz\b"), "timestamp"),
    (re.compile(r"\btimestamp_ltz\b"), "timestamp"),
    (re.compile(r"\bvoid\b"), "string"),
    (re.compile(r"\binterval(?: [a-z]+(?: to [a-z]+)?)?\b"), "string"),
]


def spark_to_athena_type(spark_type: str) -> str:
    t = (spark_type or "string").strip().lower()
    for rx, repl in _TYPE_FIXES:
        t = rx.sub(repl, t)
    return t


def build_dictionary(columns: list[dict], profile: dict | None = None) -> list[dict]:
    """columns: [{"name", "type", "nullable"?}] vindos do resultado; profile: estatísticas opcionais."""
    stats = (profile or {}).get("columns") or []
    total = (profile or {}).get("rows")
    rows = []
    for i, c in enumerate(columns):
        row = {
            "coluna": c["name"],
            "tipo_spark": c.get("type", ""),
            "tipo_athena": spark_to_athena_type(c.get("type", "")),
            "aceita_nulo": bool(c.get("nullable", True)),
            "descricao": c.get("descricao", ""),
        }
        if i < len(stats) and stats[i] is not None:
            s = stats[i]
            nulls = s.get("nulls")
            row.update({
                "nulos": nulls,
                "pct_nulos": round(100.0 * nulls / total, 2) if total and nulls is not None else None,
                "distintos_aprox": s.get("distinct"),
                "minimo": s.get("min"),
                "maximo": s.get("max"),
            })
        rows.append(row)
    return rows


def dictionary_to_json(rows: list[dict], table: str = "", rows_total: int | None = None) -> str:
    return json.dumps({"tabela": table, "linhas": rows_total, "colunas": rows}, indent=2, ensure_ascii=False,
                      default=str)


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _quote_str(text: str) -> str:
    return "'" + str(text).replace("\\", "\\\\").replace("'", "\\'") + "'"


_STORED_AS = {"parquet": "PARQUET", "orc": "ORC", "json": None, "csv": None}


def build_ddl(rows: list[dict], database: str, table: str, location: str,
              partition_columns: list[str] | None = None, fmt: str = "parquet",
              table_comment: str = "") -> str:
    """CREATE EXTERNAL TABLE para o Athena a partir do dicionário de dados.

    As colunas de partição saem da lista de colunas e vão para PARTITIONED BY, na ordem informada.
    """
    if not table.strip():
        raise ValueError("Informe o nome da tabela.")
    if not location.strip().startswith("s3://"):
        raise ValueError("A localização deve começar com s3://")
    partition_columns = partition_columns or []
    by_name = {r["coluna"]: r for r in rows}
    unknown = [p for p in partition_columns if p not in by_name]
    if unknown:
        raise ValueError(f"Colunas de partição que não existem no resultado: {', '.join(unknown)}")

    def col_line(r):
        line = f"  {_quote_ident(r['coluna'])} {r['tipo_athena']}"
        if str(r.get("descricao") or "").strip():
            line += f" COMMENT {_quote_str(r['descricao'].strip())}"
        return line

    data_cols = [r for r in rows if r["coluna"] not in partition_columns]
    if not data_cols:
        raise ValueError("A tabela precisa de ao menos uma coluna fora das partições.")

    name = f"{_quote_ident(database)}.{_quote_ident(table)}" if database.strip() else _quote_ident(table)
    parts = [f"CREATE EXTERNAL TABLE IF NOT EXISTS {name} (", ",\n".join(col_line(r) for r in data_cols), ")"]
    if table_comment.strip():
        parts.append(f"COMMENT {_quote_str(table_comment.strip())}")
    if partition_columns:
        parts.append("PARTITIONED BY (\n" + ",\n".join(col_line(by_name[p]) for p in partition_columns) + "\n)")

    fmt = fmt.lower()
    if fmt == "csv":
        parts.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'\nSTORED AS TEXTFILE")
    elif fmt == "json":
        parts.append("ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'")
    else:
        parts.append(f"STORED AS {_STORED_AS.get(fmt) or 'PARQUET'}")
    loc = location.strip() if location.strip().endswith("/") else location.strip() + "/"
    parts.append(f"LOCATION {_quote_str(loc)}")
    if fmt == "csv":
        parts.append("TBLPROPERTIES ('skip.header.line.count'='1')")
    elif fmt == "parquet":
        parts.append("TBLPROPERTIES ('parquet.compression'='SNAPPY')")
    return "\n".join(parts) + ";"
