"""
Importa uma query do Athena (Trino) e monta a orquestração automaticamente.

1. Lê a query com o sqlglot e encontra as bases usadas (ignorando CTEs).
2. Descobre as colunas de partição de cada base (Glue Data Catalog; sem acesso, supõe pelo nome).
3. Em cada lugar onde a base é lida, separa o WHERE / ON em condições (pelos AND) e fica com as que
   usam só colunas de partição dessa base. Essas condições viram o filtro da entrada na orquestração.
   Elas são COPIADAS: a query continua com os filtros originais, então o resultado não muda.
4. Traduz a query para Spark SQL, trocando "db.tabela" pelo alias da orquestração.
   INSERT ... SELECT e CREATE TABLE ... AS SELECT viram só o SELECT: a ferramenta mostra o que
   seria gravado, sem gravar. O destino da escrita não entra na orquestração.
5. Devolve um relatório do que foi feito automaticamente e do que precisa de revisão.
"""
from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field

import latest_partition
from code_builder import DEFAULT_LIMIT, InputSource, Orchestration
from sql_refs import is_read_table

try:
    import sqlglot
    from sqlglot import exp
except ImportError:  # pragma: no cover - tratado na tela
    sqlglot = None
    exp = None

# nomes dos argumentos WITH e FROM nos nós do sqlglot ("with_"/"from_" nas versões novas, "with"/"from" nas antigas)
_WITH_KEY = ("with_" if exp is not None and "with_" in exp.Select.arg_types else "with")
FROM_KEY = ("from_" if exp is not None and "from_" in exp.Select.arg_types else "from")

# nomes comuns de colunas de partição (usados só quando não dá para consultar o Glue)
_LIKELY_PARTITION_NAMES = {
    "dt", "data", "date", "anomes", "ano_mes", "anomesdia", "ano", "mes", "dia", "year", "month", "day",
    "dt_ref", "data_ref", "dt_referencia", "data_referencia", "ref", "safra", "partition_date",
    "dt_carga", "data_carga", "dt_proc", "dt_processamento", "ingestion_date", "hora", "hour",
}
_LIKELY_PARTITION_PREFIXES = ("dt_", "anomes", "particao", "partition_", "part_")

PartitionLookup = Callable[[str, str], "list[str]"]


class TableNotFoundError(LookupError):
    pass


@dataclass
class Note:
    level: str      # "info" | "atencao"
    text: str


@dataclass
class TableInfo:
    database: str
    table: str
    alias: str
    partition_keys: list[str]
    partition_source: str            # "glue" | "suposicao"
    filter: str = ""


@dataclass
class AthenaImport:
    orchestration: Orchestration
    spark_sql: str
    tables: list[TableInfo]
    notes: list[Note] = field(default_factory=list)

    @property
    def needs_review(self) -> bool:
        return any(n.level == "atencao" for n in self.notes)


def sqlglot_available() -> bool:
    return sqlglot is not None


# --------------------------------------------------------------------------------------------
# Colunas de partição
# --------------------------------------------------------------------------------------------
def glue_partition_lookup(glue_client) -> PartitionLookup:
    """Consulta as colunas de partição no Glue Data Catalog (get_table), com cache."""
    cache: dict[tuple[str, str], list[str]] = {}

    def lookup(database: str, table: str) -> list[str]:
        key = (database.lower(), table.lower())
        if key not in cache:
            try:
                resp = glue_client.get_table(DatabaseName=database, Name=table)
            except Exception as e:
                if type(e).__name__ == "EntityNotFoundException":
                    raise TableNotFoundError(f"{database}.{table}") from None
                raise
            cache[key] = [k["Name"].lower() for k in resp["Table"].get("PartitionKeys", [])]
        return cache[key]

    return lookup


def _looks_like_partition(col: str) -> bool:
    col = col.lower()
    return col in _LIKELY_PARTITION_NAMES or col.startswith(_LIKELY_PARTITION_PREFIXES)


# --------------------------------------------------------------------------------------------
# Auxiliares de AST
# --------------------------------------------------------------------------------------------
class _WarningCollector(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record):
        self.messages.append(record.getMessage())


def _sanitize_alias(name: str) -> str:
    alias = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not alias or alias[0].isdigit():
        alias = "t_" + alias
    return alias.lower()


def _sources_of(select) -> list:
    """Tabelas/subqueries do FROM e dos JOINs de um SELECT."""
    out = []
    frm = select.args.get(FROM_KEY)
    if frm is not None:
        out.append(frm.this)
    for j in select.args.get("joins") or []:
        out.append(j.this)
    return out


def _conjuncts(cond) -> list:
    if cond is None:
        return []
    return list(cond.flatten()) if isinstance(cond, exp.And) else [cond]


@dataclass
class _Occurrence:
    key: tuple[str, str]
    conds: list           # condições de partição (cópias, sem prefixo de tabela)
    reasons: list[str]    # condições com partição que não puderam ser levadas


def _analyze_occurrence(table, key, partition_keys: set[str], lookup_keys) -> _Occurrence:
    occ = _Occurrence(key, [], [])
    parent = table.parent
    if not isinstance(parent, (exp.From, exp.Join)) or not isinstance(parent.parent, exp.Select):
        return occ
    select = parent.parent
    ref_name = table.alias_or_name.lower()
    sources = _sources_of(select)
    single_source = len(sources) == 1

    # partições das outras bases do mesmo SELECT (para atribuir colunas sem prefixo)
    other_partitions = set()
    for src in sources:
        if src is not table and isinstance(src, exp.Table):
            other_partitions |= set(lookup_keys(src))

    candidates = list(_conjuncts(select.args.get("where").this if select.args.get("where") else None))
    if isinstance(parent, exp.Join) and (parent.side or "").upper() in ("", "LEFT"):
        # no ON de um JOIN interno ou LEFT, condições só da base juntada filtram essa base
        candidates += _conjuncts(parent.args.get("on"))
    for j in select.args.get("joins") or []:
        # o ON de um JOIN interno filtra o resultado inteiro: uma condição só desta base também a filtra
        if j is not parent and not (j.side or "") and (j.kind or "").upper() not in ("CROSS", "OUTER", "FULL"):
            candidates += _conjuncts(j.args.get("on"))

    for cond in candidates:
        # só as colunas deste SELECT (as de dentro de uma subquery são de outra consulta)
        cols = [c for c in cond.find_all(exp.Column) if c.find_ancestor(exp.Select) is select]
        if not cols:
            continue
        mine, foreign = [], False
        for c in cols:
            qual = (c.table or "").lower()
            name = c.name.lower()
            if qual == ref_name or (not qual and (single_source or (name in partition_keys and name not in other_partitions))):
                mine.append(name)
            else:
                foreign = True
        part_cols = [c for c in mine if c in partition_keys]
        if not part_cols:
            continue
        sql_txt = cond.sql(dialect="athena")
        subquery = cond.find(exp.Select)
        if subquery is not None:
            # max/min numa subquery já tem um aviso próprio (a base dela é lida inteira)
            if subquery.find(exp.Max, exp.Min) is None:
                occ.reasons.append(f"`{sql_txt}` depende de uma subquery (filtro dinâmico)")
            continue
        if foreign or len(part_cols) != len(mine):
            occ.reasons.append(f"`{sql_txt}` mistura coluna de partição com outras colunas")
            continue
        if cond.find(exp.Is) is not None or cond.find(exp.Coalesce) is not None:
            occ.reasons.append(f"`{sql_txt}` trata valores nulos")
            continue
        copy = cond.copy()
        for c in copy.find_all(exp.Column):
            c.set("table", None)
            c.set("db", None)
            c.set("catalog", None)
        occ.conds.append(copy)
    return occ


def _select_of_write(stmt):
    """INSERT ... SELECT e CREATE TABLE ... AS SELECT -> (destino, SELECT). Outros comandos -> None."""
    if not isinstance(stmt, (exp.Insert, exp.Create)):
        return None
    query = stmt.args.get("expression")
    if not isinstance(query, exp.Query):            # INSERT ... VALUES, CREATE TABLE sem AS SELECT
        return None
    destino = stmt.this.this if isinstance(stmt.this, exp.Schema) else stmt.this
    query = query.copy()
    with_ = stmt.args.get(_WITH_KEY)                 # WITH ... INSERT INTO ...: o WITH fica no INSERT
    if with_ is not None and query.args.get(_WITH_KEY) is None:
        query.set(_WITH_KEY, with_.copy())
    return destino.sql(dialect="athena"), query


def _is_sql_command(stmt) -> bool:
    """Diferencia um comando SQL de um trecho solto que o sqlglot leu como expressão (ex.: "SELEC * FRM t")."""
    tipos = [exp.Query] + [getattr(exp, n) for n in (
        "DDL", "DML", "Insert", "Create", "Merge", "Delete", "Update", "Drop", "Alter", "TruncateTable",
        "Command", "Set", "Use", "Describe", "Show", "Cache", "Uncache") if hasattr(exp, n)]
    return isinstance(stmt, tuple(tipos))


# --------------------------------------------------------------------------------------------
# Importação
# --------------------------------------------------------------------------------------------
def import_athena_query(sql: str, partition_lookup: PartitionLookup | None = None,
                        default_database: str = "", partition_values=None,
                        resolve_max: bool = False) -> AthenaImport:
    """partition_values: lista de partições do catálogo (latest_partition.glue_partition_values), para trocar as
    consultas a `tabela$partitions` (que só existem no Athena) pelo valor atual. resolve_max: troca também
    max/min de colunas de partição lidos de uma base (senão, só avisa que a base é lida inteira)."""
    if sqlglot is None:
        raise RuntimeError("O pacote sqlglot não está instalado (pip install sqlglot).")

    notes: list[Note] = []
    collector = _WarningCollector()
    sg_logger = logging.getLogger("sqlglot")
    sg_logger.addHandler(collector)
    try:
        try:
            statements = [s for s in sqlglot.parse(sql, read="athena") if s is not None]
        except sqlglot.errors.SqlglotError as e:      # ParseError, TokenError (ex.: string sem fechar)...
            raise ValueError(f"Não consegui ler a query do Athena: {e}") from None
        if not statements:
            raise ValueError("A query está vazia.")
        for i, stmt in enumerate(statements):
            if not _is_sql_command(stmt):
                inicio = " ".join(stmt.sql(dialect="athena").split())[:40]
                raise ValueError(f"Não consegui ler a query do Athena: o comando {i + 1} não parece SQL válido "
                                 f"(lido como `{inicio}`).")

        # INSERT ... SELECT / CREATE TABLE ... AS SELECT -> só o SELECT (o que seria gravado)
        for i, stmt in enumerate(statements):
            convertido = _select_of_write(stmt)
            if convertido is not None:
                destino, query = convertido
                statements[i] = query
                notes.append(Note("info", f"O comando {i + 1} gravaria em `{destino}`; ficou só o SELECT, para "
                                          "você ver o que seria gravado sem gravar nada."))
            elif not isinstance(stmt, exp.Query):
                tipo = type(stmt).__name__.upper()
                if isinstance(stmt, (exp.Insert, exp.Create, exp.Merge, exp.Delete, exp.Update, exp.Drop,
                                     exp.Alter, exp.TruncateTable)):
                    notes.append(Note("atencao", f"O comando {i + 1} ({tipo}) altera dados e foi mantido como está. "
                                                 "Com o modo somente leitura ele não é executado."))

        # ---- tabela$partitions (só existe no Athena) e max/min que leem a base inteira
        for nivel, texto in latest_partition.resolve(statements, partition_values, default_database, resolve_max):
            notes.append(Note(nivel, texto))

        cte_names = {c.alias_or_name.lower() for s in statements for c in s.find_all(exp.CTE)}

        # ---- bases usadas
        def key_of(t) -> tuple[str, str]:
            return ((t.db or default_database or "").lower(), t.name.lower())

        def is_base(t) -> bool:
            # só bases LIDAS: destino de escrita, hints, funções de tabela, CTEs e as tabelas de metadados do
            # Athena (tabela$partitions...) ficam de fora
            return (is_read_table(t) and not (not t.db and t.name.lower() in cte_names)
                    and latest_partition.meta_table(t) is None)

        occurrences = [t for s in statements for t in s.find_all(exp.Table) if is_base(t)]
        keys: list[tuple[str, str]] = []
        for t in occurrences:
            k = key_of(t)
            if k not in keys:
                keys.append(k)

        # ---- colunas de partição
        partition_info: dict[tuple[str, str], tuple[list[str] | None, str]] = {}
        glue_failed = False
        for db, tb in keys:
            if partition_lookup is not None and db and not glue_failed:
                try:
                    partition_info[(db, tb)] = (partition_lookup(db, tb), "glue")
                    continue
                except TableNotFoundError:
                    notes.append(Note("atencao", f"`{db}.{tb}` não existe no Glue Data Catalog. Confira o nome."))
                except Exception as e:  # sem permissão, sem rede...
                    glue_failed = True
                    notes.append(Note("atencao", f"Não consegui consultar o Glue ({type(e).__name__}: {e}). "
                                                 "As colunas de partição foram supostas pelo nome."))
            partition_info[(db, tb)] = (None, "suposicao")
            if not db:
                notes.append(Note("atencao", f"A base `{tb}` está sem database. Informe o database padrão "
                                             "ou ajuste a origem na orquestração."))

        def partitions_for(k, observed_cols) -> set[str]:
            keys_, _src = partition_info[k]
            if keys_ is not None:
                return set(keys_)
            return {c for c in observed_cols if _looks_like_partition(c)}

        def lookup_keys(t) -> set[str]:
            k = key_of(t)
            if k not in partition_info:
                return set()
            keys_, _ = partition_info[k]
            return set(keys_) if keys_ is not None else set()

        # ---- filtros de partição por ocorrência
        per_key: dict[tuple[str, str], list[_Occurrence]] = {k: [] for k in keys}
        guessed_cols: dict[tuple[str, str], set[str]] = {k: set() for k in keys}
        for t in occurrences:
            k = key_of(t)
            known, _src = partition_info[k]
            if known is None:
                # suposição: colunas filtradas no mesmo SELECT com nome típico de partição
                sel = t.parent.parent if isinstance(t.parent, (exp.From, exp.Join)) else None
                observed = {c.name.lower() for c in sel.find_all(exp.Column)} if isinstance(sel, exp.Select) else set()
                pk = partitions_for(k, observed)
                guessed_cols[k] |= pk
            else:
                pk = set(known)
            per_key[k].append(_analyze_occurrence(t, k, pk, lookup_keys))

        # ---- aliases
        name_count: dict[str, int] = {}
        for _db, tb in keys:
            name_count[tb] = name_count.get(tb, 0) + 1
        aliases: dict[tuple[str, str], str] = {}
        used = set(cte_names)
        for db, tb in keys:
            alias = _sanitize_alias(tb if name_count[tb] == 1 or not db else f"{db}_{tb}")
            while alias in used:
                alias += "_base"
            used.add(alias)
            aliases[(db, tb)] = alias

        # ---- monta as entradas
        tables: list[TableInfo] = []
        inputs: list[InputSource] = []
        for k in keys:
            db, tb = k
            occs = per_key[k]
            known, src = partition_info[k]
            pkeys = sorted(known) if known is not None else sorted(guessed_cols[k])
            for o in occs:
                for r in o.reasons:
                    notes.append(Note("atencao", f"`{db}.{tb}`: {r}. Esse filtro ficou só na query."))
            filters = []
            if occs and all(o.conds for o in occs):
                for o in occs:
                    f = exp.and_(*o.conds).sql(dialect="spark") if len(o.conds) > 1 else o.conds[0].sql(dialect="spark")
                    if f not in filters:
                        filters.append(f)
            elif any(o.conds for o in occs):
                notes.append(Note("atencao", f"`{db}.{tb}` é lida com filtro de partição em uma parte da query e sem "
                                             "filtro em outra; a entrada ficou sem filtro (todas as partições)."))
            elif pkeys:
                notes.append(Note("atencao", f"`{db}.{tb}` é particionada por {', '.join(pkeys)}, mas a query não "
                                             "filtra a partição: todas as partições serão lidas."))
            filtro = filters[0] if len(filters) == 1 else " OR ".join(f"({f})" for f in filters)
            if src == "suposicao" and filtro:
                notes.append(Note("atencao", f"`{db}.{tb}`: as colunas de partição ({', '.join(pkeys)}) foram "
                                             "supostas pelo nome. Confira o filtro."))
            info = TableInfo(db, tb, aliases[k], pkeys, src, filtro)
            tables.append(info)
            inputs.append(InputSource(alias=info.alias, kind="catalog",
                                      source=f"{db}.{tb}" if db else tb, filter=filtro))

        # ---- reescreve as referências para os aliases e traduz para Spark
        for t in occurrences:
            k = key_of(t)
            alias = aliases[k]
            original_ref = t.alias
            new = exp.to_table(alias)
            if original_ref:
                new.set("alias", t.args.get("alias"))
            elif alias != t.name.lower():
                new.set("alias", exp.TableAlias(this=exp.to_identifier(t.name)))
            t.replace(new)

        for fn in {f.name for s in statements for f in s.find_all(exp.Anonymous)}:
            notes.append(Note("atencao", f"A função `{fn}` não é conhecida pelo tradutor. Confira se ela existe no Spark."))
        if any(s.find(exp.StrToTime, exp.StrToDate, exp.TimeToStr) is not None for s in statements):
            notes.append(Note("info", "Formatos de data foram convertidos para o padrão do Spark "
                                      "(ex.: %Y-%m-%d → yyyy-MM-dd). Vale conferir."))

        spark_sql = ";\n\n".join(s.sql(dialect="spark", pretty=True) for s in statements)
    finally:
        sg_logger.removeHandler(collector)

    for msg in dict.fromkeys(collector.messages):
        notes.append(Note("atencao", f"Tradução para Spark: {msg}."))

    if not any(n.level == "atencao" for n in notes):
        notes.insert(0, Note("info", "Tudo foi traduzido sem pendências."))
    notes.insert(0, Note("info", f"{len(tables)} base(s) encontrada(s); "
                                 f"{sum(1 for t in tables if t.filter)} com filtro de partição levado para a orquestração."))

    orch = Orchestration(inputs=inputs, default_database=default_database, limit=DEFAULT_LIMIT)
    return AthenaImport(orchestration=orch, spark_sql=spark_sql, tables=tables, notes=notes)
