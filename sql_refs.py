"""
Lê uma query Spark SQL para:

- descobrir quais bases (tabelas/views) ela lê e conferir se todas estão na orquestração;
- achar comandos que gravam ou alteram dados (modo somente leitura);
- reconhecer, no erro do Spark, a base ou o banco que não foi encontrado.

A leitura das bases usa o sqlglot (um parser SQL de verdade). Se ele não estiver instalado ou
não entender algum comando, entra um leitor de tokens mais simples, que acha o que vem depois
de FROM e JOIN ignorando strings, comentários, subqueries, funções e CTEs.
"""
from __future__ import annotations

import contextlib
import functools
import logging
import re
from dataclasses import dataclass, field

from code_builder import split_statements_with_lines

try:
    import sqlglot
    from sqlglot import exp
except ImportError:  # pragma: no cover - o leitor de tokens cobre
    sqlglot = None
    exp = None


# --------------------------------------------------------------------------------------------
# Resultado
# --------------------------------------------------------------------------------------------
@dataclass
class TableRef:
    name: str          # normalizado (minúsculas, sem crases): "db.tabela" ou "view"
    line: int


@dataclass
class QueryTables:
    refs: list[TableRef] = field(default_factory=list)     # bases lidas (FROM/JOIN/USING)
    defined: set[str] = field(default_factory=set)         # CTEs e views/tabelas criadas na query


# --------------------------------------------------------------------------------------------
# Leitor de tokens (reserva)
# --------------------------------------------------------------------------------------------
_TOKEN_RE = re.compile(r"`(?:[^`]|``)*`|[A-Za-z_][A-Za-z0-9_$]*|\d+(?:\.\d+)?|\S")

# palavras que encerram o nome/alias de uma tabela
_KEYWORDS = {
    "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT", "FULL",
    "CROSS", "OUTER", "SEMI", "ANTI", "NATURAL", "ON", "USING", "UNION", "INTERSECT", "EXCEPT", "MINUS",
    "LATERAL", "WINDOW", "CLUSTER", "DISTRIBUTE", "SORT", "TABLESAMPLE", "PIVOT", "UNPIVOT", "QUALIFY",
    "AS", "VERSION", "TIMESTAMP", "FOR", "SELECT", "FROM", "INSERT", "VALUES", "WITH", "SET",
}
# um "(" depois destas palavras NÃO é chamada de função (é subquery, lista, CTE...)
_NOT_FUNCTION_BEFORE_PAREN = {
    "AS", "IN", "EXISTS", "FROM", "JOIN", "ON", "WHERE", "AND", "OR", "NOT", "SELECT", "UNION",
    "INTERSECT", "EXCEPT", "MINUS", "ALL", "ANY", "SOME", "LATERAL", "VALUES", "WITH", "THEN", "ELSE",
    "WHEN", "CASE", "BY", "HAVING", "USING", "TABLE", "VIEW", "INTO", "IS",
}
# comandos que não leem tabelas por FROM/JOIN (SHOW TABLES FROM db, SET, USE...)
_SKIP_STATEMENTS = {"SHOW", "SET", "USE", "RESET", "ADD", "LIST", "REFRESH", "MSCK", "DESCRIBE", "DESC"}


@dataclass
class _Tok:
    text: str
    upper: str
    line: int

    @property
    def is_ident(self) -> bool:
        return self.text[0] == "`" or self.text[0].isalpha() or self.text[0] == "_"


def _blank_strings_and_comments(sql: str) -> str:
    """Troca o conteúdo de strings e comentários por espaços (mantendo quebras de linha e posições)."""
    out = list(sql)
    i, n = 0, len(sql)

    def blank(a, b):
        for k in range(a, b):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        ch, nxt = sql[i], sql[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-":
            end = sql.find("\n", i)
            end = n if end == -1 else end
            blank(i, end)
            i = end
        elif ch == "/" and nxt == "*":
            end = sql.find("*/", i + 2)
            end = n if end == -1 else end + 2
            blank(i, end)
            i = end
        elif ch in ("'", '"'):
            j = i + 1
            while j < n:
                if sql[j] == "\\":
                    j += 2
                    continue
                if sql[j] == ch:
                    if j + 1 < n and sql[j + 1] == ch:     # aspa dobrada: 'it''s'
                        j += 2
                        continue
                    break
                j += 1
            blank(i + 1, min(j, n))                        # mantém as aspas, apaga o conteúdo
            i = j + 1
        elif ch == "`":
            end = sql.find("`", i + 1)
            i = n if end == -1 else end + 1
        else:
            i += 1
    return "".join(out)


def _tokens(text: str, line0: int = 1) -> list[_Tok]:
    """Tokens de UM comando (sem strings e comentários), com a linha absoluta de cada um."""
    clean = _blank_strings_and_comments(text)
    toks, line, pos = [], line0, 0
    for m in _TOKEN_RE.finditer(clean):
        line += clean.count("\n", pos, m.start())
        pos = m.start()
        if m.group() != ";":
            toks.append(_Tok(m.group(), m.group().upper(), line))
    return toks


def _norm(name: str) -> str:
    return ".".join(p.strip("`").replace("``", "`").lower() for p in name.split("."))


def _skip_parens(toks, j):
    """toks[j] == '(' -> devolve o índice logo depois do ')' correspondente."""
    depth = 0
    while j < len(toks):
        if toks[j].text == "(":
            depth += 1
        elif toks[j].text == ")":
            depth -= 1
            if depth == 0:
                return j + 1
        j += 1
    return j


def _read_name(toks, j):
    """Lê ident(.ident)* a partir de j. Devolve (nome, próximo índice, é_caminho_de_arquivo)."""
    parts, is_path = [], False
    while j < len(toks) and toks[j].is_ident:
        parts.append(toks[j].text)
        if toks[j].text.startswith("`") and ("/" in toks[j].text or ":" in toks[j].text):
            is_path = True                                  # parquet.`s3://bucket/pasta`
        j += 1
        if j < len(toks) and toks[j].text == "." and j + 1 < len(toks) and toks[j + 1].is_ident:
            j += 1
            continue
        break
    return ".".join(parts), j, is_path


def _defined_names(toks) -> set[str]:
    names = set()
    for i, t in enumerate(toks):
        # WITH [RECURSIVE] nome [(cols)] AS (   e   , nome [(cols)] AS (
        if t.is_ident and t.upper not in _KEYWORDS and i > 0 and toks[i - 1].upper in ("WITH", "RECURSIVE", ","):
            j = i + 1
            if j < len(toks) and toks[j].text == "(":
                j = _skip_parens(toks, j)
            if j + 1 < len(toks) and toks[j].upper == "AS" and toks[j + 1].text == "(":
                names.add(_norm(t.text))
    # CREATE [OR REPLACE] [GLOBAL] [TEMP|TEMPORARY] VIEW|TABLE [IF NOT EXISTS] nome
    # CACHE [LAZY] TABLE nome
    if toks and toks[0].upper in ("CREATE", "CACHE", "REPLACE"):
        for i, t in enumerate(toks):
            if t.upper in ("VIEW", "TABLE"):
                j = i + 1
                while j < len(toks) and toks[j].upper in ("IF", "NOT", "EXISTS"):
                    j += 1
                name, _, _ = _read_name(toks, j)
                if name:
                    names.add(_norm(name))
                break
    return names


def _read_from_list(toks, j, result: QueryTables, allow_list: bool) -> int:
    while j < len(toks):
        if toks[j].upper == "LATERAL":
            j += 1
        if j < len(toks) and toks[j].text == "(":
            return j                                        # subquery: o laço principal entra nela
        start_line = toks[j].line if j < len(toks) else 0
        name, j, is_path = _read_name(toks, j)
        if not name:
            return j
        if j < len(toks) and toks[j].text == "(":           # função de tabela: range(10), explode(...)
            j = _skip_parens(toks, j)
        elif not is_path:
            result.refs.append(TableRef(_norm(name), start_line))
        # alias opcional
        if j < len(toks) and toks[j].upper == "AS":
            j += 1
        if j < len(toks) and toks[j].is_ident and toks[j].upper not in _KEYWORDS:
            j += 1
            if j < len(toks) and toks[j].text == "(":       # alias com colunas: t(a, b)
                j = _skip_parens(toks, j)
        if allow_list and j < len(toks) and toks[j].text == ",":
            j += 1
            continue
        return j
    return j


def _collect_tokens(text: str, line0: int, result: QueryTables) -> None:
    toks = _tokens(text, line0)
    if not toks:
        return
    result.defined |= _defined_names(toks)
    first = toks[0].upper
    if first in _SKIP_STATEMENTS:
        return
    if first == "TABLE":                                    # TABLE x  ==  SELECT * FROM x
        _read_from_list(toks, 1, result, allow_list=False)
        return

    paren_is_function: list[bool] = []
    i = 0
    while i < len(toks):
        t = toks[i]
        if t.text == "(":
            prev = toks[i - 1] if i else None
            paren_is_function.append(bool(prev and prev.is_ident and prev.upper not in _NOT_FUNCTION_BEFORE_PAREN))
        elif t.text == ")":
            if paren_is_function:
                paren_is_function.pop()
        elif t.upper in ("FROM", "JOIN") and not (paren_is_function and paren_is_function[-1]):
            i = _read_from_list(toks, i + 1, result, allow_list=(t.upper == "FROM"))
            continue
        elif t.upper == "USING" and toks[0].upper == "MERGE":     # MERGE INTO alvo USING origem
            i = _read_from_list(toks, i + 1, result, allow_list=False)
            continue
        i += 1


# --------------------------------------------------------------------------------------------
# Leitura com o sqlglot
# --------------------------------------------------------------------------------------------
@contextlib.contextmanager
def quiet_sqlglot():
    """Silencia os avisos do sqlglot (ex.: "Falling back to parsing as a 'Command'")."""
    logger = logging.getLogger("sqlglot")
    old = logger.level
    logger.setLevel(logging.ERROR)
    try:
        yield
    finally:
        logger.setLevel(old)


def _parse_spark(text: str):
    if sqlglot is None:
        return None
    try:
        with quiet_sqlglot():
            return sqlglot.parse_one(text, read="spark")
    except Exception:
        return None


def table_name(t) -> str:
    """Nome normalizado de um nó Table do sqlglot: "db.tabela" (sem catálogo) ou "tabela"."""
    return ".".join(p for p in (t.db, t.name) if p).lower()


def is_read_table(t) -> bool:
    """True se o nó Table do sqlglot é uma base LIDA pela query.

    Fica de fora: destino de INSERT/CREATE/MERGE/DELETE, nome de view criada, USE, DESCRIBE,
    tabelas citadas em hints (/*+ BROADCAST(t) */), funções de tabela (range(10)) e leitura
    direta de arquivo (parquet.`s3://...`).
    """
    if not t.name or not isinstance(t.this, exp.Identifier):
        return False
    if "/" in t.name or ":" in t.name:
        return False
    if t.find_ancestor(exp.Hint, exp.JoinHint) is not None:
        return False
    if isinstance(t.parent, exp.Merge) and t.parent.args.get("using") is t:
        return True
    return t.find_ancestor(exp.From, exp.Join) is not None


def _collect_sqlglot(tree, line0: int, result: QueryTables) -> None:
    result.defined |= {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    for node in tree.find_all(exp.Create, exp.Cache):
        target = node.this
        if isinstance(target, exp.Schema):
            target = target.this
        if isinstance(target, exp.Table) and target.name:
            result.defined.add(table_name(target))

    found = []
    for t in tree.find_all(exp.Table):
        if is_read_table(t):
            meta = t.this.meta or {}
            found.append((meta.get("line", 1), meta.get("col", 0), table_name(t)))
    for line, _col, name in sorted(found):
        result.refs.append(TableRef(name, line0 + line - 1))


@functools.lru_cache(maxsize=32)
def _find_tables_cached(sql: str) -> tuple[tuple[tuple[str, int], ...], frozenset]:
    result = QueryTables()
    for line0, text in split_statements_with_lines(sql):
        tree = _parse_spark(text)
        if tree is not None:
            _collect_sqlglot(tree, line0, result)
        else:
            _collect_tokens(text, line0, result)
    return tuple((r.name, r.line) for r in result.refs), frozenset(result.defined)


def find_tables(sql: str) -> QueryTables:
    refs, defined = _find_tables_cached(sql or "")
    return QueryTables([TableRef(nome, linha) for nome, linha in refs], set(defined))


# --------------------------------------------------------------------------------------------
# Conferência com a orquestração
# --------------------------------------------------------------------------------------------
@dataclass
class MissingTable:
    name: str
    lines: list[int]

    def message(self) -> str:
        onde = "linha " + ", ".join(map(str, self.lines)) if len(self.lines) <= 5 else f"{len(self.lines)} lugares"
        return (f"A base `{self.name}` é usada na query ({onde}), mas não está na orquestração. "
                f"Adicione-a em Passo 1 › Entradas de dados.")


@dataclass
class FullNameRef:
    """A query usa "db.tabela" em vez do alias de uma entrada que está na orquestração."""
    name: str
    alias: str
    line: int

    def message(self) -> str:
        return (f"Com o Lake Formation no Glue 4.0, use o alias `{self.alias}` em vez de `{self.name}` "
                f"(linha {self.line}): só as entradas da orquestração são lidas pelo GlueContext, que é o que "
                f"usa as permissões do Lake Formation.")


@dataclass
class OrchestrationCheck:
    missing: list[MissingTable] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    full_names: list[FullNameRef] = field(default_factory=list)    # bloqueiam só com require_alias

    @property
    def ok(self) -> bool:
        return not self.missing and not self.full_names


def check_query_against_orchestration(sql: str, inputs, require_alias: bool = False) -> OrchestrationCheck:
    """inputs: lista de code_builder.InputSource (só as válidas são consideradas).

    require_alias: a query precisa usar os aliases (leitura pelo GlueContext, no Glue 4.0 com Lake
    Formation). Sem isso, usar o nome completo de uma entrada é só um aviso.
    """
    found = find_tables(sql)
    aliases = {_norm(i.alias.strip()): i for i in inputs if i.is_valid()}
    by_source = {_norm(i.source.strip()): i for i in inputs if i.is_valid() and i.kind == "catalog"}

    check = OrchestrationCheck()
    missing: dict[str, MissingTable] = {}
    warned = set()
    for ref in found.refs:
        if ref.name in aliases or ref.name in found.defined:
            continue
        if ref.name in by_source:
            inp = by_source[ref.name]
            if ref.name not in warned:
                if require_alias:
                    check.full_names.append(FullNameRef(ref.name, inp.alias.strip(), ref.line))
                else:
                    aviso = (f"`{ref.name}` está na orquestração com o alias `{inp.alias.strip()}`, mas a query "
                             f"usa o nome completo (linha {ref.line}). Use o alias")
                    aviso += " para que o filtro de partição seja aplicado." if inp.filter.strip() else "."
                    check.warnings.append(aviso)
                warned.add(ref.name)
            continue
        missing.setdefault(ref.name, MissingTable(ref.name, []))
        if ref.line not in missing[ref.name].lines:
            missing[ref.name].lines.append(ref.line)
    check.missing = list(missing.values())
    return check


# --------------------------------------------------------------------------------------------
# Modo somente leitura: comandos que gravam ou alteram dados
# --------------------------------------------------------------------------------------------
_WRITE_FIRST = {
    "INSERT", "MERGE", "UPDATE", "DELETE", "ALTER", "TRUNCATE", "MSCK", "ANALYZE", "LOAD", "GRANT",
    "REVOKE", "REPAIR", "OPTIMIZE", "VACUUM", "COPY", "REPLACE", "COMMENT", "RESTORE", "UNDROP", "CONVERT",
}
_CREATE_OBJECTS = {"VIEW", "TABLE", "FUNCTION", "DATABASE", "SCHEMA", "NAMESPACE", "INDEX", "CATALOG"}


@dataclass
class WriteStatement:
    number: int       # posição do comando na query (1, 2, ...)
    line: int         # linha onde o comando começa
    keyword: str      # INSERT, CREATE, DROP...
    text: str         # começo do comando, para mostrar na mensagem

    def message(self) -> str:
        return (f"O comando {self.number} (linha {self.line}) grava ou altera dados: `{self.text}`. "
                "Com o modo somente leitura ele não é executado. Para ver o que seria gravado, rode só o "
                "SELECT; para executar mesmo assim, libere a escrita em Passo 1 › Opções.")


def _write_keyword(toks: list[_Tok]) -> str | None:
    if not toks:
        return None
    first = toks[0].upper
    if first == "CREATE":
        for t in toks[1:]:
            if t.upper in ("TEMP", "TEMPORARY"):          # CREATE TEMP VIEW / TEMPORARY FUNCTION
                return None
            if t.upper in _CREATE_OBJECTS:
                break
        return "CREATE"
    if first == "DROP":
        if len(toks) > 1 and toks[1].upper in ("TEMP", "TEMPORARY"):   # DROP TEMPORARY FUNCTION
            return None
        return "DROP"
    if first in _WRITE_FIRST:
        return first
    if first in ("WITH", "FROM", "("):                     # WITH ... INSERT, FROM x INSERT ... (Hive)
        depth = 0
        for t in toks:
            if t.text == "(":
                depth += 1
            elif t.text == ")":
                depth -= 1
            elif depth == 0 and t.upper in ("INSERT", "MERGE", "UPDATE", "DELETE"):
                return t.upper
    return None


def _preview(text: str, size: int = 70) -> str:
    sem_comentarios = re.sub(r"--[^\n]*|/\*.*?\*/", " ", text, flags=re.S)
    resumo = " ".join(sem_comentarios.split())
    return resumo if len(resumo) <= size else resumo[: size - 1].rstrip() + "…"


def find_write_statements(sql: str) -> list[WriteStatement]:
    """Comandos que gravam ou alteram dados/catálogo (INSERT, CREATE TABLE, DROP, ALTER, MERGE...).

    Comandos que só afetam a sessão são permitidos: SELECT, CREATE TEMP VIEW, CACHE TABLE, SET, USE...
    """
    out = []
    for numero, (line0, text) in enumerate(split_statements_with_lines(sql or ""), start=1):
        kw = _write_keyword(_tokens(text, line0))
        if kw:
            out.append(WriteStatement(numero, line0, kw, _preview(text)))
    return out


# --------------------------------------------------------------------------------------------
# Erro do Spark -> base faltando
# --------------------------------------------------------------------------------------------
_SPARK_MISSING_RES = [
    (re.compile(r"The table or view (\S+) cannot be found"), "tabela"),          # Spark 3.4+
    (re.compile(r"Table or view not found: ([^;\s]+)"), "tabela"),               # Spark 3.3 (Glue 4)
    (re.compile(r"The schema (\S+) cannot be found"), "banco"),                  # Spark 3.4+
    (re.compile(r"Database '([^']+)' not found"), "banco"),                      # Spark 3.3
]


def missing_from_spark_error(message: str) -> tuple[str, str] | None:
    """Reconhece o erro de tabela/banco inexistente do Spark. Devolve (tipo, nome) ou None."""
    for rx, kind in _SPARK_MISSING_RES:
        m = rx.search(message or "")
        if m:
            return kind, _norm(m.group(1).rstrip(".,"))
    return None
