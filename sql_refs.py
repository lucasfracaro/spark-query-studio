"""
Descobre quais bases (tabelas/views) uma query Spark SQL usa e confere se todas estão na orquestração.

Não é um parser SQL completo: é um leitor de tokens que entende o suficiente para achar o que
vem depois de FROM e JOIN (incluindo listas com vírgula), ignorando strings, comentários,
subqueries, funções (EXTRACT(... FROM ...), range(10)...), CTEs (WITH x AS ...) e views criadas
na própria query (CREATE TEMP VIEW x ...).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# --------------------------------------------------------------------------------------------
# Tokenização
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


def _statements(sql: str) -> list[list[_Tok]]:
    clean = _blank_strings_and_comments(sql)
    stmts, cur = [], []
    for m in _TOKEN_RE.finditer(clean):
        text = m.group()
        if text == ";":
            if cur:
                stmts.append(cur)
            cur = []
            continue
        cur.append(_Tok(text, text.upper(), clean.count("\n", 0, m.start()) + 1))
    if cur:
        stmts.append(cur)
    return stmts


def _norm(name: str) -> str:
    return ".".join(p.strip("`").replace("``", "`").lower() for p in name.split("."))


# --------------------------------------------------------------------------------------------
# Leitura das referências
# --------------------------------------------------------------------------------------------
@dataclass
class TableRef:
    name: str          # como apareceu, normalizado (minúsculas, sem crases): "db.tabela" ou "view"
    line: int


@dataclass
class QueryTables:
    refs: list[TableRef] = field(default_factory=list)     # bases lidas (FROM/JOIN)
    defined: set[str] = field(default_factory=set)         # CTEs e views/tabelas criadas na query


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


def find_tables(sql: str) -> QueryTables:
    result = QueryTables()
    for toks in _statements(sql):
        result.defined |= _defined_names(toks)
        if toks[0].upper in _SKIP_STATEMENTS:
            continue

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
            i += 1
    return result


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
class OrchestrationCheck:
    missing: list[MissingTable] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing


def check_query_against_orchestration(sql: str, inputs) -> OrchestrationCheck:
    """inputs: lista de code_builder.InputSource (só as válidas são consideradas)."""
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
                aviso = (f"`{ref.name}` está na orquestração com o alias `{inp.alias.strip()}`, mas a query usa "
                         f"o nome completo (linha {ref.line}). Use o alias")
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
