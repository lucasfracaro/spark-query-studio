"""
Tabelas `tabela$partitions` do Athena e subqueries de "partição mais recente".

No Athena, `"db"."tabela$partitions"` é uma tabela virtual com a lista de partições de `db.tabela`; ela é o
jeito barato de achar a partição mais recente:

    WHERE anomes = (SELECT max(anomes) FROM "db"."tabela$partitions")

No Spark ela não existe. Na importação, a consulta a ela é calculada aqui com a lista de partições do
Glue Data Catalog (GetPartitions, só metadados) e trocada pelo valor encontrado. Assim a query roda no
Spark e o filtro vira um valor fixo, que vai para a orquestração (só aquela partição é lida).

O mesmo cálculo serve para `(SELECT max(coluna_de_particao) FROM db.tabela)`, que no Spark lê a tabela
inteira para achar o máximo. Por padrão a importação só avisa (com o valor que dá hoje, pelo catálogo);
com `resolve_max`, troca pelo valor também.

As expressões em volta do max/min também são calculadas (as funções de data mais usadas do Athena), por
exemplo `date_format(date_add('month', -1, max(dt)), '%Y%m')`.
"""
from __future__ import annotations

import calendar
import re
import time
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import sizing

try:
    from sqlglot import exp
except ImportError:  # pragma: no cover - tratado na tela
    exp = None

# tabela$partitions, tabela$properties, tabela$files, tabela$snapshots... (tabelas de metadados do Athena)
META_TABLE_RE = re.compile(r"^(?P<tabela>.+)\$(?P<tipo>[a-z_]+)$", re.IGNORECASE)
LIST_BUDGET_S = 20.0

# (database, tabela, filtro no formato do Glue ou None) -> ([(coluna, tipo)], [valores de cada partição])
PartitionValues = Callable[[str, str, "str | None"], "tuple[list[tuple[str, str]], list[list[str]]]"]


class Unresolvable(Exception):
    """A consulta usa algo que não dá para calcular só com a lista de partições."""


def meta_table(table) -> tuple[str, str] | None:
    """(tabela, tipo) se o nó for uma tabela de metadados do Athena (`tabela$partitions`...), senão None."""
    m = META_TABLE_RE.match(table.name or "")
    return (m.group("tabela"), m.group("tipo").lower()) if m else None


def glue_partition_values(glue_client, budget_s: float = LIST_BUDGET_S) -> PartitionValues:
    """Lista de partições pelo Glue (GetPartitions, só metadados), com cache por tabela e filtro."""
    cache: dict = {}

    def values(database: str, table: str, expression: str | None = None):
        chave = (database.lower(), table.lower(), expression or "")
        if chave not in cache:
            chaves, linhas, completo = sizing.partition_values(glue_client, database, table, expression,
                                                               time.monotonic() + budget_s)
            if not completo:
                raise Unresolvable("a tabela tem partições demais para listar a tempo")
            cache[chave] = (chaves, linhas)
        return cache[chave]

    return values


# --------------------------------------------------------------------------------------------
# Valores: (tipo, valor), com tipo "num" (Decimal), "date" (date) ou "str"
# --------------------------------------------------------------------------------------------
_NUM, _DATE, _STR = "num", "date", "str"


def _to_date(texto: str) -> date:
    texto = str(texto).strip()
    for formato, tamanho in (("%Y-%m-%d", 10), ("%Y%m%d", 8)):
        try:
            return datetime.strptime(texto[:tamanho], formato).date()
        except ValueError:
            pass
    raise Unresolvable(f"`{texto}` não é uma data")


def _typed(valor: str, tipo: str) -> tuple[str, object]:
    tipo = (tipo or "").strip().lower()
    if tipo == "date":
        try:
            return _DATE, _to_date(valor)
        except Unresolvable:
            pass
    if sizing._is_numeric(tipo):
        numero = sizing._number(valor)
        if numero is not None:
            return _NUM, numero
    return _STR, valor


def _text(valor: tuple[str, object]) -> str:
    tipo, v = valor
    if tipo == _NUM:
        return format(v, "f")
    if tipo == _DATE:
        return v.isoformat()
    return str(v)


def _sort_key(valor: tuple[str, object]):
    tipo, v = valor
    if tipo == _NUM:
        return (0, v, "")
    if tipo == _DATE:
        return (1, Decimal(v.toordinal()), "")
    return (2, Decimal(0), str(v))


def literal(valor: tuple[str, object]):
    """O valor como literal SQL: número sem aspas, data como DATE '...', texto com aspas."""
    if valor[0] == _NUM:
        return exp.Literal.number(_text(valor))
    if valor[0] == _DATE:
        return exp.Cast(this=exp.Literal.string(_text(valor)), to=exp.DataType.build("date"))
    return exp.Literal.string(_text(valor))


def _add_months(d: date, meses: int) -> date:
    m = d.month - 1 + meses
    ano, mes = d.year + m // 12, m % 12 + 1
    return date(ano, mes, min(d.day, calendar.monthrange(ano, mes)[1]))


def _shift(d: date, quantidade: int, unidade: str) -> date:
    unidade = unidade.upper().rstrip("S")
    if unidade == "DAY":
        return date.fromordinal(d.toordinal() + quantidade)
    if unidade == "WEEK":
        return date.fromordinal(d.toordinal() + 7 * quantidade)
    if unidade == "MONTH":
        return _add_months(d, quantidade)
    if unidade == "QUARTER":
        return _add_months(d, 3 * quantidade)
    if unidade == "YEAR":
        return _add_months(d, 12 * quantidade)
    raise Unresolvable(f"unidade de data `{unidade}`")


# formatos do Athena (estilo MySQL e Joda) para o strftime do Python
_PRESTO_FORMAT = {"%i": "%M", "%s": "%S", "%T": "%H:%M:%S", "%e": "%d", "%c": "%m", "%h": "%I", "%k": "%H"}
_JODA_FORMAT = (("yyyy", "%Y"), ("yy", "%y"), ("MM", "%m"), ("dd", "%d"), ("HH", "%H"), ("mm", "%M"), ("ss", "%S"))


def _strftime(formato: str, joda: bool = False) -> str:
    if joda:
        for de, para in _JODA_FORMAT:
            formato = formato.replace(de, para)
        return formato
    for de, para in _PRESTO_FORMAT.items():
        formato = formato.replace(de, para)
    return formato


def _unit(node) -> str:
    unidade = node.args.get("unit")
    return (unidade.name if unidade is not None else "DAY") or "DAY"


def _int(valor: tuple[str, object]) -> int:
    try:
        return int(Decimal(_text(valor)))
    except (InvalidOperation, ValueError):
        raise Unresolvable(f"`{_text(valor)}` não é número") from None


def _value(node, linha: dict, agregados: dict) -> tuple[str, object]:
    """Valor de uma expressão. linha: {coluna: valor} de uma partição; agregados: max/min já calculados."""
    if id(node) in agregados:
        return agregados[id(node)]
    if isinstance(node, exp.Paren):
        return _value(node.this, linha, agregados)
    if isinstance(node, exp.Column) and not isinstance(node.this, exp.Star):
        nome = node.name.lower()
        if nome not in linha:
            raise Unresolvable(f"`{node.name}` não é coluna de partição")
        return linha[nome]
    if isinstance(node, exp.Literal):
        return (_STR, node.this) if node.is_string else (_NUM, Decimal(node.this))
    if isinstance(node, exp.Neg):
        return _NUM, -Decimal(_int(_value(node.this, linha, agregados)))
    if isinstance(node, exp.Cast):
        tipo, v = _value(node.this, linha, agregados)
        if node.to.is_type(exp.DataType.Type.DATE):
            return _DATE, (v if tipo == _DATE else _to_date(_text((tipo, v))))
        if node.to.is_type(*exp.DataType.TEXT_TYPES):
            return _STR, _text((tipo, v))
        if node.to.is_type(*exp.DataType.NUMERIC_TYPES):
            try:
                return _NUM, Decimal(_text((tipo, v)).strip())
            except InvalidOperation:
                raise Unresolvable(f"`{_text((tipo, v))}` não é número") from None
        raise Unresolvable(node.sql(dialect="athena"))
    if isinstance(node, (exp.Concat, exp.DPipe)):
        partes = node.expressions if isinstance(node, exp.Concat) else [node.this, node.expression]
        return _STR, "".join(_text(_value(p, linha, agregados)) for p in partes)
    if isinstance(node, (exp.StrToTime, exp.StrToDate)):            # date_parse(texto, '%Y%m')
        texto = _text(_value(node.this, linha, agregados))
        formato = node.args.get("format")
        try:
            return _DATE, datetime.strptime(texto, _strftime(formato.this if formato is not None else "%Y-%m-%d")).date()
        except ValueError:
            raise Unresolvable(f"`{texto}` não está no formato da data") from None
    if isinstance(node, (exp.DateAdd, exp.DateSub)):                # date_add('month', -1, d)
        tipo, d = _value(node.this, linha, agregados)
        d = d if tipo == _DATE else _to_date(_text((tipo, d)))
        n = _int(_value(node.expression, linha, agregados))
        return _DATE, _shift(d, -n if isinstance(node, exp.DateSub) else n, _unit(node))
    if isinstance(node, (exp.Add, exp.Sub)) and isinstance(node.expression, exp.Interval):   # d - interval '1' month
        tipo, d = _value(node.this, linha, agregados)
        d = d if tipo == _DATE else _to_date(_text((tipo, d)))
        n = _int(_value(node.expression.this, linha, agregados))
        return _DATE, _shift(d, -n if isinstance(node, exp.Sub) else n, _unit(node.expression))
    if isinstance(node, (exp.TimestampTrunc, exp.DateTrunc)):       # date_trunc('month', d)
        tipo, d = _value(node.this, linha, agregados)
        d = d if tipo == _DATE else _to_date(_text((tipo, d)))
        unidade = _unit(node).upper()
        if unidade == "MONTH":
            return _DATE, d.replace(day=1)
        if unidade == "YEAR":
            return _DATE, d.replace(month=1, day=1)
        if unidade == "DAY":
            return _DATE, d
        raise Unresolvable(f"date_trunc('{unidade.lower()}')")
    if isinstance(node, exp.TimeToStr) or (isinstance(node, exp.Anonymous)
                                           and node.name.lower() == "format_datetime"):
        alvo, formato = ((node.this, node.args.get("format")) if isinstance(node, exp.TimeToStr)
                         else (node.expressions[0], node.expressions[1]))
        tipo, d = _value(alvo, linha, agregados)
        d = d if tipo == _DATE else _to_date(_text((tipo, d)))
        return _STR, d.strftime(_strftime(formato.this, joda=isinstance(node, exp.Anonymous)))
    if isinstance(node, (exp.Year, exp.Month, exp.Day)):
        tipo, d = _value(node.this, linha, agregados)
        d = d if tipo == _DATE else _to_date(_text((tipo, d)))
        parte = {exp.Year: d.year, exp.Month: d.month, exp.Day: d.day}[type(node)]
        return _NUM, Decimal(parte)
    if isinstance(node, exp.Substring):
        texto = _text(_value(node.this, linha, agregados))
        inicio = _int(_value(node.args["start"], linha, agregados)) if node.args.get("start") else 1
        tamanho = node.args.get("length")
        fim = None if tamanho is None else inicio - 1 + _int(_value(tamanho, linha, agregados))
        return _STR, texto[inicio - 1:fim]
    if isinstance(node, exp.Pad):                                   # lpad/rpad
        texto = _text(_value(node.this, linha, agregados))
        tamanho = _int(_value(node.expression, linha, agregados))
        preenche = _text(_value(node.args["fill_pattern"], linha, agregados)) if node.args.get("fill_pattern") else " "
        return _STR, (texto.rjust if node.args.get("is_left") else texto.ljust)(tamanho, preenche[:1] or " ")
    raise Unresolvable(f"`{node.sql(dialect='athena')}`")


# --------------------------------------------------------------------------------------------
# Cálculo de um SELECT sobre a lista de partições
# --------------------------------------------------------------------------------------------
def _projections(select, chaves: list[tuple[str, str]]) -> list[tuple[str, object]]:
    out = []
    for i, e in enumerate(select.expressions):
        if isinstance(e, exp.Star):
            out += [(nome, exp.column(nome)) for nome, _ in chaves]
            continue
        corpo = e.this if isinstance(e, exp.Alias) else e
        nome = e.alias if isinstance(e, exp.Alias) else (corpo.name if isinstance(corpo, exp.Column) else "")
        if not nome and isinstance(corpo, (exp.Max, exp.Min)) and isinstance(corpo.this, exp.Column):
            nome = corpo.this.name
        out.append((nome or f"_col{i}", corpo))
    return out


def evaluate(select, chaves: list[tuple[str, str]], valores: list[list[str]]) -> tuple[list[str], list[list]]:
    """Resultado do SELECT (sobre a lista de partições): (nomes das colunas, linhas de valores).

    Cobre o que se usa para achar a partição mais recente: colunas, max/min (também dentro de funções de
    data, concat, substr, lpad, cast), WHERE com colunas de partição, DISTINCT, ORDER BY e LIMIT."""
    for arg in ("joins", "group", "having", "windows", "offset", "laterals"):
        if select.args.get(arg):
            raise Unresolvable(f"a consulta tem {arg.upper()}")
    tipos = dict(chaves)
    nomes = [n for n, _ in chaves]
    linhas = [{n: _typed(v, tipos[n]) for n, v in zip(nomes, vals, strict=False)} for vals in valores]
    where = select.args.get("where")
    if where is not None:
        try:
            linhas = [linha for linha, vals in zip(linhas, valores, strict=False)
                      if sizing.partition_matches(where.this, dict(zip(nomes, vals, strict=False)), tipos)]
        except sizing._Unsupported as e:
            raise Unresolvable(f"o filtro usa `{e}`") from None

    projecoes = _projections(select, chaves)
    com_agregado = [c.find(exp.Max, exp.Min) is not None for _, c in projecoes]
    if any(com_agregado):
        if not all(com_agregado):
            raise Unresolvable("a consulta mistura max/min com outras colunas")
        if not linhas:
            raise Unresolvable("nenhuma partição atende ao filtro")
        resultado = []
        for _, c in projecoes:
            agregados = {}
            for a in c.find_all(exp.Max, exp.Min):
                escolha = max if isinstance(a, exp.Max) else min
                agregados[id(a)] = escolha((_value(a.this, linha, {}) for linha in linhas), key=_sort_key)
            resultado.append(_value(c, {}, agregados))
        return [n for n, _ in projecoes], [resultado]

    pares = [([_value(c, linha, {}) for _, c in projecoes], linha) for linha in linhas]
    if select.args.get("distinct"):
        vistos, unicos = set(), []
        for proj, linha in pares:
            chave = tuple(_text(v) for v in proj)
            if chave not in vistos:
                vistos.add(chave)
                unicos.append((proj, linha))
        pares = unicos
    ordem = select.args.get("order")
    if ordem is not None:
        por_nome = {n.lower(): i for i, (n, _) in enumerate(projecoes)}
        for o in reversed(ordem.expressions):            # ordenação estável: do último critério para o primeiro
            alvo, desc = o.this, bool(o.args.get("desc"))
            if isinstance(alvo, exp.Literal) and not alvo.is_string:
                i = int(alvo.this) - 1
                if not 0 <= i < len(projecoes):
                    raise Unresolvable(f"ORDER BY {alvo.this}")
                pares.sort(key=lambda p, i=i: _sort_key(p[0][i]), reverse=desc)
            elif isinstance(alvo, exp.Column) and alvo.name.lower() in por_nome:
                i = por_nome[alvo.name.lower()]
                pares.sort(key=lambda p, i=i: _sort_key(p[0][i]), reverse=desc)
            else:
                pares.sort(key=lambda p, alvo=alvo: _sort_key(_value(alvo, p[1], {})), reverse=desc)
    limite = select.args.get("limit")
    if limite is not None:
        valor = limite.args.get("expression")
        if not (isinstance(valor, exp.Literal) and not valor.is_string):
            raise Unresolvable("LIMIT sem número")
        pares = pares[:int(valor.this)]
    return [n for n, _ in projecoes], [proj for proj, _ in pares]


# --------------------------------------------------------------------------------------------
# Troca da subquery pelo valor
# --------------------------------------------------------------------------------------------
_COMPARACOES = ("EQ", "NEQ", "GT", "GTE", "LT", "LTE")


def _is_comparison(node) -> bool:
    return any(type(node) is getattr(exp, nome) for nome in _COMPARACOES)


def _constant_select(nomes: list[str], linhas: list[list]):
    """SELECT de valores fixos com as mesmas colunas (para quando a subquery não está num filtro simples)."""
    if len(linhas) == 1:
        return exp.select(*[literal(v).as_(n) for n, v in zip(nomes, linhas[0], strict=False)])
    valores = exp.Values(expressions=[exp.Tuple(expressions=[literal(v) for v in linha]) for linha in linhas],
                         alias=exp.TableAlias(this=exp.to_identifier("_p"),
                                              columns=[exp.to_identifier(n) for n in nomes]))
    return exp.select("*").from_(valores)


def _summary(nomes: list[str], linhas: list[list]) -> str:
    if len(nomes) == 1 and nomes[0].startswith("_col"):
        resumo = _text(linhas[0][0])
    else:
        resumo = ", ".join(f"{n} = {_text(v)}" for n, v in zip(nomes, linhas[0], strict=False))
    return f"{len(linhas)} valores ({resumo}, …)" if len(linhas) > 1 else resumo


def substitute(select, nomes: list[str], linhas: list[list]) -> str:
    """Troca a consulta pelo seu resultado no lugar onde ela é usada. Devolve um resumo do que entrou."""
    if not linhas:
        raise Unresolvable("a consulta não devolve nenhuma linha")
    resumo = _summary(nomes, linhas)
    sub = select.parent
    alvo = sub.parent if isinstance(sub, exp.Subquery) else None
    if alvo is not None and _is_comparison(alvo):
        outro = alvo.expression if alvo.this is sub else alvo.this
        if len(linhas) == 1 and len(nomes) == 1:
            sub.replace(literal(linhas[0][0]))
            return resumo
        if (isinstance(outro, exp.Tuple) and type(alvo) is exp.EQ and len(linhas) == 1
                and len(outro.expressions) == len(nomes)):
            iguais = [exp.EQ(this=c.copy(), expression=literal(v))
                      for c, v in zip(outro.expressions, linhas[0], strict=False)]
            alvo.replace(exp.Paren(this=exp.and_(*iguais)))
            return resumo
    if isinstance(alvo, exp.In) and alvo.args.get("query") is sub:
        if isinstance(alvo.this, exp.Tuple) and len(alvo.this.expressions) == len(nomes):
            if len(linhas) == 1:
                iguais = [exp.EQ(this=c.copy(), expression=literal(v))
                          for c, v in zip(alvo.this.expressions, linhas[0], strict=False)]
                alvo.replace(exp.Paren(this=exp.and_(*iguais)))
            else:
                alvo.replace(exp.In(this=alvo.this.copy(), expressions=[
                    exp.Tuple(expressions=[literal(v) for v in linha]) for linha in linhas]))
            return resumo
        if len(nomes) == 1:
            alvo.replace(exp.In(this=alvo.this.copy(), expressions=[literal(linha[0]) for linha in linhas]))
            return resumo
    novo = _constant_select(nomes, linhas)
    if select.parent is None:
        raise _ReplaceRoot(novo, resumo)
    select.replace(novo)
    return resumo


class _ReplaceRoot(Exception):
    """A consulta é o comando inteiro: quem tem a lista de comandos troca."""

    def __init__(self, novo, resumo: str):
        super().__init__(resumo)
        self.novo, self.resumo = novo, resumo


def inline_constant_ctes(statements: list):
    """CTEs de uma linha só com valores fixos (ex.: a partição mais recente, já calculada) entram direto nos
    filtros e JOINs que as usam: `v.anomes = u.anomes` vira `v.anomes = '202609'`, que vai para a orquestração."""
    for stmt in statements:
        constantes = {}
        for cte in stmt.find_all(exp.CTE):
            corpo = cte.this
            if (isinstance(corpo, exp.Select) and corpo.args.get(_from_key()) is None and corpo.expressions
                    and all(isinstance(e, exp.Alias) and isinstance(e.this, (exp.Literal, exp.Cast))
                            for e in corpo.expressions)):
                constantes[cte.alias_or_name.lower()] = {e.alias.lower(): e.this for e in corpo.expressions}
        if not constantes:
            continue
        # (SELECT coluna FROM cte) -> valor
        for sub in list(stmt.find_all(exp.Subquery)):
            sel = sub.this
            frm = sel.args.get(_from_key()) if isinstance(sel, exp.Select) else None
            fonte = frm.this if frm is not None else None
            if (isinstance(fonte, exp.Table) and not fonte.db and fonte.name.lower() in constantes
                    and not sel.args.get("joins") and not sel.args.get("where") and len(sel.expressions) == 1):
                e = sel.expressions[0]
                coluna = e.this if isinstance(e, exp.Alias) else e
                valor = constantes[fonte.name.lower()].get(coluna.name.lower()) if isinstance(coluna, exp.Column) else None
                if valor is not None and _predicate_of(sub) is not None:
                    sub.replace(valor.copy())
        # alias.coluna, no SELECT que junta a CTE -> valor
        for sel in list(stmt.find_all(exp.Select)):
            fontes = [sel.args.get(_from_key()).this] if sel.args.get(_from_key()) is not None else []
            fontes += [j.this for j in sel.args.get("joins") or []]
            apelidos = {t.alias_or_name.lower(): constantes[t.name.lower()] for t in fontes
                        if isinstance(t, exp.Table) and not t.db and t.name.lower() in constantes}
            if not apelidos:
                continue
            for c in list(sel.find_all(exp.Column)):
                if c.find_ancestor(exp.Select) is not sel or not c.table:
                    continue
                valores = apelidos.get(c.table.lower())
                if valores is not None and c.name.lower() in valores and c.find_ancestor(exp.Where, exp.Join):
                    c.replace(valores[c.name.lower()].copy())


def _snippet(node, limite: int = 140) -> str:
    texto = " ".join(node.sql(dialect="athena").split())
    return texto if len(texto) <= limite else texto[:limite - 1] + "…"


def _predicate_of(sub):
    """O filtro (comparação ou IN) em que a subquery é usada, ou None."""
    alvo = sub.parent
    if _is_comparison(alvo) or (isinstance(alvo, exp.In) and alvo.args.get("query") is sub):
        return alvo
    return None


def _from_key() -> str:
    return "from_" if "from_" in exp.Select.arg_types else "from"


# --------------------------------------------------------------------------------------------
# Na importação
# --------------------------------------------------------------------------------------------
def resolve(statements: list, values: PartitionValues | None, default_database: str = "",
            resolve_max: bool = False) -> list[tuple[str, str]]:
    """Trata as tabelas `$partitions` e as subqueries com max/min das queries (altera as árvores).

    Devolve observações (nível, texto) para o relatório da importação."""
    notas: list[tuple[str, str]] = []
    if exp is None:
        return notas
    cte_names = {c.alias_or_name.lower() for s in statements for c in s.find_all(exp.CTE)}
    feitas: dict[str, list[str]] = {}

    # 1) tabela$partitions: não existe no Spark. Vira o valor que está no catálogo agora.
    for indice in range(len(statements)):
        for t in list(statements[indice].find_all(exp.Table)):
            meta = meta_table(t)
            if meta is None or t.find_ancestor(exp.Select) is None:
                continue
            base, tipo = meta
            db = (t.db or default_database or "").strip()
            nome = f"{db}.{t.name}" if db else t.name
            if tipo != "partitions":
                notas.append(("atencao", f"`{nome}` é uma tabela de metadados do Athena e não existe no Spark: "
                                         "troque a parte da query que usa ela."))
                continue
            select = t.parent.parent if isinstance(t.parent, exp.From) else None
            try:
                if not isinstance(select, exp.Select):
                    raise Unresolvable("ela não está num SELECT simples")
                if values is None:
                    raise Unresolvable("sem acesso ao Glue para ler a lista de partições")
                if not db:
                    raise Unresolvable("a tabela está sem database")
                chaves, linhas = values(db, base, None)
                if not chaves:
                    raise Unresolvable(f"`{db}.{base}` não é particionada")
                try:
                    feitas.setdefault(nome, []).append(substitute(select, *evaluate(select, chaves, linhas)))
                except _ReplaceRoot as troca:
                    statements[indice] = troca.novo
                    feitas.setdefault(nome, []).append(troca.resumo)
            except Unresolvable as e:
                notas.append(("atencao", f"`{nome}` é uma tabela do Athena com a lista de partições e não existe "
                                         f"no Spark, e não consegui trocá-la por um valor ({e}). Troque a subquery "
                                         "por um valor fixo antes de executar (a partição mais recente: no Athena, "
                                         f'`SELECT * FROM "{db}"."{t.name}" ORDER BY 1 DESC LIMIT 1`).'))
            except Exception as e:          # sem permissão no Glue, tabela que não existe...
                notas.append(("atencao", f"`{nome}` é uma tabela do Athena com a lista de partições e não existe "
                                         f"no Spark, e não consegui ler as partições no Glue ({type(e).__name__}). "
                                         "Troque a subquery por um valor fixo antes de executar."))
    for nome, resumos in feitas.items():
        notas.append(("atencao", f"`{nome}` é uma tabela do Athena com a lista de partições e não existe no Spark: "
                                 f"a consulta a ela foi trocada pelo valor que está no catálogo agora "
                                 f"({'; '.join(dict.fromkeys(resumos))}). Esse valor fica fixo: para pegar uma "
                                 "partição mais nova depois, importe a query de novo."))

    # 2) filtro com subquery que calcula max/min (ou ORDER BY ... LIMIT) lendo uma base: no Spark, ela é lida inteira
    for stmt in statements:
        for sub in list(stmt.find_all(exp.Subquery)):
            select = sub.this
            filtro = _predicate_of(sub)
            if filtro is None or not isinstance(select, exp.Select):
                continue
            agregados = list(select.find_all(exp.Max, exp.Min))
            ordenado = select.args.get("order") is not None and select.args.get("limit") is not None
            frm = select.args.get(_from_key())
            fonte = frm.this if frm is not None else None
            if not (agregados or ordenado) or not isinstance(fonte, exp.Table) or select.args.get("joins"):
                continue
            if meta_table(fonte) is not None or (not fonte.db and fonte.name.lower() in cte_names):
                continue
            db = (fonte.db or default_database or "").strip()
            nome = f"{db}.{fonte.name}" if db else fonte.name
            trecho = _snippet(filtro)
            chaves, linhas = None, []
            if values is not None and db:
                try:
                    chaves, linhas = values(db, fonte.name, None)
                except Exception:
                    chaves = None
            particoes = {n for n, _ in chaves or []}
            colunas = {c.name.lower() for c in select.find_all(exp.Column)}
            calculado = None
            if chaves and colunas and colunas <= particoes:
                try:
                    calculado = evaluate(select, chaves, linhas)
                except Unresolvable:
                    calculado = None
            if calculado is not None and calculado[1] and resolve_max:
                resumo = substitute(select, *calculado)
                notas.append(("atencao", f"`{trecho}` foi trocado pelo valor que dá hoje pela lista de partições do "
                                         f"catálogo ({resumo}), para o Spark não ler `{nome}` inteira. Esse valor "
                                         "fica fixo: importe a query de novo para atualizar. Se a partição mais "
                                         "recente puder estar vazia, confira."))
                continue
            texto = (f"`{trecho}`: para calcular o {'máximo/mínimo' if agregados else 'primeiro valor'}, o Spark lê "
                     f"a tabela `{nome}` inteira (no Athena isso costuma ser rápido; no Spark, todas as partições são "
                     "lidas).")
            if calculado is not None and calculado[1]:
                texto += (f" Pela lista de partições do catálogo, a subquery dá hoje {_summary(*calculado)}: troque-a "
                          "por esse valor fixo, ou marque **Trocar max/min de colunas de partição pelo valor do "
                          "catálogo** e gere a orquestração de novo.")
            else:
                texto += " Se der, troque a subquery por um valor fixo."
            notas.append(("atencao", texto))

    inline_constant_ctes(statements)
    return notas
