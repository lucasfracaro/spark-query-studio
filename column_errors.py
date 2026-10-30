"""
Coluna que o Spark não encontrou ("Column 'x' does not exist", UNRESOLVED_COLUMN).

A mensagem do Spark só diz o nome e lista as colunas que ele enxerga naquele ponto da query (no Spark 3.3, do
Glue 4.0, todas elas: numa tabela larga, centenas), sem dizer o motivo. Aqui sai o motivo provável, comparando
três listas de colunas de cada entrada da orquestração:
- as que a leitura trouxe (o DataFrame da view), vindas da sessão junto com o erro;
- as que a role da sessão vê no catálogo, também vindas da sessão. Com o Lake Formation, o catálogo só mostra
  a cada um as colunas que ele tem permissão de ler (o glue:GetTable já vem filtrado);
- as que o seu usuário da AWS vê no catálogo (user_catalog_columns, consultado pelo app depois do erro).

Os casos:
- "alias_do_select": o nome é de uma coluna calculada num SELECT da própria query (usado no WHERE, ou no mesmo
  SELECT no Spark 3.3);
- "alias": o alias antes do ponto (t.coluna) não é de nenhuma tabela daquele trecho;
- "fora_do_alcance": uma entrada tem a coluna, mas ela não chega àquele ponto da query (CTE/subquery que não
  a seleciona, ou alias de outra tabela);
- "arquivos": a role vê a coluna no catálogo, mas a leitura não a trouxe (pelo GlueContext, as colunas vêm dos
  arquivos lidos: as partições escolhidas não têm a coluna);
- "role": o seu usuário vê a coluna e a role da sessão não. É permissão do Lake Formation: o Athena usa a sua
  permissão, a sessão usa a da role;
- "nao_existe": nenhuma entrada tem a coluna, nem no catálogo: nome errado (com os nomes parecidos);
- "desconhecido": não deu para conferir (sem as listas): o que dá para dizer pela mensagem.

O Spark para na primeira coluna que não encontra; quando as listas mostram outras colunas que a query usa e
que faltam pelo mesmo motivo, elas vão juntas, para não ser uma por execução.
"""
from __future__ import annotations

import difflib
import re
import time
from dataclasses import dataclass, field

_ID = r"`(?:[^`]|``)*`"                                  # identificador entre crases (`` é uma crase)
_MULTI = rf"{_ID}(?:\.{_ID})*"

# (expressão, a lista do Spark traz todas as colunas visíveis?)
_FORMATOS = (
    # Spark 3.4+ (Glue 5.x e 6.0): só as 5 colunas mais parecidas
    (re.compile(rf"\[UNRESOLVED_COLUMN\.\w+\] A column.{{0,40}}? with name (?P<nome>{_MULTI}) cannot be "
                r"resolved\.(?: Did you mean one of the following\? \[(?P<lista>[^\n]*?)\]\.?)?"), False),
    # Spark 3.3 (Glue 4.0): todas as colunas visíveis
    (re.compile(r"Column '(?P<nome>[^\n]+?)' does not exist\.(?: Did you mean one of the following\? "
                r"\[(?P<lista>[^\n]*?)\](?=;|\s*$))?", re.M), True),
    # Spark 3.1 e 3.2 (Glue 3.0)
    (re.compile(r"cannot resolve '(?P<nome>[^\n]+?)' given input columns: \[(?P<lista>[^\n]*?)\](?=;|\s*$)",
                re.M), True),
)
_CAMPO = (
    (re.compile(rf"\[UNRESOLVED_FIELD\.\w+\] A field with name (?P<nome>{_MULTI}) cannot be resolved with the "
                rf"struct-type column (?P<struct>{_MULTI})\.(?: Did you mean one of the following\? "
                r"\[(?P<lista>[^\n]*?)\]\.?)?"), False),
    (re.compile(r"No such struct field (?P<nome>\S+) in (?P<lista>[^;\n]*)"), True),
)
# Spark 3.3 (Glue 4.0), coluna que falta numa CTE que a query final não usa: no lugar do nome da coluna, o operador
# inteiro ("unresolved operator 'Project [...]"; corrigido no Spark 3.4, SPARK-40595). No texto do operador, as
# colunas encontradas têm número (DATA#3768) e as que não foram encontradas vêm com ' na frente ('DATA_REF).
_OPERADOR = re.compile(r"unresolved operator '(?P<op>\w+)(?P<resto>[^\n]*)")
_IDENT = r"(?:`(?:[^`]|``)*`|[A-Za-z_][\w$]*)"
_NAO_RESOLVIDA = re.compile(rf"(?<![\w#'`])'({_IDENT}(?:\.{_IDENT})*)(?![\w$(`])")
_RESOLVIDA = re.compile(rf"(?<![\w'`.])({_IDENT})#\d+")
_SEM_COMENTARIOS = re.compile(r"--[^\n]*|/\*.*?\*/", re.S)
_MAX_LISTA = 12                   # colunas listadas numa frase; o resto vira "e mais N"
_TALVEZ_PERMISSAO = ("Se no Athena essa query roda, o motivo provável é permissão: com o Lake Formation, a role da "
                     "sessão só enxerga no catálogo as colunas que pode ler (o Athena usa a sua permissão).")


# --------------------------------------------------------------------------------------------
# A mensagem do Spark
# --------------------------------------------------------------------------------------------
@dataclass
class UnresolvedColumn:
    name: str                                   # como na query, sem crases: "t.x" ou "x"
    column: str                                 # só a coluna: "x"
    qualifier: str                              # o que vem antes do nome (alias da tabela): "t" ou ""
    candidates: list[tuple[str, str]]           # (qualificador, coluna) que o Spark listou, na ordem dele
    full_list: bool                             # o Spark listou todas as colunas visíveis (3.3) ou só as parecidas
    struct: str = ""                            # campo de struct: a coluna struct onde ele foi procurado
    # "unresolved operator": as outras colunas não encontradas, o operador e os nomes que ele cria (AS x)
    others: list[str] = field(default_factory=list)
    operator: str = ""
    outputs: list[str] = field(default_factory=list)


def _partes(identificador: str) -> list[str]:
    """`a`.`b` / a.b / `a.b` (um nome com ponto) -> partes, sem as crases."""
    partes, atual, i, crase = [], "", 0, False
    texto = identificador.strip()
    while i < len(texto):
        ch = texto[i]
        if crase:
            if ch == "`" and texto[i + 1:i + 2] == "`":
                atual += "`"
                i += 1
            elif ch == "`":
                crase = False
            else:
                atual += ch
        elif ch == "`":
            crase = True
        elif ch == ".":
            partes.append(atual)
            atual = ""
        else:
            atual += ch
        i += 1
    partes.append(atual)
    return [p.strip() for p in partes if p.strip()]


def _itens(lista: str | None) -> list[str]:
    """Separa a lista do Spark nas vírgulas que estão fora de crases."""
    itens, atual, crase = [], "", False
    for ch in lista or "":
        if ch == "`":
            crase = not crase
        if ch == "," and not crase:
            itens.append(atual.strip())
            atual = ""
        else:
            atual += ch
    if atual.strip():
        itens.append(atual.strip())
    return [i for i in itens if i]


def _candidatos(lista: str | None) -> list[tuple[str, str]]:
    saida = []
    for item in _itens(lista):
        partes = _partes(item)
        if partes:
            saida.append((partes[-2] if len(partes) > 1 else "", partes[-1]))
    return saida


def parse(error: str) -> UnresolvedColumn | None:
    """Reconhece o erro de coluna (ou campo de struct) não encontrada, nos formatos do Spark 3.1 ao 4.x."""
    texto = error or ""
    for regex, completa in _FORMATOS:
        m = regex.search(texto)
        if m:
            nome = m.group("nome")
            if regex is _FORMATOS[-1][0]:            # Spark 3.1 e 3.2: '`t.x`', o nome inteiro entre crases
                nome = nome.strip("`")
            partes = _partes(nome) or [nome]
            return UnresolvedColumn(".".join(partes), partes[-1], partes[-2] if len(partes) > 1 else "",
                                    _candidatos(m.group("lista")), completa)
    for regex, completa in _CAMPO:
        m = regex.search(texto)
        if m:
            partes = _partes(m.group("nome")) or [m.group("nome")]
            struct = ".".join(_partes(m.groupdict().get("struct") or ""))
            return UnresolvedColumn(".".join(partes), partes[-1], "", _candidatos(m.group("lista")), completa,
                                    struct=struct or "?")
    m = _OPERADOR.search(texto)
    if m:
        resto = m.group("resto")
        nomes = list(dict.fromkeys(".".join(_partes(n)) for n in _NAO_RESOLVIDA.findall(resto)))
        if nomes:
            lidas, criadas = [], []
            for a in _RESOLVIDA.finditer(resto):
                (criadas if resto[:a.start()].endswith("AS ") else lidas).append(".".join(_partes(a.group(1))))
            partes = nomes[0].split(".")
            return UnresolvedColumn(nomes[0], partes[-1], partes[-2] if len(partes) > 1 else "",
                                    [("", n) for n in dict.fromkeys(lidas)], False, others=nomes[1:],
                                    operator=m.group("op"), outputs=list(dict.fromkeys(criadas)))
    return None


# --------------------------------------------------------------------------------------------
# Colunas que o seu usuário vê no catálogo (consultado pelo app depois do erro)
# --------------------------------------------------------------------------------------------
def _db_tabela(source: str, default_database: str) -> tuple[str, str] | None:
    partes = [p.strip("`") for p in source.strip().split(".")]
    if len(partes) >= 2:
        return partes[-2], partes[-1]
    return (default_database.strip(), partes[0]) if default_database.strip() and partes[0] else None


def _colunas_da_tabela(tabela: dict) -> list[str]:
    sd = tabela.get("StorageDescriptor") or {}
    return [c.get("Name", "") for c in (sd.get("Columns") or []) + (tabela.get("PartitionKeys") or [])
            if c.get("Name")]


def user_catalog_columns(glue, inputs, default_database: str = "", budget_s: float = 8.0) -> dict[str, dict]:
    """{alias: {"source": "db.tabela", "columns": [...]}} das entradas do catálogo, com a permissão do seu
    usuário da AWS (glue:GetTable). Uma entrada que não deu para ler fica com {"error": "..."}."""
    saida: dict[str, dict] = {}
    limite = time.monotonic() + budget_s
    for inp in inputs:
        if getattr(inp, "kind", "") != "catalog" or not inp.is_valid():
            continue
        if time.monotonic() > limite:
            break
        alias = inp.alias.strip()
        nome = _db_tabela(inp.source, default_database)
        if nome is None:
            continue
        db, tab = nome
        try:
            tabela = glue.get_table(DatabaseName=db, Name=tab)["Table"]
            colunas = _colunas_da_tabela(tabela)
            alvo = tabela.get("TargetTable") or {}
            if not colunas and alvo.get("Name"):         # link do Lake Formation sem as colunas: a tabela de origem
                pedido = {"DatabaseName": alvo.get("DatabaseName") or db, "Name": alvo["Name"]}
                if alvo.get("CatalogId"):
                    pedido["CatalogId"] = alvo["CatalogId"]
                colunas = _colunas_da_tabela(glue.get_table(**pedido)["Table"])
            saida[alias] = {"source": f"{db}.{tab}", "columns": colunas}
        except Exception as e:
            resposta = getattr(e, "response", None)
            codigo = (resposta.get("Error") or {}).get("Code") if isinstance(resposta, dict) else None
            saida[alias] = {"source": f"{db}.{tab}", "error": codigo or type(e).__name__}
    return saida


def needs_user_catalog(res: dict) -> bool:
    """O erro é de coluna que nenhuma entrada trouxe: vale conferir o que o seu usuário vê no catálogo."""
    uc = parse(res.get("error") or "")
    if uc is None or uc.struct:
        return False
    alvo = uc.column.lower()
    return not any(alvo in {c.lower() for c in (e.get("columns") or [])} for e in res.get("inputs") or [])


# --------------------------------------------------------------------------------------------
# O motivo
# --------------------------------------------------------------------------------------------
@dataclass
class _Entrada:
    alias: str
    source: str
    kind: str
    read: str = ""
    columns: list[str] | None = None            # o que a leitura trouxe
    role: list[str] | None = None               # o que a role da sessão vê no catálogo
    user: list[str] | None = None               # o que o seu usuário vê no catálogo
    user_error: str = ""                        # por que não deu para ver (ex.: AccessDeniedException)

    def tem(self, lista: list[str] | None, coluna: str) -> bool:
        return lista is not None and coluna.lower() in {c.lower() for c in lista}

    def faltando(self, lista: list[str] | None) -> list[str]:
        """Colunas da lista (catálogo) que a leitura não trouxe."""
        if lista is None or self.columns is None:
            return []
        trazidas = {c.lower() for c in self.columns}
        return [c for c in lista if c.lower() not in trazidas]

    def nome(self) -> str:
        if self.source and self.source.lower() != self.alias.lower():
            return f"`{self.alias}` (`{self.source}`)"
        return f"`{self.alias}`"


@dataclass
class ColumnDiagnosis:
    kind: str                       # role | arquivos | fora_do_alcance | alias | alias_do_select | nao_existe
                                    # | desconhecido | campo
    title: str                      # uma linha
    cause: str                      # markdown: o motivo e o que fazer
    mark: str                       # o nome como está na query (para sublinhar na linha do erro)
    similar: list[str] = field(default_factory=list)       # nomes parecidos
    missing_too: list[str] = field(default_factory=list)   # outras colunas da query que faltam pelo mesmo motivo
    scope: str = ""                 # o que o Spark enxerga naquele ponto da query
    # onde está a coluna no comando (linha, posição), quando o Spark não diz ("unresolved operator")
    position: tuple[int, int] | None = None
    cte: str = ""                   # a CTE onde está o erro, quando se sabe


def _entradas(inputs_info, orch_inputs, user_catalog) -> list[_Entrada]:
    entradas: dict[str, _Entrada] = {}
    for inp in orch_inputs or []:
        if inp.is_valid():
            entradas[inp.alias.strip().lower()] = _Entrada(inp.alias.strip(), inp.source.strip(), inp.kind)
    for info in inputs_info or []:
        alias = str(info.get("alias") or "").strip()
        if not alias:
            continue
        e = entradas.setdefault(alias.lower(), _Entrada(alias, str(info.get("source") or ""),
                                                        str(info.get("kind") or "")))
        e.read = str(info.get("read") or "")
        e.columns = info.get("columns")
        e.role = info.get("catalog")
    for alias, info in (user_catalog or {}).items():
        e = entradas.get(str(alias).lower())
        if e is not None and info.get("columns") is not None:
            e.user = info["columns"]
        elif e is not None:
            e.user_error = str(info.get("error") or "")
    return list(entradas.values())


def _analisa_query(statement: str) -> tuple[dict[str, tuple[str, str]], set[str]]:
    """Nomes que a query usa para as tabelas: {alias: (tipo, nome)}, tipo = tabela | cte | subquery; e os
    aliases das colunas calculadas nos SELECTs (minúsculos)."""
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:  # pragma: no cover
        return {}, set()
    try:
        arvore = sqlglot.parse_one(statement, read="spark")
    except Exception:
        return {}, set()
    if arvore is None:
        return {}, set()
    # um "f(x) AS x" só formata a própria coluna: x continua sendo a coluna da tabela, não um nome novo
    calculadas = {e.alias.lower() for sel in arvore.find_all(exp.Select) for e in sel.expressions
                  if isinstance(e, exp.Alias) and e.alias
                  and not any(c.name.lower() == e.alias.lower() for c in e.this.find_all(exp.Column))}
    mapa: dict[str, tuple[str, str]] = {}
    ctes = {c.alias_or_name.lower() for c in arvore.find_all(exp.CTE) if c.alias_or_name}
    for cte in ctes:
        mapa[cte] = ("cte", cte)
    for t in arvore.find_all(exp.Table):
        nome = ".".join(p for p in (t.catalog, t.db, t.name) if p)
        if not nome:
            continue
        tipo = "cte" if t.name.lower() in ctes and not t.db else "tabela"
        mapa[t.alias_or_name.lower()] = (tipo, nome)
    for sq in arvore.find_all(exp.Subquery):
        if sq.alias:
            mapa[sq.alias.lower()] = ("subquery", sq.alias)
    return mapa, calculadas


def _usa(query: str, coluna: str) -> bool:
    return re.search(rf"(?<![\w$]){re.escape(coluna)}(?![\w$])", query, re.I) is not None


def _lista(nomes: list[str]) -> str:
    texto = ", ".join(f"`{n}`" for n in nomes[:_MAX_LISTA])
    return texto + (f" e mais {len(nomes) - _MAX_LISTA}" if len(nomes) > _MAX_LISTA else "")


def _parecidos(coluna: str, nomes: list[str], n: int = 5) -> list[str]:
    unicos = list(dict.fromkeys(x for x in nomes if x and x.lower() != coluna.lower()))
    por_minuscula = {x.lower(): x for x in unicos}
    achados = difflib.get_close_matches(coluna.lower(), list(por_minuscula), n=n, cutoff=0.6)
    return [por_minuscula[a] for a in achados]


def _rotulo(nomes: list[str]) -> str:
    """'a coluna `x`' ou 'as colunas `x` e `y`'."""
    if len(nomes) == 1:
        return f"a coluna `{nomes[0]}`"
    return "as colunas " + ", ".join(f"`{n}`" for n in nomes[:-1]) + f" e `{nomes[-1]}`"


def _maiuscula(texto: str) -> str:
    return texto[:1].upper() + texto[1:]


@dataclass
class _CteDoErro:
    nome: str = ""                          # a CTE onde está a coluna ("" = fora de CTE)
    usada: bool | None = None               # a query final usa essa CTE (direta ou indiretamente)?
    posicao: tuple[int, int] | None = None  # (linha, posição) da primeira coluna não encontrada, no comando


def _cte_do_erro(statement: str, uc: UnresolvedColumn, nomes: list[str]) -> _CteDoErro:
    """Para o "unresolved operator" (que não diz linha nem CTE): acha a CTE pelo que o operador cria (os AS x) e
    pelas colunas não encontradas, se a query final a usa, e a linha da primeira dessas colunas."""
    try:
        import sqlglot
        from sqlglot import exp
        arvore = sqlglot.parse_one(statement, read="spark")
    except Exception:
        return _CteDoErro()
    if arvore is None:
        return _CteDoErro()
    procuradas = {n.lower() for n in nomes}
    criadas = {n.lower() for n in uc.outputs}

    def referencias(no) -> list:
        return [c for c in no.find_all(exp.Column) if c.name.lower() in procuradas]

    with_ = arvore.find(exp.With)
    ctes = list(with_.expressions) if with_ is not None else []
    escolhida, melhor = None, (0, 0)
    for cte in ctes:
        refs = referencias(cte.this)
        if not refs:
            continue
        pontos = (len(criadas & {n.lower() for n in getattr(cte.this, "named_selects", [])}), len(refs))
        if pontos > melhor:
            escolhida, melhor = cte, pontos
    saida = _CteDoErro()
    alvo = escolhida.this if escolhida is not None else arvore
    inicios = [c.this.meta["start"] for c in referencias(alvo)
               if isinstance(c.this, exp.Identifier) and isinstance(c.this.meta.get("start"), int)]
    if inicios:
        inicio = min(inicios)
        saida.posicao = (statement.count("\n", 0, inicio) + 1, inicio - (statement.rfind("\n", 0, inicio) + 1))
    if escolhida is None:
        return saida
    saida.nome = escolhida.alias_or_name
    nomes_ctes = {c.alias_or_name.lower() for c in ctes}

    def usa(no) -> set[str]:
        return {t.name.lower() for t in no.find_all(exp.Table) if not t.db and t.name.lower() in nomes_ctes}

    grafo = {c.alias_or_name.lower(): usa(c.this) for c in ctes}
    principal = arvore.copy()
    principal.find(exp.With).pop()
    usadas, fila = set(), list(usa(principal))
    while fila:
        nome = fila.pop()
        if nome not in usadas:
            usadas.add(nome)
            fila.extend(grafo.get(nome, ()))
    saida.usada = saida.nome.lower() in usadas
    return saida


def diagnose(error: str, statement: str = "", query: str = "", inputs=None, user_catalog=None, orch_inputs=(),
             role: str = "", glue: bool = False) -> ColumnDiagnosis | None:
    """O motivo provável de uma coluna não encontrada, ou None se o erro não é desse tipo.

    statement: o comando que falhou; query: a query inteira (para achar as outras colunas que faltam);
    inputs: as entradas como a sessão as leu (o "inputs" do resultado); user_catalog: user_catalog_columns;
    orch_inputs: as entradas da orquestração; role: o nome da role da sessão; glue: rodou no Glue."""
    uc = parse(error)
    if uc is None:
        return None
    diag = _diagnostico(uc, statement, query, inputs, user_catalog, orch_inputs, role, glue)
    if not uc.operator:
        return diag
    # "unresolved operator": todas as colunas que faltam, a CTE e a linha (o Spark não diz)
    cte = _cte_do_erro(statement, uc, [uc.column] + [o.split(".")[-1] for o in uc.others])
    diag.position, diag.cte = cte.posicao, cte.nome
    for outra in uc.others:
        nome = outra.split(".")[-1]
        if nome.lower() not in {c.lower() for c in [uc.column] + diag.missing_too}:
            diag.missing_too.append(nome)
    if cte.nome and cte.usada is False:
        diag.cause += (f"\n\n**O erro está na CTE `{cte.nome}`, que a query final não usa.** O Glue 4.0 (Spark 3.3) "
                       "confere todas as CTEs mesmo assim e, nesse caso, mostra o trecho inteiro em vez do nome da "
                       "coluna. Se ela não é necessária, apague-a da query: o resto roda sem essas colunas.")
    elif cte.nome:
        diag.cause += f"\n\nO erro está na CTE `{cte.nome}`."
    return diag


def _diagnostico(uc: UnresolvedColumn, statement: str, query: str, inputs, user_catalog, orch_inputs, role: str,
                 glue: bool) -> ColumnDiagnosis:
    texto_query = _SEM_COMENTARIOS.sub(" ", query or statement or "")
    entradas = _entradas(inputs, orch_inputs, user_catalog)
    por_alias = {e.alias.lower(): e for e in entradas}
    por_origem = {e.source.lower(): e for e in entradas if e.source}
    aliases, calculadas = _analisa_query(statement) if statement else ({}, set())

    def entrada_de(qualificador: str) -> _Entrada | None:
        """A entrada da orquestração por trás de um alias da query (None para CTE, subquery ou desconhecido)."""
        q = qualificador.lower()
        if q not in aliases:
            return por_alias.get(q)
        tipo, nome = aliases[q]
        if tipo != "tabela":
            return None
        return por_alias.get(nome.lower()) or por_origem.get(nome.lower())

    # o que o Spark enxerga ali: os qualificadores da lista dele (no Spark 3.3, a lista inteira)
    escopo = list(dict.fromkeys(q for q, _ in uc.candidates if q))
    no_escopo = [e for e in (entrada_de(q) for q in escopo) if e is not None]
    nomes_visiveis = [c for _, c in uc.candidates]
    if uc.operator and nomes_visiveis:
        # sem qualificadores: a entrada é a que tem as colunas que o operador encontrou
        lidas = {n.lower() for n in nomes_visiveis}
        pontos = [(len(lidas & {c.lower() for c in (e.columns or [])}), e) for e in entradas]
        melhor = max((p for p, _ in pontos), default=0)
        no_escopo = [e for p, e in pontos if melhor and p == melhor]
    alvo = entrada_de(uc.qualifier) if uc.qualifier else None
    candidatas = [alvo] if alvo else (no_escopo or entradas)
    if uc.operator:
        # as colunas que o próprio trecho já usa não são sugestão (ex.: ESTADO para SUBESTADO); as outras da tabela, sim
        ja_usadas = {n.lower() for n in nomes_visiveis}
        parecidos = _parecidos(uc.column, [c for e in candidatas for c in (e.columns or []) if c.lower() not in ja_usadas])
    else:
        parecidos = _parecidos(uc.column, nomes_visiveis + [c for e in candidatas for c in (e.columns or [])])
    escopo_txt = _descreve_escopo(uc, escopo, entrada_de, aliases)
    if uc.operator and len(no_escopo) == 1:
        escopo_txt = (f"As outras colunas desse trecho ({_lista(nomes_visiveis)}) o Spark encontrou na entrada "
                      f"{no_escopo[0].nome()}.")
    coluna = uc.column
    todas = list(dict.fromkeys([coluna] + [o.split(".")[-1] for o in uc.others]))
    alvo_txt = _rotulo(todas)

    if uc.struct:
        return ColumnDiagnosis(
            "campo", f"O campo `{coluna}` não existe na coluna `{uc.struct}`",
            f"A coluna `{uc.struct}` é uma struct e não tem o campo `{coluna}`. Confira o nome do campo"
            + (" (nomes parecidos abaixo)." if parecidos else "."), uc.name, parecidos, [], "")

    donos = [e for e in entradas if e.tem(e.columns, coluna)]      # entradas cuja leitura trouxe a coluna

    # 1) é o nome de uma coluna calculada num SELECT da própria query (SELECT a + b AS soma ... WHERE soma > 0)
    calculada = next((n for n in (coluna, uc.qualifier) if n and n.lower() in calculadas), None)
    if calculada is not None and any(e.tem(lista, calculada) for e in entradas for lista in (e.role, e.user)):
        calculada = None                    # é coluna de uma tabela (só não chegou à sessão): os casos abaixo
    if calculada is not None and not (uc.operator and calculada.lower() in {o.lower() for o in uc.outputs}):
        causa = (f"`{calculada}` é o nome de uma coluna calculada num SELECT da própria query, não uma coluna das "
                 "tabelas. Esse nome vale por fora da consulta que o cria (numa subquery ou CTE) e no ORDER BY dela, "
                 "mas não no WHERE da mesma consulta, nem (no Glue 4.0, Spark 3.3) dentro do mesmo SELECT. Repita "
                 "a expressão no lugar do nome ou calcule-a numa subquery/CTE e use o nome por fora.")
        return ColumnDiagnosis("alias_do_select", f"`{calculada}` é um nome criado no SELECT", causa, uc.name, [],
                               [], escopo_txt)

    # 2) o alias antes do ponto não existe nesse trecho (só afirma quando a query foi lida ou a lista é completa)
    if uc.qualifier and alvo is None and (aliases or uc.full_list) and uc.qualifier.lower() not in aliases \
            and not escopo_tem(uc.qualifier, escopo):
        causa = f"`{uc.qualifier}` não é o nome (nem o alias) de nenhuma tabela do FROM/JOIN desse trecho."
        if escopo:
            causa += f" Aqui o Spark enxerga {_lista(escopo)}."
        if donos:
            causa += f" A coluna `{coluna}` existe na entrada {donos[0].nome()}: use o alias que esse trecho dá a ela."
        return ColumnDiagnosis("alias", f"O alias `{uc.qualifier}` não existe nesse trecho da query", causa,
                               uc.name, [], [], escopo_txt)

    # 3) alguma entrada trouxe a coluna: ela só não chega a esse ponto da query
    if donos:
        dono = donos[0]
        if alvo is not None and alvo is not dono:
            causa = (f"`{coluna}` está na entrada {dono.nome()}, não em {alvo.nome()}. "
                     f"Use o alias certo: `{dono.alias}.{coluna}` (ou o alias que a query dá a essa tabela).")
            titulo = f"A coluna `{coluna}` é de outra tabela"
        else:
            onde = ("aqui o Spark só enxerga " + _lista(escopo)) if escopo else "ela não está no FROM desse trecho"
            causa = (f"`{coluna}` existe na entrada {dono.nome()}, mas não chega a esse ponto da query: {onde}. "
                     "Se o FROM aqui é uma CTE ou subquery, inclua a coluna no SELECT dela; se não, faça o JOIN "
                     "com a tabela que tem a coluna.")
            titulo = f"A coluna `{coluna}` não chega a esse ponto da query"
        return ColumnDiagnosis("fora_do_alcance", titulo, causa, uc.name, [], [], escopo_txt)

    # 4) a role vê a coluna no catálogo, mas a leitura não a trouxe (as colunas vieram dos arquivos)
    for e in candidatas:
        if e.tem(e.role, coluna) and e.columns is not None:
            outras = [c for c in e.faltando(e.role) if c.lower() != coluna.lower() and _usa(texto_query, c)]
            if e.read in ("nativa", "dynamicframe", "glue_context"):
                como = {"dynamicframe": " (como DynamicFrame)", "nativa": " (pela leitura nativa)"}.get(e.read, "")
                causa = (f"`{coluna}` está no catálogo de `{e.source}`, mas a leitura da entrada `{e.alias}` não a "
                         f"trouxe. A leitura pelo GlueContext{como} monta as colunas a partir dos arquivos lidos, e "
                         "os arquivos das partições escolhidas não têm essa coluna (é comum quando ela foi criada "
                         "depois na tabela; no Athena, ela viria vazia nessas partições). Confira as partições do "
                         "filtro da entrada.")
            else:
                causa = (f"`{coluna}` está no catálogo de `{e.source}`, mas a leitura da entrada `{e.alias}` não "
                         "a trouxe: os arquivos lidos não têm essa coluna. Confira as partições do filtro da entrada.")
            if outras:
                causa += (f"\n\nA query usa também {_lista(outras)}, que "
                          f"{'falta' if len(outras) == 1 else 'faltam'} pelo mesmo motivo.")
            return ColumnDiagnosis("arquivos", f"A leitura de `{e.alias}` não trouxe {alvo_txt}", causa,
                                   uc.name, [], outras, escopo_txt)

    # 5) o seu usuário vê a coluna no catálogo, a role da sessão não: permissão do Lake Formation
    for e in candidatas:
        if e.tem(e.user, coluna) and not e.tem(e.role, coluna):
            escondidas = e.faltando(e.user)
            outras = [c for c in escondidas if c.lower() != coluna.lower() and _usa(texto_query, c)]
            quem = f"a role da sessão (`{role}`)" if role else "a role da sessão do Glue"
            certeza = "" if e.role is not None else "provavelmente "
            causa = (f"`{coluna}` existe em `{e.source}`: o seu usuário da AWS vê essa coluna no catálogo, mas "
                     f"{quem} não. Com o Lake Formation, o catálogo só mostra a cada um as colunas que ele tem "
                     "permissão de ler; o Athena usa a sua permissão, a sessão usa a da role. Isso "
                     f"{certeza}é falta de permissão da role nessas colunas.\n\n"
                     "**O que fazer:** peça SELECT no Lake Formation para a role nas colunas que ela não vê (a mesma "
                     "permissão do seu usuário, ou a dos jobs que leem essa tabela).")
            if outras:
                usadas = [coluna] + outras
                causa += f"\n\nA query usa {len(usadas)} colunas que a role não vê: {_lista(usadas)}."
            if len(escondidas) > len(outras) + 1:
                causa += f" Ao todo, a role não vê {len(escondidas)} colunas dessa tabela."
            return ColumnDiagnosis("role", f"A role da sessão não enxerga {alvo_txt}", causa, uc.name,
                                   [], outras, escopo_txt)

    # 6) ninguém tem a coluna: nome errado (ou ela é de uma tabela que não está na orquestração)
    conferidas = [e for e in candidatas if e.columns is not None or e.role is not None or e.user is not None]
    varias = len(todas) > 1
    if conferidas:
        usuario_conferido = any(e.user is not None for e in conferidas)
        onde = "no alcance desse trecho" if (alvo is not None or no_escopo) else "da orquestração"
        causa = f"Nenhuma entrada {onde} ({_lista([e.alias for e in conferidas])}) tem {alvo_txt}"
        if usuario_conferido:
            causa += ", nem no catálogo com a sua permissão"
        causa += ". Confira o nome" + (": há nomes parecidos abaixo." if parecidos else ".")
        if glue and not usuario_conferido:
            causa += " " + _TALVEZ_PERMISSAO + _sem_conferir(conferidas)
        if usuario_conferido or not glue:
            titulo = f"{_maiuscula(alvo_txt)} não {'existem' if varias else 'existe'}"
        else:
            titulo = f"A sessão não encontrou {alvo_txt}"
        return ColumnDiagnosis("nao_existe", titulo, causa, uc.name, parecidos, [], escopo_txt)

    # 7) sem as listas: só o que a mensagem diz
    causa = "Confira o nome e de qual tabela ou alias ela vem" + (": há nomes parecidos abaixo." if parecidos
                                                                    else ".")
    if glue:
        causa += " " + _TALVEZ_PERMISSAO + _sem_conferir(candidatas)
    return ColumnDiagnosis("desconhecido", f"O Spark não encontrou {alvo_txt}", causa, uc.name,
                           parecidos, [], escopo_txt)


def _sem_conferir(entradas: list[_Entrada]) -> str:
    erros = sorted({e.user_error for e in entradas if e.user_error})
    if not erros:
        return ""
    return (f" Não deu para conferir as colunas com a sua permissão ({', '.join(erros)}): o seu usuário precisa de "
            "glue:GetTable nessa tabela.")


def escopo_tem(qualificador: str, escopo: list[str]) -> bool:
    return qualificador.lower() in {q.lower() for q in escopo}


def _colunas(n: int) -> str:
    return f"{n} coluna" if n == 1 else f"{n} colunas"


def _descreve_escopo(uc: UnresolvedColumn, escopo: list[str], entrada_de, aliases) -> str:
    """Frase curta com o que o Spark enxerga ali, no lugar da lista enorme da mensagem dele. Só quando a lista é
    a de todas as colunas visíveis (Spark 3.3); do 3.4 em diante ela traz só as 5 mais parecidas."""
    if not uc.candidates or not uc.full_list:
        return ""
    partes = []
    for q in escopo:
        n = sum(1 for qq, _ in uc.candidates if qq == q)
        e = entrada_de(q)
        tipo, _nome = aliases.get(q.lower(), ("", ""))
        if e is not None:
            if e.alias.lower() != q.lower():
                onde = f"entrada {e.nome()}"
            else:
                onde = f"`{e.source}`" if e.source and e.source.lower() != q.lower() else "entrada da orquestração"
            partes.append(f"`{q}` ({onde}, {_colunas(n)})")
        elif tipo in ("cte", "subquery"):
            partes.append(f"`{q}` ({'CTE' if tipo == 'cte' else 'subquery'}, {_colunas(n)})")
        else:
            partes.append(f"`{q}` ({_colunas(n)})")
    sem_qualificador = sum(1 for q, _ in uc.candidates if not q)
    if sem_qualificador:
        partes.append(f"{_colunas(sem_qualificador)} sem tabela (calculadas na própria query)")
    return "Nesse ponto da query, o Spark enxerga: " + "; ".join(partes) + "."
