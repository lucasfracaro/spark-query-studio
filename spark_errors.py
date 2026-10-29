"""
Resume os erros do Spark para a tela.

O erro que volta do Spark costuma ter dezenas de linhas: o plano lógico da query e a pilha de
chamadas do Java. Aqui sai só o que interessa (a causa), um título em português e, quando dá
para saber, o que fazer. A mensagem completa continua disponível na tela.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class ErrorSummary:
    title: str              # uma linha
    detail: str | None      # a mensagem do Spark, sem o plano e sem a pilha do Java
    hint: str | None        # o que fazer
    error_class: str | None = None


_ANSI = ("Com o modo ANSI (padrão no Glue 6.0), contas e conversões inválidas viram erro em vez de NULL. "
         "Use try_divide, try_cast ou trate o caso na query; ou teste na versão do Glue da produção.")
_MEMORIA = ("Faltou memória (ou disco) nos workers. Use um worker maior (G.2X, G.4X…), mais workers, "
            "ou filtre mais as entradas na orquestração.")
_S3_NEGADO = ("A role da sessão não conseguiu ler esse caminho no S3. Se os dados são de outra conta e o Athena "
              "lê pelo Lake Formation, ligue **Ler as tabelas pelo Lake Formation** em ⚙️ Configuração. "
              "Se não, a role precisa de permissão no bucket (a mesma dos jobs de produção resolve).")
_LF_NEGADO = ("Falta permissão no Lake Formation para a role da sessão: ela precisa de SELECT nas tabelas e de "
              "`lakeformation:GetDataAccess` no IAM. No Athena vale o seu usuário; aqui vale a role.")

# classe de erro do Spark -> (título, dica)
_CLASSES = {
    "UNRESOLVED_COLUMN": ("Coluna não encontrada", "Confira o nome e de qual tabela ou alias ela vem. "
                          "O Spark sugere nomes parecidos no detalhe abaixo."),
    "UNRESOLVED_ROUTINE": ("Função não encontrada", "Algumas funções do Athena têm outro nome no Spark "
                           "(a importação do Athena traduz a maioria)."),
    "PARSE_SYNTAX_ERROR": ("Erro de sintaxe no SQL", "Confira o trecho marcado com ^ abaixo. O Spark SQL difere do "
                           "Athena em alguns pontos; a importação do Athena faz essa tradução."),
    "TABLE_OR_VIEW_NOT_FOUND": ("Tabela ou view não encontrada", "Ela precisa estar na orquestração "
                                "(Passo 1 › Entradas de dados), usada pelo alias."),
    "SCHEMA_NOT_FOUND": ("Database não encontrado", "Confira o nome do database na orquestração e o database padrão."),
    "DIVIDE_BY_ZERO": ("Divisão por zero", _ANSI),
    "CAST_INVALID_INPUT": ("Conversão de tipo inválida", _ANSI),
    "ARITHMETIC_OVERFLOW": ("Número grande demais para o tipo", _ANSI),
    "NUMERIC_VALUE_OUT_OF_RANGE": ("Número fora do intervalo do tipo", _ANSI),
    "INVALID_ARRAY_INDEX": ("Posição inexistente no array", _ANSI),
    "PATH_NOT_FOUND": ("Caminho não encontrado", "Confira o caminho no S3 e se as partições do filtro existem."),
    "UNABLE_TO_INFER_SCHEMA": ("Não há arquivos para ler", "O caminho está vazio ou o filtro não pegou nenhuma "
                               "partição. Confira o caminho, o formato e o filtro."),
    "AMBIGUOUS_REFERENCE": ("Coluna ambígua", "A coluna existe em mais de uma tabela do JOIN: use alias.coluna."),
}

# padrões sem classe de erro (ou mais específicos que ela): (condição, título, dica)
def _tem(*padroes: str):
    regs = [re.compile(p, re.I) for p in padroes]
    return lambda texto: all(r.search(texto) for r in regs)


_NEGADO = r"AccessDenied|Access Denied|403 Forbidden|Status Code: 403|not authorized"
_PADROES = [
    (_tem(r"não pôde ser usado para escolher as partições"), "O filtro não serve para escolher partições",
     "Deixe no filtro da entrada só colunas de partição (ex.: dt = '2026-10-28'); as outras condições vão "
     "no WHERE da query."),
    (_tem(r"Insufficient Lake Formation permission|lakeformation:GetDataAccess"),
     "Sem permissão no Lake Formation", _LF_NEGADO),
    (_tem(r"not authorized to perform: glue:|glue:Get\w+"),
     "Sem permissão no Glue Data Catalog",
     "A role da sessão precisa ler o catálogo (glue:GetTable, glue:GetPartitions…) e, se a tabela é do "
     "Lake Formation, de DESCRIBE e SELECT nele."),
    (_tem(_NEGADO, r"\bS3\b|s3a?://"), "Acesso negado ao S3", _S3_NEGADO),
    (_tem(_NEGADO), "Acesso negado",
     "A role da sessão não tem permissão para essa operação. Confira a role em ⚙️ Configuração."),
    (_tem(r"OutOfMemoryError|Container killed|exceeding memory limits|ExecutorLostFailure|"
          r"No space left on device"),
     "Faltou memória nos workers", _MEMORIA),
    (_tem(r"No module named '?awsglue"),
     "GlueContext indisponível", "A leitura pelo Lake Formation no Glue 4.0 usa o GlueContext, que só existe "
                                  "no Glue. No Spark local, desligue essa opção."),
    (_tem(r"Path does not exist|FileNotFoundException"),
     "Arquivo ou caminho não encontrado", "Confira o caminho. Se os arquivos foram trocados durante a leitura "
                                          "(ex.: um ETL regravou a tabela), rode de novo."),
]

_JAVA_STACK = re.compile(r"^\s*(at |\.\.\. \d+ more)")
_PLAN_LINE = re.compile(r"^\s*('|\+-|:-|:\s+[+:]-|\|)")
_CLASS_RE = re.compile(r"\[([A-Z][A-Z0-9_]+)(?:\.[A-Z0-9_]+)*\]")


def _sem_pilha_e_plano(texto: str) -> list[str]:
    linhas = []
    for linha in texto.splitlines():
        if _JAVA_STACK.match(linha):
            continue
        if _PLAN_LINE.match(linha) and linhas:        # o plano lógico vem depois da mensagem
            break
        linhas.append(linha.rstrip())
    return [linha for linha in linhas if linha.strip()]


def _bloco_sql(texto: str) -> str | None:
    """O trecho "== SQL ==" de um erro de sintaxe, com a marcação ^^^ de onde está o problema."""
    i = texto.find("== SQL ==")
    if i == -1:
        return None
    bloco = []
    for linha in texto[i:].splitlines()[:30]:
        if _JAVA_STACK.match(linha) or (bloco and linha.startswith(("Traceback", "\tat"))):
            break
        bloco.append(linha.rstrip())
    return "\n".join(bloco).strip() or None


def summarize(error: str, traceback: str = "") -> ErrorSummary:
    error = (error or "").strip()
    tudo = f"{error}\n{traceback or ''}"

    # Py4JJavaError: a mensagem útil é a do Java (depois de "\n: ") e, se houver, a causa raiz
    principal = error
    if "An error occurred while calling" in error and "\n: " in error:
        principal = error.split("\n: ", 1)[1]
    causas = re.findall(r"^Caused by: (.+)$", tudo, re.M)

    linhas = _sem_pilha_e_plano(principal)
    detalhe = "\n".join(linhas[:12]) if linhas else error
    sql = _bloco_sql(tudo)
    if sql and sql not in detalhe:
        detalhe = detalhe.split("== SQL ==")[0].rstrip() + "\n\n" + sql
    if causas and causas[-1] not in detalhe:
        detalhe += f"\n\nCausa: {causas[-1].strip()}"

    m = _CLASS_RE.search(tudo)
    classe = m.group(1) if m else None
    for condicao, titulo, dica in _PADROES:
        if condicao(tudo):
            return ErrorSummary(titulo, detalhe, dica, classe)
    if classe in _CLASSES:
        titulo, dica = _CLASSES[classe]
        return ErrorSummary(titulo, detalhe, dica, classe)
    if "ParseException" in tudo:
        titulo, dica = _CLASSES["PARSE_SYNTAX_ERROR"]
        return ErrorSummary(titulo, detalhe, dica, classe)

    primeira = (linhas[0] if linhas else error).strip()
    primeira = re.sub(r"^(\w+\.)*(\w+(Exception|Error)): ", "", primeira)
    titulo = primeira if len(primeira) <= 160 else primeira[:157] + "…"
    return ErrorSummary(titulo or "Erro na execução", detalhe, None, classe)
