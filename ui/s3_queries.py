"""Busca de arquivos no S3 pelo nome da tabela e navegação pelos buckets da conta.

- Importação do Athena (render_athena): a query do Athena (nome_da_tabela.sql numa pasta /athena/) vai para a caixa
  "Query do Athena", como se fosse um arquivo aberto da máquina.
- Processo do Spark (process_dialog): a query (pasta /spark/) e os JSON de orquestração (/orchestration/) e de
  metadados (/metadata/), todos com o nome da base final. Com mais de um arquivo do mesmo tipo, você escolhe (dá
  para comparar o conteúdo); e dá para escolher cada um navegando nas pastas.
- Orquestração (render_orchestration): só o JSON de orquestração da base, na janela da orquestração; ele vai para o
  editor da janela, como um arquivo aberto da máquina.

Cada janela guarda o seu estado com um prefixo (ns): "s3q" (Athena), "s3p" (processo do Spark) e "s3o"
(orquestração). O nome da última busca vale como sugestão nas outras (os arquivos têm o nome da base final).
"""
from __future__ import annotations

import time
from collections.abc import Callable

import streamlit as st

import metadata
import orchestration_io
import s3_queries

FOLDER_TTL_S = 300          # o conteúdo de uma pasta já aberta vale 5 minutos (voltar para ela não lista de novo)
KINDS = {"query": "Query do Spark", "orquestracao": "Orquestração (JSON)", "metadados": "Metadados (JSON)"}

Abrir = Callable[[str, str], None]                  # abrir(texto, origem): põe a query na caixa da importação
AoEscolher = Callable[[s3_queries.S3File], None]    # o que fazer com o arquivo clicado na navegação


def _cliente() -> s3_queries.S3Queries:
    """O mesmo cliente enquanto a conta for a mesma (a lista de buckets e as pastas lidas ficam guardadas)."""
    ss = st.session_state
    chave = ss.account.key()
    atual = ss.get("s3_cliente")
    if atual is None or atual[0] != chave:
        atual = (chave, s3_queries.S3Queries(ss.account.client("s3")))     # com as suas credenciais
        ss.s3_cliente = atual
        ss.s3_indices, ss.s3_pastas, ss.s3_textos = {}, {}, {}
    return atual[1]


def _erro_de_conexao(e: Exception) -> str:
    if isinstance(e, s3_queries.S3QueryError):
        return str(e)
    return f"Não consegui falar com o S3 ({type(e).__name__}: {e})."


def _iniciar(ns: str):
    ss = st.session_state
    # a mesma pasta raiz nas duas janelas; ela segue a da configuração, a não ser que tenha sido trocada numa busca
    config = ss.account.queries_root
    if ss.get("s3_raiz_config") != config:
        if ss.get("s3_raiz", ss.get("s3_raiz_config", "")) == ss.get("s3_raiz_config", ""):
            ss.s3_raiz = config
        ss.s3_raiz_config = config
    ss.setdefault("s3_indices", {})
    ss.setdefault("s3_pastas", {})
    ss.setdefault("s3_textos", {})
    for nome, valor in (("achados", None), ("nav", None), ("erro", "")):
        ss.setdefault(f"{ns}_{nome}", valor)


def _formulario(ns: str, rotulo: str, ajuda: str) -> tuple[str, str, bool, bool]:
    ss = st.session_state
    if f"{ns}_nome" not in ss and ss.get("s3_ultimo_nome"):
        ss[f"{ns}_nome"] = ss.s3_ultimo_nome         # o nome da última busca (as três usam o nome da base)
    with st.form(f"{ns}_busca", border=False):
        c1, c2 = st.columns([2, 3])
        nome = c1.text_input(rotulo, key=f"{ns}_nome", placeholder="nome_da_tabela", help=ajuda)
        raiz = c2.text_input("Pasta raiz", ss.s3_raiz, placeholder="s3://meu-bucket/queries/", key=f"{ns}_raiz",
                             help="A busca olha esta pasta e as subpastas. Para guardar a pasta, ⚙️ Configuração › "
                                  "Pasta das queries no S3.")
        b1, b2, _ = st.columns([1, 1.4, 2])
        buscar = b1.form_submit_button("🔎 Buscar", type="primary", width="stretch")
        navegar = b2.form_submit_button("📂 Navegar nas pastas", width="stretch",
                                        help="Abre os buckets da sua conta (começa pela pasta raiz, se houver) para "
                                             "escolher o arquivo.")
    return nome, raiz, buscar, navegar


def _indice(raiz_normal: str) -> s3_queries.QueryIndex:
    """A lista de arquivos da pasta raiz, guardada por alguns minutos (vale para as duas janelas)."""
    ss = st.session_state
    cliente = _cliente()
    indice = ss.s3_indices.get(raiz_normal)
    if indice is None or not indice.fresh():
        with st.spinner(f"Listando os arquivos de `{raiz_normal}`…"):
            indice = cliente.index(raiz_normal)
        ss.s3_indices[raiz_normal] = indice
    return indice


def _rodape(indice: s3_queries.QueryIndex) -> str:
    lido = time.strftime("%H:%M", time.localtime(indice.built_at))
    texto = f"Lista de `{indice.root}` lida às {lido}: {indice.scanned:,} arquivos.".replace(",", ".")
    if not indice.complete:
        texto += (" A pasta tem arquivos demais e a busca parou no tempo limite: escolha uma pasta mais específica "
                  "(a das queries).")
    return texto


def _texto(arquivo: s3_queries.S3File) -> str:
    """O conteúdo do arquivo, lido uma vez só (comparar e usar não leem de novo)."""
    ss = st.session_state
    if arquivo.uri not in ss.s3_textos:
        ss.s3_textos[arquivo.uri] = _cliente().read(arquivo.bucket, arquivo.key)
    return ss.s3_textos[arquivo.uri]


def _rotulo_arquivo(f: s3_queries.S3File) -> str:
    motor = {"athena": "Athena", "spark": "Spark"}.get(f.engine, "sem motor no caminho")
    return f"{f.uri} · {motor} · {f.describe()}"


def _comparar(candidatos: list[s3_queries.S3File], chave: str):
    """Uma aba com o conteúdo de cada arquivo, para comparar antes de escolher."""
    if not st.toggle("Comparar o conteúdo", key=chave):
        return
    abas = st.tabs([f"{i}. {f.key.rsplit('/', 2)[-2] if f.key.count('/') else f.bucket}/{f.name}"
                    for i, f in enumerate(candidatos, start=1)])
    for aba, arquivo in zip(abas, candidatos, strict=True):
        with aba:
            st.caption(arquivo.uri)
            try:
                st.code(_texto(arquivo), language="json" if arquivo.name.lower().endswith(".json") else "sql")
            except Exception as e:
                st.error(_erro_de_conexao(e))


# ------------------------------------------------------------------------------------ navegação pelos buckets
def _abrir_navegacao(ns: str, raiz: str):
    """Começa pela pasta raiz (se ela for de um bucket da conta) ou pela lista de buckets."""
    ss = st.session_state
    bucket, prefixo = s3_queries.split_uri(raiz)
    ss[f"{ns}_nav"] = {"bucket": "", "prefix": ""}
    if not bucket:
        return
    try:
        if bucket in _cliente().buckets():
            ss[f"{ns}_nav"] = {"bucket": bucket, "prefix": prefixo}
        else:
            ss[f"{ns}_erro"] = f"O bucket `{bucket}` não é da conta logada: abri a lista dos buckets da sua conta."
    except Exception as e:
        ss[f"{ns}_erro"] = _erro_de_conexao(e)


def _ir(ns: str, bucket: str, prefixo: str):
    st.session_state[f"{ns}_nav"] = {"bucket": bucket, "prefix": prefixo}


def _fechar_navegacao(ns: str):
    st.session_state[f"{ns}_nav"] = None


def _pasta(bucket: str, prefixo: str, filtro: str, extensoes: tuple[str, ...]) -> s3_queries.Folder:
    """O conteúdo da pasta, guardado por alguns minutos (cada clique na janela redesenha tudo)."""
    ss = st.session_state
    chave = (bucket, prefixo, filtro, extensoes)
    guardada = ss.s3_pastas.get(chave)
    if guardada is None or time.time() - guardada[0] > FOLDER_TTL_S:
        guardada = (time.time(), _cliente().folder(bucket, prefixo, filtro, extensions=extensoes))
        ss.s3_pastas[chave] = guardada
    return guardada[1]


def _navegacao(ns: str, extensoes: tuple[str, ...], ao_escolher: AoEscolher, descricao: str):
    ss = st.session_state
    nav = ss[f"{ns}_nav"]
    bucket, prefixo = nav["bucket"], nav["prefix"]
    with st.container(border=True):
        c1, c2, c3 = st.columns([4, 1, 1], vertical_alignment="center")
        c1.markdown(f"📂 `s3://{bucket}/{prefixo}`" if bucket else "🪣 **Buckets da sua conta**")
        if bucket:
            acima = prefixo.rstrip("/").rpartition("/")[0]
            if prefixo:
                c2.button("⬆️ Acima", key=f"{ns}_acima", width="stretch", on_click=_ir,
                          args=(ns, bucket, acima + "/" if acima else ""))
            else:
                c2.button("🪣 Buckets", key=f"{ns}_buckets", width="stretch", on_click=_ir, args=(ns, "", ""))
        c3.button("✖ Fechar", key=f"{ns}_fechar", width="stretch", on_click=_fechar_navegacao, args=(ns,))
        st.caption(descricao)
        try:
            if not bucket:
                for nome in _cliente().buckets():
                    st.button(f"🪣 {nome}", key=f"{ns}_b_{nome}", type="tertiary", on_click=_ir, args=(ns, nome, ""))
                return
            filtro = st.text_input("Filtrar pelo começo do nome", key=f"{ns}_filtro_{bucket}/{prefixo}",
                                   placeholder="ex.: tbl_vendas").strip()
            pasta = _pasta(bucket, prefixo, filtro, extensoes)
        except Exception as e:
            st.error(_erro_de_conexao(e))
            return
        if not pasta.folders and not pasta.files:
            st.caption("Nada aqui" + (f" começando por `{filtro}`." if filtro else "."))
        for nome in pasta.folders:
            st.button(f"📁 {nome}/", key=f"{ns}_p_{bucket}/{prefixo}{nome}", type="tertiary", on_click=_ir,
                      args=(ns, bucket, f"{prefixo}{nome}/"))
        for arquivo in pasta.files:
            motor = {"athena": " · Athena", "spark": " · Spark"}.get(arquivo.engine, "")
            st.button(f"📄 {arquivo.name} · {arquivo.describe()}{motor}", key=f"{ns}_a_{arquivo.uri}",
                      type="tertiary", on_click=ao_escolher, args=(arquivo,))
        notas = []
        if pasta.others:
            notas.append(f"{pasta.others} outro(s) arquivo(s) aqui, de outros tipos")
        if pasta.truncated:
            notas.append("a pasta tem mais itens do que os mostrados: use o filtro")
        if notas:
            st.caption("; ".join(notas).capitalize() + ".")


# ------------------------------------------------------------------------------------ importação do Athena
def render_athena(abrir: Abrir):
    ss = st.session_state
    _iniciar("s3q")
    with st.container(border=True):
        st.markdown("**☁️ Buscar no S3**")
        tabela, raiz, buscar, navegar = _formulario(
            "s3q", "Tabela", "A query da tabela fica salva como nome_da_tabela.sql. Uma query numa pasta /athena/ é "
                             "do Athena.")
        if buscar:
            _buscar_athena(tabela, raiz, abrir)
        if navegar:
            _abrir_navegacao("s3q", raiz)
        if ss.s3q_erro:
            st.error(ss.s3q_erro)
            ss.s3q_erro = ""
        _achados_athena(abrir)
        if ss.s3q_nav is not None:
            _navegacao("s3q", s3_queries.QUERY_EXTENSIONS, lambda f: _abrir_do_athena(f, abrir, fechar=True),
                       "Clique na query para abrir.")


def _buscar_athena(tabela: str, raiz: str, abrir: Abrir):
    ss = st.session_state
    ss.s3q_achados = None
    nome = s3_queries.table_file_name(tabela)
    raiz_normal = s3_queries.normalize_uri(raiz)
    if not nome:
        st.warning("Digite o nome da tabela.")
        return
    if not raiz_normal:
        st.warning("Informe a pasta raiz, como `s3://meu-bucket/queries/`.")
        return
    ss.s3_raiz, ss.s3_ultimo_nome = raiz.strip(), s3_queries.base_name(tabela)
    try:
        indice = _indice(raiz_normal)
    except Exception as e:
        st.error(_erro_de_conexao(e))
        return
    achados = indice.find(nome)
    ss.s3q_achados = {"nome": nome, "raiz": raiz_normal, "arquivos": achados, "indice": indice}
    do_athena = [f for f in achados if f.athena]
    if len(do_athena) == 1:                 # uma query do Athena com esse nome: vai direto para a caixa
        _abrir_do_athena(do_athena[0], abrir)


def _achados_athena(abrir: Abrir):
    ss = st.session_state
    a = ss.s3q_achados
    if a is None:
        return
    arquivos = a["arquivos"]
    if not arquivos:
        st.warning(f"Nenhum `{a['nome']}` em `{a['raiz']}` nem nas subpastas.")
        st.caption(_rodape(a["indice"]) + " Dá para procurar em **📂 Navegar nas pastas**.")
        return
    do_athena = [f for f in arquivos if f.athena]
    if len(do_athena) == 1 and len(arquivos) == 1:
        st.caption(_rodape(a["indice"]))
        return                              # abriu direto: a origem aparece em cima da caixa da query
    if not do_athena:
        st.warning(f"Achei `{a['nome']}`, mas fora de uma pasta `/athena/`: confira se é mesmo uma query do Athena.")
    elif len(do_athena) > 1:
        st.info(f"Achei {len(do_athena)} queries do Athena com esse nome: escolha qual abrir.")
    escolha = st.radio("Arquivos encontrados", arquivos, key=f"s3q_escolha_{a['raiz']}_{a['nome']}",
                       format_func=_rotulo_arquivo)
    st.button("📄 Abrir esta query", key="s3q_abrir_escolha", on_click=_abrir_do_athena, args=(escolha, abrir))
    st.caption(_rodape(a["indice"]))


def _abrir_do_athena(arquivo: s3_queries.S3File, abrir: Abrir, fechar: bool = False):
    """Lê o arquivo e põe na caixa da query (também é chamada como callback de botão)."""
    ss = st.session_state
    try:
        texto = _texto(arquivo)
    except Exception as e:
        ss.s3q_erro = _erro_de_conexao(e)
        return
    abrir(texto, arquivo.uri if arquivo.athena else
          f"{arquivo.uri} (fora de uma pasta /athena/: confira se é mesmo uma query do Athena)")
    if fechar:
        _fechar_navegacao("s3q")


# ------------------------------------------------------------------------------------ processo do Spark
@st.dialog("Buscar no S3: query do Spark, orquestração e metadados", width="large")
def process_dialog():
    process_body()


def process_body():
    """Conteúdo da janela (separado dela para poder ser testado sem abrir a janela)."""
    ss = st.session_state
    _iniciar("s3p")
    ss.setdefault("s3p_escolhas", dict.fromkeys(KINDS))
    ss.setdefault("s3p_alvo", "query")
    ss.setdefault("s3p_versao", 0)
    st.caption("A query do Spark (pasta `/spark/`) e os JSON de orquestração (`/orchestration/`) e de metadados "
               "(`/metadata/`) têm o nome da base final. Busque pelo nome ou escolha cada arquivo navegando nas "
               "pastas.")
    nome, raiz, buscar, navegar = _formulario(
        "s3p", "Base final", "O nome da tabela final, salva no Athena: os três arquivos têm esse nome.")
    if buscar:
        _buscar_processo(nome, raiz)
    if navegar:
        _abrir_navegacao("s3p", raiz)
    if ss.s3p_erro:
        st.error(ss.s3p_erro)
        ss.s3p_erro = ""
    a = ss.s3p_achados
    if a is not None:
        st.caption(_rodape(a["indice"]))
    for tipo, titulo in KINDS.items():
        _escolha_do_tipo(tipo, titulo)
    if ss.s3p_nav is not None:
        st.radio("O arquivo clicado vai para", list(KINDS), format_func=KINDS.get, key="s3p_alvo", horizontal=True)
        _navegacao("s3p", s3_queries.PROCESS_EXTENSIONS, _escolher_na_navegacao,
                   "Clique no arquivo para usá-lo como o tipo escolhido acima.")
    escolhidos = {t: f for t, f in ss.s3p_escolhas.items() if f is not None}
    if st.button("✅ Usar os arquivos escolhidos", type="primary", disabled=not escolhidos):
        if _aplicar(escolhidos):
            st.rerun()


def _buscar_processo(nome: str, raiz: str):
    ss = st.session_state
    base = s3_queries.base_name(nome)
    raiz_normal = s3_queries.normalize_uri(raiz)
    if not base:
        st.warning("Digite o nome da base final.")
        return
    if not raiz_normal:
        st.warning("Informe a pasta raiz, como `s3://meu-bucket/queries/`.")
        return
    ss.s3_raiz, ss.s3_ultimo_nome = raiz.strip(), base
    try:
        indice = _indice(raiz_normal)
    except Exception as e:
        st.error(_erro_de_conexao(e))
        return
    arquivos = indice.find_process(base)
    ss.s3p_achados = {"nome": base, "raiz": raiz_normal, "indice": indice, "arquivos": arquivos}
    ss.s3p_escolhas = {tipo: (lista[0] if lista else None) for tipo, lista in arquivos.items()}
    ss.s3p_versao += 1


def _escolha_do_tipo(tipo: str, titulo: str):
    ss = st.session_state
    a = ss.s3p_achados
    candidatos = list(a["arquivos"][tipo]) if a is not None else []
    atual = ss.s3p_escolhas.get(tipo)
    if atual is not None and atual not in candidatos:
        candidatos.append(atual)             # escolhido na navegação
    st.markdown(f"**{titulo}**")
    if not candidatos:
        st.caption("Não encontrado" + (f" (`{a['nome']}`)" if a else "") + ": escolha em **📂 Navegar nas pastas**.")
        return
    if len(candidatos) == 1:
        st.caption(f"✔ {_rotulo_arquivo(candidatos[0])}")
        return
    st.caption(f"Achei {len(candidatos)} arquivos: escolha o que vale para esta execução (dá para comparar).")
    escolha = st.radio(titulo, candidatos, index=candidatos.index(atual) if atual in candidatos else 0,
                       format_func=_rotulo_arquivo, key=f"s3p_r_{tipo}_{ss.s3p_versao}", label_visibility="collapsed")
    ss.s3p_escolhas[tipo] = escolha
    _comparar(candidatos, f"s3p_comparar_{tipo}_{ss.s3p_versao}")


def _escolher_na_navegacao(arquivo: s3_queries.S3File):
    """Callback do clique num arquivo da navegação: vale para o tipo escolhido; depois, o próximo tipo vazio."""
    ss = st.session_state
    ss.s3p_escolhas[ss.s3p_alvo] = arquivo
    ss.s3p_versao += 1
    vazio = next((t for t in KINDS if ss.s3p_escolhas.get(t) is None), None)
    if vazio is not None:
        ss.s3p_alvo = vazio


def _aplicar(escolhidos: dict[str, s3_queries.S3File]) -> bool:
    """Lê os arquivos e usa: a query vai para o editor, a orquestração para o Passo 1 e as descrições das colunas
    para os metadados do resultado. Devolve False se nada pôde ser lido (a janela continua aberta)."""
    from ui.state import apply_orchestration
    ss = st.session_state
    textos = {}
    for tipo, arquivo in escolhidos.items():
        try:
            textos[tipo] = (arquivo, _texto(arquivo))
        except Exception as e:
            st.error(_erro_de_conexao(e))
            return False
    feito, pendente = [], []
    if "query" in textos:
        arquivo, texto = textos["query"]
        ss.sql = texto
        ss.editor_version += 1
        feito.append(f"a query foi para o editor (`{arquivo.uri}`)")
    if "orquestracao" in textos:
        arquivo, texto = textos["orquestracao"]
        try:
            if not orchestration_io.is_app_format(texto):
                raise orchestration_io.OrchestrationFormatError("formato de outro sistema")
            apply_orchestration(orchestration_io.from_json(texto), keep_conf=True)
            ss.open_orch_after = True        # abre para revisar, como na importação do Athena
            feito.append(f"a orquestração foi aplicada no Passo 1 (`{arquivo.uri}`)")
        except orchestration_io.OrchestrationFormatError as e:
            ss.saf_orquestracao = (arquivo.uri, texto)
            motivo = ("está num formato que o app ainda não converte (o do SAF)"
                      if str(e) == "formato de outro sistema" else f"tem um problema ({e})")
            pendente.append(f"a orquestração `{arquivo.uri}` {motivo}: as entradas do Passo 1 não mudaram. O "
                            "conteúdo está guardado para quando o formato for suportado.")
    if "metadados" in textos:
        arquivo, texto = textos["metadados"]
        try:
            descricoes = metadata.descriptions_from_json(texto)
        except ValueError:
            descricoes = {}
        ss.saf_metadados = (arquivo.uri, texto)
        if descricoes:
            ss.metadata_descricoes = descricoes
            feito.append(f"{len(descricoes)} descrição(ões) de coluna vão para os metadados do resultado "
                         f"(`{arquivo.uri}`)")
        else:
            pendente.append(f"não achei descrições de colunas em `{arquivo.uri}`: o formato do SAF ainda não é "
                            "lido pelo app.")
    ss.s3p_resumo = {"feito": feito, "pendente": pendente}
    return True


# ------------------------------------------------------------------------------------ orquestração
def render_orchestration(abrir: Abrir):
    """Na janela da orquestração: o JSON de orquestração da base (nome_da_base.json numa pasta /orchestration/),
    pela busca ou navegando. abrir(texto, origem) põe o JSON no editor da janela."""
    ss = st.session_state
    _iniciar("s3o")
    with st.container(border=True):
        st.markdown("**☁️ Buscar no S3**")
        nome, raiz, buscar, navegar = _formulario(
            "s3o", "Base final", "A orquestração fica salva como nome_da_base.json numa pasta /orchestration/.")
        if buscar:
            _buscar_orquestracao(nome, raiz, abrir)
        if navegar:
            _abrir_navegacao("s3o", raiz)
        if ss.s3o_erro:
            st.error(ss.s3o_erro)
            ss.s3o_erro = ""
        _achados_orquestracao(abrir)
        if ss.s3o_nav is not None:
            _navegacao("s3o", (".json",), lambda f: _abrir_orquestracao(f, abrir, fechar=True),
                       "Clique no JSON para abrir no editor.")


def _buscar_orquestracao(nome: str, raiz: str, abrir: Abrir):
    ss = st.session_state
    ss.s3o_achados = None
    base = s3_queries.base_name(nome)
    raiz_normal = s3_queries.normalize_uri(raiz)
    if not base:
        st.warning("Digite o nome da base final.")
        return
    if not raiz_normal:
        st.warning("Informe a pasta raiz, como `s3://meu-bucket/queries/`.")
        return
    ss.s3_raiz, ss.s3_ultimo_nome = raiz.strip(), base
    try:
        indice = _indice(raiz_normal)
    except Exception as e:
        st.error(_erro_de_conexao(e))
        return
    achados = indice.find_process(base)["orquestracao"]
    # o mesmo nome fora de uma pasta de orquestração (e que não é o JSON de metadados): pode ser ela, numa pasta
    # com outro nome
    alvo = f"{base.lower()}.json"
    fora = sorted((f for f in indice.files if f.name.lower() == alvo and not f.kind),
                  key=lambda f: (-(f.modified.timestamp() if f.modified else 0), f.key))
    ss.s3o_achados = {"nome": base, "raiz": raiz_normal, "arquivos": achados + fora, "indice": indice}
    if len(achados) == 1 and not fora:      # uma orquestração com esse nome: vai direto para o editor
        _abrir_orquestracao(achados[0], abrir)


def _achados_orquestracao(abrir: Abrir):
    ss = st.session_state
    a = ss.s3o_achados
    if a is None:
        return
    arquivos = a["arquivos"]
    if not arquivos:
        st.warning(f"Nenhum `{a['nome']}.json` numa pasta `/orchestration/` em `{a['raiz']}` nem nas subpastas.")
        st.caption(_rodape(a["indice"]) + " Dá para procurar em **📂 Navegar nas pastas**.")
        return
    na_pasta = [f for f in arquivos if f.kind == "orquestracao"]
    if len(arquivos) == 1 and na_pasta:
        st.caption(_rodape(a["indice"]))
        return                              # abriu direto: a origem aparece em cima do editor
    if not na_pasta:
        st.warning(f"Achei `{a['nome']}.json`, mas fora de uma pasta `/orchestration/`: confira se é mesmo a "
                   "orquestração.")
    elif len(na_pasta) > 1:
        st.info(f"Achei {len(na_pasta)} orquestrações com esse nome (por exemplo, uma da execução no Athena e outra "
                "no Spark): escolha qual abrir.")
    chave = f"{a['raiz']}_{a['nome']}"
    escolha = st.radio("Arquivos encontrados", arquivos, key=f"s3o_escolha_{chave}", format_func=_rotulo_arquivo)
    _comparar(arquivos, f"s3o_comparar_{chave}")
    st.button("📄 Abrir este JSON", key="s3o_abrir_escolha", on_click=_abrir_orquestracao, args=(escolha, abrir))
    st.caption(_rodape(a["indice"]))


def _abrir_orquestracao(arquivo: s3_queries.S3File, abrir: Abrir, fechar: bool = False):
    """Lê o JSON e põe no editor da janela (também é chamada como callback de botão)."""
    ss = st.session_state
    try:
        texto = _texto(arquivo)
    except Exception as e:
        ss.s3o_erro = _erro_de_conexao(e)
        return
    abrir(texto, arquivo.uri if arquivo.kind == "orquestracao" else
          f"{arquivo.uri} (fora de uma pasta /orchestration/: confira se é mesmo a orquestração)")
    if fechar:
        _fechar_navegacao("s3o")


def render_summary():
    """No Passo 2: o que foi carregado da última busca no S3 (até ser dispensado)."""
    ss = st.session_state
    resumo = ss.get("s3p_resumo")
    if not resumo:
        return
    with st.container(border=True):
        c1, c2 = st.columns([8, 1], vertical_alignment="center")
        c1.markdown("**☁️ Carregado do S3**")
        if c2.button("✖", key="s3p_resumo_fechar", help="Dispensar este aviso"):
            ss.s3p_resumo = None
            st.rerun()
        for item in resumo["feito"]:
            st.caption("✔ " + item[0].upper() + item[1:])
        for item in resumo["pendente"]:
            st.caption("⚠️ " + item[0].upper() + item[1:])
