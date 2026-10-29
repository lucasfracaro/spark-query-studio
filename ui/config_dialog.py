"""Janela ⚙️ Configuração: mostra o que veio do configuracao.toml, deixa corrigir e salvar no arquivo."""
from __future__ import annotations

import streamlit as st

import settings_store
from engines import (
    DEFAULT_GLUE_VERSION,
    DEFAULT_WORKER_TYPE,
    GLUE_VERSION_NOTES,
    GLUE_VERSIONS,
    GLUE_WORKER_TYPES,
    AwsAccount,
    GlueSessionEngine,
    InstanceConfig,
    glue_version_tuple,
)
from ui.state import apply_config

AUTH_LABELS = {"profile": "Perfil do AWS CLI (~/.aws)", "keys": "Access key / Secret key",
               "default": "Cadeia padrão (env vars, SSO, role da instância)"}


def aws_profiles() -> list[str]:
    try:
        import boto3
        return sorted(boto3.session.Session().available_profiles)
    except Exception:
        return []


def file_status(lc: settings_store.LoadedConfig) -> str:
    """Uma linha dizendo de onde veio a configuração (para a barra lateral)."""
    nome = lc.path.name
    if lc.source == "arquivo":
        if lc.error:
            return f"⚠️ `{nome}` tem um erro de digitação"
        if lc.problems:
            return f"⚠️ `{nome}`: {len(lc.problems)} ponto(s) para corrigir"
        return f"📄 Configuração lida de `{nome}`"
    if lc.source == "legado":
        return "📄 Configuração do formato antigo (salve para criar o `configuracao.toml`)"
    return f"📄 Sem `{nome}`: usando os valores padrão"


def lake_formation_help(versao: str) -> str:
    if glue_version_tuple(versao) >= (5, 0):
        texto = ("A sessão é criada com o *Full Table Access* do Lake Formation: as tabelas são lidas com as "
                 "credenciais que ele fornece, como no Athena. O administrador do Lake Formation precisa permitir "
                 "*full table access* para engines externas.")
    else:
        texto = ("No Glue 4.0, as entradas `catalog` são lidas pelo GlueContext (`create_dynamic_frame.from_catalog`, "
                 "com o filtro como `push_down_predicate`), como nos jobs do Glue: é a leitura que usa o Lake "
                 "Formation nessa versão. A query precisa usar os aliases da orquestração.")
    return (texto + " A role da sessão precisa de SELECT nas tabelas no Lake Formation e de "
            "`lakeformation:GetDataAccess` no IAM. Vale para sessões novas.")


def open_config_dialog():
    st.session_state.config_opened = st.session_state.get("config_opened", 0) + 1
    config_dialog()


@st.dialog("⚙️ Configuração", width="large")
def config_dialog():
    config_body()


def config_body():
    """Conteúdo da janela (separado dela para poder ser testado sem abrir a janela)."""
    ss = st.session_state
    lc: settings_store.LoadedConfig = ss.config_loaded
    if lc.source == "arquivo":
        st.caption(f"Arquivo: `{lc.path}`. **Aplicar** usa os valores só enquanto o app estiver aberto; "
                   "**Salvar no arquivo** grava para as próximas vezes.")
    elif lc.source == "legado":
        st.caption(f"A configuração veio do formato antigo. **Salvar no arquivo** cria `{lc.path}`.")
    else:
        st.caption(f"Ainda não existe `{lc.path.name}`. Preencha e clique em **Salvar no arquivo** para criá-lo em "
                   f"`{lc.path.parent}` (ele fica fora do git). O modelo comentado está em "
                   f"`{settings_store.EXAMPLE_FILE.name}`.")
    if lc.error:
        st.error(f"{lc.error} Corrija na aba **Arquivo (texto)**: enquanto isso, valem os valores padrão.")
    t_form, t_texto = st.tabs(["📝 Formulário", "📄 Arquivo (texto)"])
    with t_form:
        _formulario(lc)
    with t_texto:
        _texto(lc)


def _formulario(lc: settings_store.LoadedConfig):
    ss = st.session_state
    acc: AwsAccount = ss.account
    inst: InstanceConfig = ss.instance
    v = ss.get("config_opened", 0)        # widgets novos a cada vez que a janela abre

    st.markdown("**Conta AWS**")
    c1, c2 = st.columns(2)
    region = c1.text_input("Região", acc.region, help="Ex: us-east-1, sa-east-1")
    modos = list(AUTH_LABELS)
    auth_mode = c2.selectbox("Autenticação", modos, index=modos.index(acc.auth_mode) if acc.auth_mode in modos else 0,
                             format_func=AUTH_LABELS.get)
    profile, ak, sk, tok = acc.profile, acc.access_key, acc.secret_key, acc.session_token
    gravar_chaves = False
    if auth_mode == "profile":
        profile = st.text_input("Nome do perfil", acc.profile, placeholder="default")
    elif auth_mode == "keys":
        k1, k2 = st.columns(2)
        ak = k1.text_input("Access key ID", acc.access_key)
        sk = k2.text_input("Secret access key", acc.secret_key, type="password")
        tok = st.text_input("Session token (opcional)", acc.session_token, type="password")
        gravar_chaves = st.checkbox("Guardar as chaves no arquivo", lc.has_secrets,
                                    help="Ficam em texto puro no arquivo, que só o seu usuário consegue ler.")
        st.caption("⚠️ Chaves em arquivo são arriscadas: prefira um perfil do AWS CLI." if gravar_chaves else
                   "🔒 As chaves ficam só na memória enquanto o app estiver aberto.")
    role = st.text_input("ARN da IAM Role do Glue", acc.glue_role_arn,
                         placeholder="arn:aws:iam::123456789012:role/GlueInteractiveSessionRole",
                         help="Role que o Glue assume para rodar a sessão Spark (precisa ler o S3 e o Glue Catalog). "
                              "Se os jobs de produção já leem as tabelas, a role deles é a escolha mais segura.")
    temp = st.text_input("Diretório temporário no S3 (opcional)", acc.temp_dir, placeholder="s3://meu-bucket/glue-tmp/")

    st.markdown("**Sessão do Glue**")
    c1, c2 = st.columns(2)
    tipos = list(GLUE_WORKER_TYPES)
    worker = c1.selectbox("Tipo de worker", tipos,
                          index=tipos.index(inst.worker_type if inst.worker_type in tipos else DEFAULT_WORKER_TYPE),
                          format_func=lambda w: f"{w} — {GLUE_WORKER_TYPES[w].descricao()}", key=f"cfg_worker_{v}")
    workers = c2.number_input("Número de workers", settings_store.MIN_WORKERS, settings_store.MAX_WORKERS,
                              int(inst.num_workers), key=f"cfg_workers_{v}")
    c3, c4 = st.columns(2)
    versoes = list(GLUE_VERSIONS)
    versao = c3.selectbox("Versão do Glue", versoes,
                          index=versoes.index(inst.glue_version if inst.glue_version in versoes else DEFAULT_GLUE_VERSION),
                          format_func=lambda x: f"{x} — {GLUE_VERSIONS[x]}", key=f"cfg_versao_{v}")
    ocioso = c4.number_input("Encerrar sozinha após (min sem uso)", settings_store.MIN_IDLE, settings_store.MAX_IDLE,
                             int(inst.idle_timeout_min), key=f"cfg_ocioso_{v}")
    if versao in GLUE_VERSION_NOTES:
        st.caption("⚠️ " + GLUE_VERSION_NOTES[versao])
    lf = st.checkbox("Ler as tabelas pelo Lake Formation (dados no S3 de outra conta)", inst.lake_formation,
                     key=f"cfg_lf_{v}", help="Use quando o Athena lê as tabelas mas a sessão do Glue recebe acesso negado "
                                        "no S3.")
    if lf:
        st.caption(lake_formation_help(versao))
    iniciar = st.checkbox("Iniciar a sessão assim que o app abrir", inst.start_on_open, key=f"cfg_iniciar_{v}",
                          help="A sessão sobe em segundo plano enquanto você monta a query. Ela cobra desde que fica "
                               "pronta, mesmo sem uso, até o tempo sem uso acabar.")

    novo_acc = AwsAccount(region=region.strip(), auth_mode=auth_mode, profile=profile.strip(),
                          access_key=ak.strip(), secret_key=sk.strip(), session_token=tok.strip(),
                          glue_role_arn=role.strip(), temp_dir=temp.strip())
    nova_inst = InstanceConfig(worker_type=worker, num_workers=int(workers), glue_version=versao,
                               idle_timeout_min=int(ocioso), extra_args=dict(inst.extra_args), tags=dict(inst.tags),
                               start_on_open=iniciar, lake_formation=lf)
    perfis = aws_profiles() if auth_mode == "profile" and novo_acc.profile else None
    problemas = settings_store.check(novo_acc, nova_inst, profiles=perfis)
    for p in problemas:
        st.warning(p)

    b1, b2, b3 = st.columns(3)
    if b1.button("🔌 Testar conexão", width="stretch", disabled=bool(problemas)):
        try:
            ident = GlueSessionEngine(novo_acc, nova_inst).whoami()
            st.success(f"Conectado como `{ident['Arn']}` (conta {ident['Account']})")
        except Exception as e:
            st.error(f"Falha: {e}")
    if b2.button("✅ Aplicar", width="stretch", disabled=bool(problemas),
                 help="Usa estes valores enquanto o app estiver aberto, sem mexer no arquivo."):
        apply_config(account=novo_acc, instance=nova_inst)
        st.rerun()
    if b3.button("💾 Salvar no arquivo", type="primary", width="stretch", disabled=bool(problemas)):
        try:
            caminho = settings_store.save(novo_acc, nova_inst, include_secrets=gravar_chaves)
        except OSError as e:
            st.error(f"Não consegui gravar o arquivo: {e}")
        else:
            apply_config(settings_store.load(), account=novo_acc)   # as chaves digitadas continuam valendo
            ss.config_text_version += 1
            st.toast(f"Configuração salva em {caminho}", icon="✅")
            st.rerun()


def _texto(lc: settings_store.LoadedConfig):
    ss = st.session_state
    st.caption("O conteúdo do arquivo, para editar como texto. Também serve para corrigir um erro de digitação "
               "que impede o formulário de ler o arquivo.")
    inicial = lc.text if lc.source == "arquivo" and lc.text else settings_store.to_toml(ss.account, ss.instance)
    texto = st.text_area("Conteúdo do configuracao.toml", inicial, height=420,
                         key=f"config_text_{ss.config_text_version}", label_visibility="collapsed")
    erro, problemas = None, []
    try:
        _acc, _inst, problemas, _segredo = settings_store.from_dict(settings_store.tomllib.loads(texto))
    except settings_store.tomllib.TOMLDecodeError as e:
        erro = str(e)
    if erro:
        st.error(f"Erro de digitação: {erro}.")
    for p in problemas:
        st.warning(p)
    if not erro and not problemas:
        st.caption("✅ O texto não tem erros.")
    if st.button("💾 Salvar este texto no arquivo", type="primary", disabled=bool(erro), key="salvar_texto"):
        try:
            novo = settings_store.save_text(texto)
        except (ValueError, OSError) as e:
            st.error(str(e))
        else:
            apply_config(novo)
            ss.config_text_version += 1
            st.toast(f"Configuração salva em {novo.path}", icon="✅")
            st.rerun()
