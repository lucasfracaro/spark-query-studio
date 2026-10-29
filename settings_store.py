"""
Configuração do app num arquivo fora do código: configuracao.toml.

- Por padrão o arquivo fica na pasta do projeto. Ele está no .gitignore e não entra no zip,
  então dá para ter a sua configuração sem que ela vá para o git.
- Para usar um arquivo em outro lugar: variável de ambiente SQS_CONFIG=/caminho/arquivo.toml
- O modelo comentado está em configuracao.exemplo.toml (é o que este módulo gera com os padrões).
- Sem o arquivo, valem os padrões (ou a configuração do formato antigo, se existir).
- Valores errados não impedem o app de abrir: viram avisos, a tela mostra o que corrigir e
  o botão "Salvar no arquivo" grava a versão corrigida.
- Chaves de acesso (access/secret key) só vão para o arquivo se você pedir. Nesse caso o arquivo
  fica legível só pelo seu usuário. Prefira um perfil do AWS CLI.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:          # Python 3.10
    import tomli as tomllib

from engines import (
    DEFAULT_GLUE_VERSION,
    DEFAULT_WORKER_TYPE,
    GLUE_VERSIONS,
    GLUE_WORKER_TYPES,
    AwsAccount,
    InstanceConfig,
)

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_FILE = PROJECT_DIR / "configuracao.toml"
EXAMPLE_FILE = PROJECT_DIR / "configuracao.exemplo.toml"
ENV_VAR = "SQS_CONFIG"
LEGACY_FILE = Path.home() / ".spark_query_studio" / "config.json"    # formato antigo (só leitura)

SECRET_FIELDS = ("access_key", "secret_key", "session_token")
AUTH_MODES = {"perfil": "profile", "chaves": "keys", "padrao": "default"}       # no arquivo -> no app
AUTH_NAMES = {v: k for k, v in AUTH_MODES.items()}
MIN_WORKERS, MAX_WORKERS = 2, 100
MIN_IDLE, MAX_IDLE = 1, 480

REGION_RE = re.compile(r"^[a-z]{2}(-[a-z]+)+-\d+$")
ROLE_ARN_RE = re.compile(r"^arn:aws[a-z-]*:iam::\d{12}:role/[\w+=,.@/-]+$")

# chave no arquivo -> campo no app
_AWS_KEYS = {"regiao": "region", "autenticacao": "auth_mode", "perfil": "profile", "role_glue": "glue_role_arn",
             "diretorio_temporario": "temp_dir", "access_key": "access_key", "secret_key": "secret_key",
             "session_token": "session_token"}
_INST_KEYS = {"tipo_worker": "worker_type", "workers": "num_workers", "versao_glue": "glue_version",
              "minutos_sem_uso": "idle_timeout_min", "lake_formation": "lake_formation",
              "iniciar_ao_abrir": "start_on_open", "argumentos_extras": "extra_args", "tags": "tags"}


@dataclass
class LoadedConfig:
    account: AwsAccount
    instance: InstanceConfig
    path: Path
    source: str = "padrao"          # "arquivo" | "legado" | "padrao"
    error: str | None = None        # o arquivo não pôde ser lido (ex.: erro de sintaxe)
    problems: list[str] = field(default_factory=list)   # valores inválidos (o padrão foi usado no lugar)
    has_secrets: bool = False       # o arquivo tem chaves de acesso
    text: str = ""                  # conteúdo do arquivo, como está no disco

    @property
    def ok(self) -> bool:
        return self.error is None and not self.problems


def config_path() -> Path:
    caminho = os.environ.get(ENV_VAR, "").strip()
    return Path(caminho).expanduser() if caminho else DEFAULT_FILE


# --------------------------------------------------------------------------------------------
# Leitura
# --------------------------------------------------------------------------------------------
def _as_int(valor, nome: str, minimo: int, maximo: int, padrao: int, problemas: list[str]) -> int:
    if isinstance(valor, bool) or not isinstance(valor, (int, float, str)):
        problemas.append(f"`{nome}` deve ser um número entre {minimo} e {maximo}; usei {padrao}.")
        return padrao
    try:
        numero = int(str(valor).strip())
    except ValueError:
        problemas.append(f"`{nome}` = {valor!r} não é um número; usei {padrao}.")
        return padrao
    if not minimo <= numero <= maximo:
        problemas.append(f"`{nome}` = {numero} está fora do intervalo {minimo}–{maximo}; usei {padrao}.")
        return padrao
    return numero


def _as_bool(valor, nome: str, padrao: bool, problemas: list[str]) -> bool:
    if isinstance(valor, bool):
        return valor
    texto = str(valor).strip().lower()
    if texto in ("true", "sim", "1", "yes"):
        return True
    if texto in ("false", "nao", "não", "0", "no", ""):
        return False
    problemas.append(f"`{nome}` deve ser true ou false; usei {str(padrao).lower()}.")
    return padrao


def _as_str_dict(valor, nome: str, problemas: list[str]) -> dict[str, str]:
    if not isinstance(valor, dict):
        problemas.append(f"`{nome}` deve ser uma seção de chave = valor; foi ignorado.")
        return {}
    return {str(k).strip(): str(v).strip() for k, v in valor.items() if str(k).strip()}


def from_dict(data: dict) -> tuple[AwsAccount, InstanceConfig, list[str], bool]:
    """Converte o conteúdo do arquivo. Valor inválido não derruba a leitura: vira aviso e usa o padrão."""
    problemas: list[str] = []
    acc, inst = AwsAccount(), InstanceConfig()

    for secao in data:
        if secao not in ("aws", "instancia"):
            problemas.append(f"Seção `[{secao}]` desconhecida: foi ignorada (as seções são [aws] e [instancia]).")

    aws = data.get("aws", {})
    if not isinstance(aws, dict):
        problemas.append("`aws` deve ser uma seção ([aws]); foi ignorada.")
        aws = {}
    for chave, valor in aws.items():
        campo = _AWS_KEYS.get(chave)
        if campo is None:
            problemas.append(f"Chave `{chave}` desconhecida em [aws]: foi ignorada. "
                             f"As chaves são: {', '.join(_AWS_KEYS)}.")
        elif campo == "auth_mode":
            modo = str(valor).strip().lower()
            modo = AUTH_MODES.get(modo, modo)
            if modo in AUTH_NAMES:
                acc.auth_mode = modo
            else:
                problemas.append(f"`autenticacao` = {valor!r} não existe; use perfil, chaves ou padrao. "
                                 "Usei perfil.")
        else:
            setattr(acc, campo, str(valor).strip())

    secao_inst = data.get("instancia", {})
    if not isinstance(secao_inst, dict):
        problemas.append("`instancia` deve ser uma seção ([instancia]); foi ignorada.")
        secao_inst = {}
    for chave, valor in secao_inst.items():
        campo = _INST_KEYS.get(chave)
        if campo is None:
            problemas.append(f"Chave `{chave}` desconhecida em [instancia]: foi ignorada. "
                             f"As chaves são: {', '.join(_INST_KEYS)}.")
        elif campo == "worker_type":
            tipo = str(valor).strip().upper()
            if tipo in GLUE_WORKER_TYPES:
                inst.worker_type = tipo
            else:
                problemas.append(f"`tipo_worker` = {valor!r} não existe; use {', '.join(GLUE_WORKER_TYPES)}. "
                                 f"Usei {DEFAULT_WORKER_TYPE}.")
        elif campo == "glue_version":
            versao = str(valor).strip()
            if versao in GLUE_VERSIONS:
                inst.glue_version = versao
            else:
                problemas.append(f"`versao_glue` = {valor!r} não existe; use {', '.join(GLUE_VERSIONS)}. "
                                 f"Usei {DEFAULT_GLUE_VERSION}.")
        elif campo == "num_workers":
            inst.num_workers = _as_int(valor, chave, MIN_WORKERS, MAX_WORKERS, inst.num_workers, problemas)
        elif campo == "idle_timeout_min":
            inst.idle_timeout_min = _as_int(valor, chave, MIN_IDLE, MAX_IDLE, inst.idle_timeout_min, problemas)
        elif campo in ("lake_formation", "start_on_open"):
            setattr(inst, campo, _as_bool(valor, chave, getattr(inst, campo), problemas))
        else:
            setattr(inst, campo, _as_str_dict(valor, chave, problemas))

    tem_segredo = any(getattr(acc, f) for f in SECRET_FIELDS)
    return acc, inst, problemas, tem_segredo


def check(account: AwsAccount, instance: InstanceConfig | None = None, profiles: list[str] | None = None,
          require_keys: bool = True) -> list[str]:
    """Problemas de conteúdo (região, role, perfil...). profiles: perfis do AWS CLI deste computador.
    require_keys=False: chaves ausentes não são problema (normalmente elas não ficam no arquivo)."""
    problemas = []
    if not REGION_RE.match(account.region or ""):
        problemas.append(f"Região `{account.region}` inválida. Exemplos: `sa-east-1`, `us-east-1`.")
    if account.glue_role_arn and not ROLE_ARN_RE.match(account.glue_role_arn):
        problemas.append("O ARN da role do Glue parece incompleto. Formato: "
                         "`arn:aws:iam::123456789012:role/NomeDaRole`.")
    if account.temp_dir and not account.temp_dir.startswith("s3://"):
        problemas.append("O diretório temporário deve começar com `s3://`.")
    if account.auth_mode == "profile" and account.profile and profiles and account.profile not in profiles:
        problemas.append(f"O perfil `{account.profile}` não existe no AWS CLI deste computador. "
                         f"Perfis encontrados: {', '.join(profiles)}.")
    if require_keys and account.auth_mode == "keys" and not (account.access_key and account.secret_key):
        problemas.append("Com autenticação por chaves, informe a access key e a secret key.")
    return problemas


def load() -> LoadedConfig:
    caminho = config_path()
    if caminho.exists():
        try:
            texto = caminho.read_text(encoding="utf-8-sig")
        except OSError as e:
            return LoadedConfig(AwsAccount(), InstanceConfig(), caminho, "arquivo",
                                error=f"Não consegui ler o arquivo: {e}")
        try:
            dados = tomllib.loads(texto)
        except tomllib.TOMLDecodeError as e:
            return LoadedConfig(AwsAccount(), InstanceConfig(), caminho, "arquivo", text=texto,
                                error=f"O arquivo tem um erro de digitação: {e}.")
        acc, inst, problemas, segredo = from_dict(dados)
        return LoadedConfig(acc, inst, caminho, "arquivo", problems=problemas + check(acc, require_keys=False),
                            has_secrets=segredo, text=texto)
    if LEGACY_FILE.exists():
        try:
            dados = json.loads(LEGACY_FILE.read_text(encoding="utf-8"))
            acc = AwsAccount(**{k: v for k, v in dados.get("account", {}).items()
                                if k in AwsAccount.__dataclass_fields__ and k not in SECRET_FIELDS})
            inst = InstanceConfig(**{k: v for k, v in dados.get("instance", {}).items()
                                     if k in InstanceConfig.__dataclass_fields__})
            return LoadedConfig(acc, inst, caminho, "legado")
        except Exception:
            pass
    return LoadedConfig(AwsAccount(), InstanceConfig(), caminho, "padrao")


# --------------------------------------------------------------------------------------------
# Gravação
# --------------------------------------------------------------------------------------------
def _toml_str(texto: str) -> str:
    return json.dumps(str(texto), ensure_ascii=False)      # string JSON também é string TOML válida


def _toml_table(valores: dict[str, str], exemplo: str) -> str:
    if not valores:
        return f"# {exemplo}\n"
    return "".join(f"{_toml_str(k)} = {_toml_str(v)}\n" for k, v in valores.items())


def to_toml(account: AwsAccount, instance: InstanceConfig, include_secrets: bool = False) -> str:
    """Texto do arquivo, com comentários explicando cada opção."""
    a, i = account, instance
    if include_secrets and any(getattr(a, f) for f in SECRET_FIELDS):
        chaves = (f"access_key = {_toml_str(a.access_key)}\n"
                  f"secret_key = {_toml_str(a.secret_key)}\n"
                  f"session_token = {_toml_str(a.session_token)}\n")
    else:
        chaves = '# access_key = ""\n# secret_key = ""\n# session_token = ""\n'
    return f"""\
# Configuração do Spark Query Studio.
# Este arquivo fica fora do git (.gitignore). Dá para editar aqui ou na tela, em ⚙️ Configuração.

[aws]
regiao = {_toml_str(a.region)}
# Como autenticar na AWS:
#   perfil = perfil do AWS CLI (~/.aws), o mais seguro
#   chaves = access key e secret key (abaixo)
#   padrao = cadeia padrão da AWS (variáveis de ambiente, SSO, role da máquina)
autenticacao = {_toml_str(AUTH_NAMES.get(a.auth_mode, "perfil"))}
perfil = {_toml_str(a.profile)}
# Role que o Glue assume para rodar a sessão Spark (precisa ler o S3 e o Glue Data Catalog)
role_glue = {_toml_str(a.glue_role_arn)}
# Diretório temporário no S3 (opcional), ex.: "s3://meu-bucket/glue-tmp/"
diretorio_temporario = {_toml_str(a.temp_dir)}
# Só com autenticacao = "chaves". Chaves gravadas aqui ficam em texto puro: prefira um perfil.
{chaves}
[instancia]
# {", ".join(GLUE_WORKER_TYPES)}
tipo_worker = {_toml_str(i.worker_type)}
workers = {int(i.num_workers)}
# {", ".join(f"{v} ({d})" for v, d in GLUE_VERSIONS.items())}
versao_glue = {_toml_str(i.glue_version)}
# A sessão encerra sozinha depois desse tempo sem uso
minutos_sem_uso = {int(i.idle_timeout_min)}
# Ler as tabelas do catálogo com as permissões do Lake Formation, como o Athena
# (para dados que estão no S3 de outra conta)
lake_formation = {str(bool(i.lake_formation)).lower()}
# Iniciar a sessão em segundo plano assim que o app abre (ela cobra desde que fica pronta)
iniciar_ao_abrir = {str(bool(i.start_on_open)).lower()}

[instancia.argumentos_extras]
{_toml_table(i.extra_args, '"--datalake-formats" = "iceberg"')}
[instancia.tags]
{_toml_table(i.tags, 'projeto = "analises"')}"""


def _write(caminho: Path, texto: str, privado: bool) -> Path:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    temporario = caminho.with_name(caminho.name + ".tmp")
    temporario.write_text(texto, encoding="utf-8")
    if privado:
        try:
            os.chmod(temporario, 0o600)     # só o seu usuário lê (como o ~/.aws/credentials)
        except OSError:
            pass
    os.replace(temporario, caminho)        # troca de uma vez: um erro no meio não corrompe o arquivo
    return caminho


def save(account: AwsAccount, instance: InstanceConfig, include_secrets: bool = False) -> Path:
    tem_segredo = include_secrets and any(getattr(account, f) for f in SECRET_FIELDS)
    return _write(config_path(), to_toml(account, instance, include_secrets), privado=tem_segredo)


def save_text(texto: str) -> LoadedConfig:
    """Grava o texto do arquivo como está (editado à mão na tela). Recusa texto que não dá para ler."""
    try:
        dados = tomllib.loads(texto)
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"O texto tem um erro de digitação: {e}.") from e
    _acc, _inst, _problemas, segredo = from_dict(dados)
    _write(config_path(), texto, privado=segredo)
    return load()
