"""
Queries salvas no S3, para a importação do Athena.

- Busca pelo nome da tabela: a query de uma tabela fica salva como nome_da_tabela.sql. A busca lista os arquivos da
  pasta raiz e das subpastas e fica com os que têm esse nome. No padrão do SAF, o motor de execução faz parte do
  caminho: um arquivo dentro de uma pasta /athena/ é uma query do Athena.
- Processo do Spark (find_process): a query em uma pasta /spark/ (nome_da_base.sql) e os JSON de orquestração e de
  metadados (nome_da_base.json) em pastas /orchestration/ e /metadata/, todos com o nome da base final.
- Navegação: os buckets da conta e as pastas de cada um, para escolher um arquivo específico.
- Só nos buckets da conta logada, os que o ListBuckets devolve (ele só devolve os da própria conta): o app não
  consulta buckets de outras contas.
- A lista de arquivos de uma pasta raiz vale por alguns minutos (QueryIndex.fresh): a busca seguinte na mesma pasta,
  por outra tabela, não lista tudo de novo.

Permissões do seu usuário: s3:ListAllMyBuckets, s3:ListBucket no bucket das queries e s3:GetObject nos arquivos.
"""
from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from sizing import fmt_bytes

QUERY_EXTENSIONS = (".sql", ".hql", ".txt")
PROCESS_EXTENSIONS = (".sql", ".hql", ".txt", ".json")     # query do Spark e os JSON de orquestração e metadados
MAX_QUERY_BYTES = 2 * 1024 * 1024           # uma query é texto: mais que isso não é uma query
SEARCH_BUDGET_S = 25.0                      # tempo máximo listando a pasta raiz
INDEX_TTL_S = 600                           # a lista de arquivos de uma pasta raiz vale 10 minutos
MAX_FOLDER_ITEMS = 300                      # itens mostrados de uma pasta na navegação (o filtro acha o resto)
MAX_FOLDER_PAGES = 20                       # páginas de 1.000 objetos lidas de uma pasta na navegação


class S3QueryError(Exception):
    """Mensagem pronta para a tela: bucket de outra conta, sem permissão, arquivo grande demais..."""


def _codigo(e: Exception) -> str:
    resposta = getattr(e, "response", None)
    if isinstance(resposta, dict):
        codigo = (resposta.get("Error") or {}).get("Code")
        if codigo:
            return str(codigo)
    return type(e).__name__


def normalize_uri(uri: str) -> str:
    """Aceita "s3://bucket/pasta", "bucket/pasta" ou "s3a://..." e devolve "s3://bucket/pasta/" ("" se vazio)."""
    texto = (uri or "").strip()
    if not texto:
        return ""
    texto = re.sub(r"^s3[an]?://", "", texto, flags=re.I).lstrip("/")
    bucket, _, prefixo = texto.partition("/")
    if not bucket:
        return ""
    prefixo = prefixo.strip()
    return f"s3://{bucket}/{prefixo}" + ("/" if prefixo and not prefixo.endswith("/") else "")


def split_uri(uri: str) -> tuple[str, str]:
    """s3://bucket/pasta -> ("bucket", "pasta/"); só o bucket -> ("bucket", "")."""
    normal = normalize_uri(uri)
    if not normal:
        return "", ""
    bucket, _, prefixo = normal[len("s3://"):].partition("/")
    return bucket, prefixo


def base_name(table: str) -> str:
    """O nome da base: "db.tabela", "`tabela`", "tabela.sql" ou "tabela.json" -> "tabela"."""
    nome = (table or "").strip().strip("`\"' ")
    for extensao in PROCESS_EXTENSIONS:
        if nome.lower().endswith(extensao):
            nome = nome[:-len(extensao)]
            break
    return nome.split(".")[-1].strip("`\"' ")


def table_file_name(table: str) -> str:
    """O arquivo da query de uma tabela: "db.tabela", "`tabela`" ou "tabela.sql" -> "tabela.sql"."""
    nome = base_name(table)
    return f"{nome}.sql" if nome else ""


@dataclass
class S3File:
    bucket: str
    key: str
    size: int = 0
    modified: datetime | None = None

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"

    @property
    def name(self) -> str:
        return self.key.rsplit("/", 1)[-1]

    @property
    def folders(self) -> list[str]:
        """As pastas do caminho, em minúsculas."""
        return [p.lower() for p in self.key.split("/")[:-1]]

    @property
    def athena(self) -> bool:
        """No padrão do SAF, o motor de execução faz parte do caminho: .../athena/... é query do Athena."""
        return "athena" in self.folders

    @property
    def engine(self) -> str:
        """"athena", "spark" ou "" (o caminho não diz, ou diz os dois)."""
        motores = [m for m in ("athena", "spark") if m in self.folders]
        return motores[0] if len(motores) == 1 else ""

    @property
    def kind(self) -> str:
        """Para o processo do Spark: "query" (.sql), "orquestracao" ou "metadados" (.json na pasta de cada um)."""
        nome = self.name.lower()
        if nome.endswith((".sql", ".hql")):
            return "query"
        if nome.endswith(".json"):
            if any(p.startswith(("orch", "orquestra")) for p in self.folders):
                return "orquestracao"
            if any(p.startswith(("metadata", "metadado")) for p in self.folders):
                return "metadados"
        return ""

    def describe(self) -> str:
        partes = [fmt_bytes(self.size)]
        if self.modified is not None:
            try:
                partes.append(self.modified.astimezone().strftime("%d/%m/%Y %H:%M"))
            except (ValueError, OSError):
                pass
        return " · ".join(partes)


@dataclass
class QueryIndex:
    """Os arquivos de query (.sql, .hql, .txt) de uma pasta raiz e das subpastas."""
    root: str
    files: list[S3File]
    scanned: int                 # objetos olhados
    complete: bool               # False: a pasta tem objetos demais e a listagem parou no tempo limite
    built_at: float

    def fresh(self, ttl: float = INDEX_TTL_S) -> bool:
        return time.time() - self.built_at < ttl

    def find(self, table: str) -> list[S3File]:
        """Os arquivos nome_da_tabela.sql: primeiro os do Athena, depois os mais novos."""
        alvo = table_file_name(table).lower()
        if not alvo:
            return []
        achados = [f for f in self.files if f.name.lower() == alvo]
        return sorted(achados, key=lambda f: (not f.athena, -(f.modified.timestamp() if f.modified else 0), f.key))

    def find_process(self, name: str) -> dict[str, list[S3File]]:
        """Os arquivos do processo do Spark de uma base: {"query", "orquestracao", "metadados"}. Em cada lista,
        primeiro os de caminho do Spark, depois os sem motor no caminho, por último os do Athena; e os mais novos."""
        base = base_name(name).lower()
        saida: dict[str, list[S3File]] = {"query": [], "orquestracao": [], "metadados": []}
        if not base:
            return saida
        for f in self.files:
            if f.name.lower().rpartition(".")[0] != base or f.kind not in saida:
                continue
            if f.kind == "query" and f.engine == "athena":
                continue                     # a query do Athena não é a query do Spark
            saida[f.kind].append(f)
        ordem = {"spark": 0, "": 1, "athena": 2}
        for lista in saida.values():
            lista.sort(key=lambda f: (ordem[f.engine], -(f.modified.timestamp() if f.modified else 0), f.key))
        return saida


@dataclass
class Folder:
    bucket: str
    prefix: str
    folders: list[str]           # subpastas (nome, sem a barra)
    files: list[S3File]          # os arquivos com as extensões pedidas (queries; no processo do Spark, também JSON)
    others: int                  # outros arquivos
    truncated: bool              # a pasta tem mais itens do que os mostrados

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"


class S3Queries:
    """Busca e navegação nos buckets da conta logada (cliente boto3 do S3 com as suas credenciais)."""

    def __init__(self, s3_client):
        self.s3 = s3_client
        self._own: list[str] | None = None

    def buckets(self) -> list[str]:
        """Os buckets da conta logada (ListBuckets), em ordem alfabética."""
        if self._own is None:
            nomes, token = [], None
            try:
                while True:
                    resp = self.s3.list_buckets(**({"ContinuationToken": token} if token else {}))
                    nomes += [b["Name"] for b in resp.get("Buckets") or []]
                    token = resp.get("ContinuationToken")
                    if not token:
                        break
            except Exception as e:
                raise S3QueryError(f"Não consegui listar os buckets da sua conta ({_codigo(e)}). O seu usuário "
                                   "precisa de s3:ListAllMyBuckets: o app só abre buckets da própria conta.") from e
            self._own = sorted(set(nomes))
        return self._own

    def _check(self, bucket: str):
        if bucket not in self.buckets():
            raise S3QueryError(f"O bucket `{bucket}` não é da conta logada: o app só procura nos buckets da sua "
                               "conta.")

    def index(self, root: str, budget_s: float = SEARCH_BUDGET_S,
              on_progress: Callable[[int], None] | None = None,
              extensions: tuple[str, ...] = PROCESS_EXTENSIONS) -> QueryIndex:
        """Lista a pasta raiz (e as subpastas) e guarda os arquivos de query e os JSON. Para no tempo limite."""
        bucket, prefixo = split_uri(root)
        if not bucket:
            raise S3QueryError("A pasta raiz deve ser como `s3://bucket/pasta/`.")
        self._check(bucket)
        limite = time.monotonic() + budget_s
        arquivos, olhados, token, completo = [], 0, None, True
        while True:
            params = {"Bucket": bucket, "Prefix": prefixo}
            if token:
                params["ContinuationToken"] = token
            try:
                resp = self.s3.list_objects_v2(**params)
            except Exception as e:
                raise S3QueryError(f"Não consegui listar `s3://{bucket}/{prefixo}` ({_codigo(e)}). O seu usuário "
                                   "precisa de s3:ListBucket nesse bucket.") from e
            for obj in resp.get("Contents") or []:
                olhados += 1
                chave = str(obj.get("Key", ""))
                if chave.lower().endswith(extensions):
                    arquivos.append(S3File(bucket, chave, int(obj.get("Size") or 0), obj.get("LastModified")))
            if on_progress is not None:
                on_progress(olhados)
            token = resp.get("NextContinuationToken") if resp.get("IsTruncated") else None
            if not token:
                break
            if time.monotonic() > limite:
                completo = False
                break
        return QueryIndex(f"s3://{bucket}/{prefixo}", arquivos, olhados, completo, time.time())

    def folder(self, bucket: str, prefix: str = "", starts_with: str = "", max_items: int = MAX_FOLDER_ITEMS,
               max_pages: int = MAX_FOLDER_PAGES, extensions: tuple[str, ...] = QUERY_EXTENSIONS) -> Folder:
        """O que tem numa pasta: subpastas e os arquivos com as extensões pedidas. starts_with filtra pelo começo
        do nome (no próprio S3)."""
        self._check(bucket)
        pastas, arquivos, outros, token, cortado, paginas = [], [], 0, None, False, 0
        while True:
            params = {"Bucket": bucket, "Prefix": prefix + starts_with, "Delimiter": "/"}
            if token:
                params["ContinuationToken"] = token
            try:
                resp = self.s3.list_objects_v2(**params)
            except Exception as e:
                raise S3QueryError(f"Não consegui abrir `s3://{bucket}/{prefix}` ({_codigo(e)}). O seu usuário "
                                   "precisa de s3:ListBucket nesse bucket.") from e
            pastas += [str(p.get("Prefix", ""))[len(prefix):].rstrip("/") for p in resp.get("CommonPrefixes") or []]
            for obj in resp.get("Contents") or []:
                chave = str(obj.get("Key", ""))
                if chave == prefix or chave.endswith("/"):
                    continue                     # o "arquivo" que marca a própria pasta
                if chave.lower().endswith(extensions):
                    arquivos.append(S3File(bucket, chave, int(obj.get("Size") or 0), obj.get("LastModified")))
                else:
                    outros += 1
            token = resp.get("NextContinuationToken") if resp.get("IsTruncated") else None
            paginas += 1
            if not token:
                break
            if len(pastas) + len(arquivos) >= max_items or paginas >= max_pages:
                cortado = True
                break
        pastas = sorted(p for p in pastas if p)
        return Folder(bucket, prefix, pastas[:max_items], arquivos[:max(0, max_items - len(pastas))], outros,
                      cortado or len(pastas) + len(arquivos) > max_items)

    def read(self, bucket: str, key: str) -> str:
        """O texto do arquivo (UTF-8, com ou sem BOM; senão, Windows-1252)."""
        self._check(bucket)
        try:
            resp = self.s3.get_object(Bucket=bucket, Key=key)
        except Exception as e:
            raise S3QueryError(f"Não consegui abrir `s3://{bucket}/{key}` ({_codigo(e)}). O seu usuário precisa "
                               "de s3:GetObject nesse arquivo.") from e
        tamanho = int(resp.get("ContentLength") or 0)
        if tamanho > MAX_QUERY_BYTES:
            raise S3QueryError(f"`s3://{bucket}/{key}` tem {fmt_bytes(tamanho)}: grande demais para ser uma query.")
        dados = resp["Body"].read(MAX_QUERY_BYTES + 1)
        if len(dados) > MAX_QUERY_BYTES:
            raise S3QueryError(f"`s3://{bucket}/{key}` é grande demais para ser uma query.")
        try:
            return dados.decode("utf-8-sig")
        except UnicodeDecodeError:
            return dados.decode("cp1252", errors="replace")
