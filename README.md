# ⚡ Spark Query Studio

[![testes](https://github.com/lucasfracaro/spark-query-studio/actions/workflows/testes.yml/badge.svg)](https://github.com/lucasfracaro/spark-query-studio/actions/workflows/testes.yml)

**⬇️ [Baixar o projeto completo (.zip)](https://github.com/lucasfracaro/spark-query-studio/raw/main/dist/spark-query-studio.zip)**: o arquivo também está na pasta [`dist/`](dist/) do repositório e é atualizado a cada versão.

Frontend em Streamlit para escrever ou importar uma query Spark SQL, configurar a orquestração das entradas e a instância, executar no **AWS Glue (Interactive Sessions)** e ver as primeiras linhas, com exportação para CSV. Assim você não precisa passar pelo Athena para validar uma ETL.

```
Navegador (Streamlit)  ──boto3──▶  Glue Interactive Session (Spark gerenciado)
  orquestração + query               ├─ lê Glue Data Catalog (mesmas tabelas do Athena)
  ◀── 100 primeiras linhas ───────   └─ lê S3 direto (parquet/csv/json/…)
```

## Como rodar

```bash
# Windows: dê dois cliques em run_windows.bat
# Linux/macOS:
./run.sh
# ou manualmente:
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m streamlit run app.py
```

O navegador abre em `http://localhost:8501`. Os scripts instalam as dependências na primeira vez e de novo quando o `requirements.txt` muda. Se a instalação falhar no meio (rede, proxy), é só rodar outra vez: ela recomeça em vez de abrir o app pela metade.

## Configuração (`configuracao.toml`)

A conta AWS e a sessão ficam num arquivo fora do código, o **`configuracao.toml`**, na pasta do projeto. Ele está no `.gitignore` e nunca entra no zip, então cada um tem o seu sem que ele vá para o git. O modelo comentado é o [`configuracao.exemplo.toml`](configuracao.exemplo.toml):

```toml
[aws]
regiao = "sa-east-1"
autenticacao = "perfil"        # perfil (AWS CLI), chaves ou padrao (env vars, SSO, role da máquina)
perfil = "dev"
role_glue = "arn:aws:iam::123456789012:role/GlueInteractiveSessionRole"
diretorio_temporario = ""

[instancia]
tipo_worker = "G.1X"
workers = 2
versao_glue = "4.0"
minutos_sem_uso = 30
lake_formation = true          # ler tabelas de outra conta, como o Athena (veja abaixo)
iniciar_ao_abrir = false
database_padrao = "workspace_db"   # USE de cada execução e database do DDL; "" = nenhum
```

- **⚙️ Configuração** (barra lateral) mostra o que foi lido do arquivo e deixa corrigir pelo formulário ou pelo texto do arquivo. **Aplicar** usa os valores enquanto o app estiver aberto; **Salvar no arquivo** grava para as próximas vezes. Sem o arquivo, a primeira gravação cria o `configuracao.toml`.
- Se o arquivo tiver um problema (erro de digitação, região inválida, chave com nome errado, perfil que não existe no computador), o app abre mesmo assim, usa o valor padrão onde havia erro e mostra no topo o que corrigir, com um botão que abre a configuração.
- Para usar um arquivo em outro lugar: `SQS_CONFIG=/caminho/minha_config.toml ./run.sh`.
- Chaves de acesso (`access_key`/`secret_key`) só vão para o arquivo se você marcar **Guardar as chaves no arquivo**. Nesse caso o arquivo fica legível só pelo seu usuário. Prefira um perfil do AWS CLI.
- **Database padrão** (`database_padrao`, padrão `workspace_db`): vem preenchido na orquestração (aba **⚙️ Opções**), na importação do Athena e no DDL, e pode ser trocado em cada orquestração ou deixado vazio. Ele vira um `USE` no começo de cada execução e vale para as tabelas escritas sem database. Escrever `database.tabela` na query continua funcionando sempre. Se o database não existir ou a role não tiver acesso a ele, a query roda assim mesmo e o resultado mostra um aviso.

## Fluxo

1. **⚙️ Configuração**: região, autenticação e o **ARN da IAM Role do Glue** (veja acima). Use **Testar conexão** para conferir.
2. **🖥️ Instância**: tipo de worker (G.1X a G.16X e os R.1X a R.8X, de memória otimizada, com os DPUs de cada um), número de workers, versão do Glue, tempo sem uso até encerrar, **Ler as tabelas pelo Lake Formation** e argumentos extras do job (ex.: `--datalake-formats = iceberg`). Escolha a **mesma versão do Glue do job em produção**:

   | Glue | Spark | Python | Observação |
   |---|---|---|---|
   | 6.0 | 4.1 | 3.13 | modo ANSI ligado: overflow, divisão por zero e cast inválido viram erro |
   | 5.1 | 3.5.6 | 3.11 | |
   | 5.0 | 3.5.4 | 3.11 | padrão |
   | 4.0 | 3.3 | 3.10 | |
3. **Passo 1 – Orquestração**: cada linha de entrada vira uma *temp view*:
   - `catalog` + `database.tabela` → lê do Glue Data Catalog;
   - `s3` + `s3://bucket/caminho/` + formato + opções (`header=true;sep=;`).
   Também dá para aplicar um filtro antes de criar a view, ligar **cache**, definir configs `spark.*`, escolher um database padrão e o limite de linhas. Em **📝 Abrir orquestração** ela pode ser editada como arquivo, salva na máquina ou carregada de um arquivo (veja abaixo).
4. **Passo 2 – Query**: escreva no editor ou importe um `.sql`/`.txt`. O conteúdo vai para o editor, onde pode ser alterado. Vários comandos separados por `;` são executados em ordem, e o resultado exibido é o do último. Comentários e hints do Spark (`/*+ BROADCAST(t) */`) chegam ao Spark exatamente como foram escritos.
5. **Conferência da orquestração**: antes de enviar ao cluster, a query é lida e cada base usada em `FROM`/`JOIN` é comparada com as entradas da orquestração. Se faltar alguma, a execução é bloqueada com uma mensagem como *"A base `db.produtos` é usada na query (linha 3), mas não está na orquestração"*. O aviso já aparece embaixo do editor enquanto você digita. CTEs (`WITH`), views criadas na própria query, subqueries e funções como `range()` não são cobradas. Se o Spark ainda assim não encontrar uma tabela, o erro dele é traduzido para a mesma mensagem. A conferência pode ser desligada em **Opções**.
6. **🔒 Modo somente leitura** (ligado por padrão): comandos que gravam ou alteram dados ou o catálogo (`INSERT`, `CREATE TABLE`, `DROP`, `ALTER`, `MERGE`, `DELETE`, `MSCK REPAIR`…) são bloqueados antes de ir para o cluster, com o número e a linha do comando. Consultas e comandos que só afetam a sessão (`CREATE TEMP VIEW`, `CACHE TABLE`, `SET`, `USE`) rodam normalmente. A role do Glue costuma ter permissão de escrita, então um comando esquecido na query gravaria de verdade; para executar mesmo assim, ligue **Permitir comandos que gravam** em **Opções**.
7. **▶️ Executar**: veja [Sessão e velocidade](#sessão-e-velocidade). Durante a execução no Glue aparece **⏹️ Cancelar execução**, que cancela a query no cluster.
8. **Passo 3 – Resultado**: mostra as N primeiras linhas (**Linhas exibidas**, em ⚙️ Opções: 100 por padrão, até 10 mil), o schema e o log. **CSV das linhas exibidas** baixa essas linhas. **📦 Preparar CSV com todas as linhas** roda a mesma query de novo na sessão e traz o resultado inteiro, até 1.048.575 linhas (o máximo do Excel): as linhas vão para um CSV comprimido no driver da sessão e voltam em partes de 2 MB pela própria API da sessão, sem bucket e sem Athena. As execuções normais continuam trazendo só as linhas exibidas, então não ficam mais lentas. O formato padrão é o do **Excel em português** (`;` como separador e vírgula nos decimais, com UTF-8 BOM para os acentos); também há o padrão internacional (`,` e ponto) e tabulação, e os dois CSVs saem no mesmo formato. Quando a query falha, o erro aparece resumido (sem o plano da query e sem a pilha do Java), com um título em português e, quando dá para saber, o que fazer; a mensagem completa fica logo abaixo.

## Sessão e velocidade

A sessão do Glue é a parte cara: leva de 30 a 90 s para subir e cobra enquanto está ativa. Por isso ela é criada **uma vez** e reaproveitada:

- A sessão pertence ao app, não à aba do navegador: recarregar a página ou abrir outra aba continua usando a mesma.
- **Antes de cada execução, o status é conferido.** Se a sessão caiu (tempo sem uso, erro), outra é criada na hora. Se ela cair entre a conferência e o envio da query, a AWS recusa o envio e a ferramenta cria outra sessão e reenvia; uma query que chegou a rodar nunca é reenviada.
- Se o app foi reiniciado e você tem uma sessão criada antes **com a mesma configuração**, ela é reaproveitada em vez de pagar outra. Sessões com outra configuração aparecem na barra lateral com **♻️ Reaproveitar** ou **⏹️ Encerrar**.
- **▶️ Iniciar sessão agora** sobe a sessão em segundo plano: dá para continuar montando a query enquanto isso. Com `iniciar_ao_abrir = true`, isso acontece sozinho quando o app abre.
- A barra lateral mostra quando a sessão deve encerrar sozinha se ficar sem uso.
- Para escolher o tipo e o número de workers, veja a [estimativa do tamanho da sessão](#-estimativa-do-tamanho-da-sessão), feita ao importar a query do Athena.
- Cada execução roda numa **sessão Spark nova** dentro da sessão do Glue, o que é quase instantâneo: configurações Spark, `USE database` e views de execuções anteriores não passam para a próxima, como num job novo.
- **Cache entre execuções**: entradas marcadas com **Cache** são lidas do S3 uma vez e ficam em memória; as execuções seguintes, mesmo com outra query, usam os dados em memória enquanto a definição da entrada (origem, filtro, formato) não mudar. O mesmo vale para `CACHE TABLE` escrito na própria query. O que sai da orquestração é liberado. Como todo cache, ele é uma foto dos dados: se a tabela mudou no S3, use **🧹 Limpar cache** na barra lateral. O log da execução diz o que veio do cache e de que horas são os dados.
- A conferência do resultado começa em 0,25 s e vai espaçando até 2 s (antes eram 3 s fixos), então queries rápidas aparecem logo. A conexão com a AWS é criada uma vez, e as chamadas à AWS têm tempo limite: com a rede bloqueada, a tela avisa em segundos.

## Dados em outra conta (Lake Formation)

Quando as tabelas estão num data lake de **outra conta** e o acesso é controlado pelo **Lake Formation**, o Athena lê os dados com as credenciais que o Lake Formation fornece. O Spark no Glue, por padrão, lê os arquivos direto do S3 com a role da sessão, e o S3 da outra conta recusa (*Access Denied*). Ligue **Ler as tabelas pelo Lake Formation** (ou `lake_formation = true` no arquivo):

- **Glue 5.0, 5.1 e 6.0**: a sessão é criada com as configurações de [*Full Table Access*](https://docs.aws.amazon.com/glue/latest/dg/security-access-control-fta.html) e o Spark lê as tabelas normalmente, com as credenciais do Lake Formation.
- **Glue 4.0**: o Lake Formation só vale para leituras feitas pelo GlueContext. As entradas `catalog` passam a ser lidas por ele, com a parte de partição do filtro da orquestração indo como `push_down_predicate`:
  1. primeiro pela **leitura nativa do Spark** ([`create_data_frame.from_catalog` com `useSparkDataSource`](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-glue-context.html)), que respeita as permissões do Lake Formation e lê só as colunas que a query usa;
  2. se ela não funcionar na sua conta, pelo **DynamicFrame** (`create_dynamic_frame.from_catalog`), como num job do Glue. Ele lê todas as colunas das partições escolhidas e é bem mais lento.

  O log da execução diz qual das duas foi usada e quanto tempo a leitura levou para ser preparada. A query precisa usar os **aliases** da orquestração (usar `db.tabela` direto é bloqueado antes de rodar, com a mensagem dizendo qual alias usar).

Pré-requisitos (quem administra a conta precisa conferir):
- a **role da sessão** precisa de SELECT nas tabelas no Lake Formation e de `lakeformation:GetDataAccess` no IAM (já está no `iam_glue_role_policy.json`). No Athena vale o seu usuário; na sessão do Glue vale a role configurada. Se os jobs de produção já leem essas tabelas, usar a role deles é o caminho mais curto;
- no Glue 5 ou mais novo, o administrador do Lake Formation precisa permitir *full table access* para engines externas.

Cuidados com a leitura pelo GlueContext (Glue 4.0):
- quando cai no DynamicFrame, ela lê **todas as colunas** das partições escolhidas e lista as partições do catálogo antes de filtrar, então é bem mais lenta que a leitura normal do Spark. Use filtros de partição pequenos (ex.: `dt = '2026-10-28'`) e, para tabelas grandes, mais workers (com 2 workers só um deles processa os dados);
- marque **Cache** nessas entradas: a leitura lenta acontece uma vez só por sessão e as queries seguintes usam os dados em memória;
- o filtro da entrada pode ter qualquer coluna. O `push_down_predicate` só aceita colunas de partição, então a ferramenta confere no catálogo quais são as colunas de partição da tabela e manda para a leitura só a parte do filtro (os trechos separados por `AND`) que usa apenas elas; o resto é aplicado logo depois da leitura, e o resultado é o mesmo. O log mostra as duas partes. Se nenhuma parte do filtro for de partição, a tabela inteira é lida, e o log avisa.

### 📥 Importar query do Athena

Cole (ou abra) uma query escrita para o Athena e clique em **Gerar orquestração**. A ferramenta:

1. encontra as bases usadas. CTEs (e views criadas na própria query) não viram entrada da orquestração, porque não vêm do catálogo: os filtros de partição de dentro delas vão para a base de origem. Um filtro de partição aplicado fora da CTE, na query de cima, não é levado: o relatório avisa que todas as partições serão lidas, e dá para colocar o filtro na orquestração à mão;
2. consulta o **Glue Data Catalog** para saber quais colunas são de partição. Sem acesso ao Glue, supõe pelo nome (`dt`, `anomes`…) e avisa;
3. em cada CTE ou subquery, separa o `WHERE` e o `ON` do join pelos `AND` e leva para a orquestração as condições que usam **só colunas de partição** daquela base. Se a mesma base é lida com filtros diferentes, eles são combinados com `OR`;
4. `INSERT ... SELECT` e `CREATE TABLE ... AS SELECT` viram só o `SELECT`: você vê o que seria gravado, sem gravar, e a tabela de destino não entra na orquestração;
5. traduz a query para Spark SQL com o [sqlglot](https://github.com/tobymao/sqlglot) (`date_add('day', 1, d)` → `date_add(d, 1)`, `%Y-%m-%d` → `yyyy-MM-dd`, `approx_distinct` → `approx_count_distinct`…) e troca `db.tabela` pelo alias da orquestração;
6. mostra um relatório do que precisa de revisão: filtros dinâmicos (`dt = (SELECT max(dt) ...)`), `OR` misturando partição com outras colunas, funções sem tradução, bases particionadas lidas sem filtro.

Os filtros são **copiados**, não movidos: a query continua com as condições originais, então o resultado não muda. O filtro na orquestração só faz o Spark ler menos partições.

Ao aplicar, a orquestração gerada abre para revisão.

### 📏 Estimativa do tamanho da sessão

Na mesma janela, com **Estimar o tamanho da sessão** marcado, a ferramenta sugere o tipo e o número de workers para a query processar em ~2 minutos (dá para escolher 1, 2, 5 ou 10). **Nada roda no Athena**: a estimativa não ocupa sessão nem espaço de resultado dele. As informações vêm de:

1. **Glue Data Catalog**: quais partições o filtro de cada entrada escolhe (pelo `Expression` do `GetPartitions`; se o Glue não aceitar o filtro, ele é avaliado aqui, partição por partição) e o tamanho guardado nelas (estatísticas do crawler, do Hive ou do `ANALYZE TABLE`). Das colunas da tabela sai quanto dela a query lê: em Parquet/ORC o Spark lê só as colunas usadas, e texto pesa mais que número.
2. **S3**, quando o catálogo não tem o tamanho: soma os arquivos das partições escolhidas (até 40 partições; o resto é proporcional), **só em buckets da sua conta**. A lista vem do `ListBuckets`, que só devolve buckets da própria conta: um bucket de outra conta nunca recebe nenhuma chamada.
3. **Histórico do Athena**: se a mesma query já rodou no Athena (o histórico guarda 45 dias), os números daquela execução: dados lidos, linhas e quanto as etapas passaram umas para as outras. A busca ignora comentários, espaços e maiúsculas, e a mesma query com outra data também serve, com confiança menor. Dá para informar o workgroup (a busca fica mais rápida) ou colar o **ID da execução** (console do Athena → *Recent queries*). Execuções que reaproveitaram resultado são ignoradas; uma que falhou (por falta de recursos, por exemplo) entra com números parciais.
4. **A própria query**: JOINs, funções de janela, `COUNT(DISTINCT)`, `ORDER BY` sem `LIMIT`.

A conta (em `sizing.py`) é simples de propósito, e a janela mostra cada número usado:
- leitura: ~50 MB/s por núcleo em Parquet/ORC comprimido (~20 MB/s pelo DynamicFrame, que lê todas as colunas), mais ~0,15 s por pedaço de 128 MB de arquivo;
- shuffle: ~60 MB/s por núcleo, com o volume do Athena ou estimado pela query (leve, média ou pesada);
- workers: os núcleos que terminam no tempo escolhido, sem passar do paralelismo que os dados rendem. Cada worker é 1 executor, 1 worker é o driver, e o shuffle precisa caber no disco livre dos workers;
- tipo: G.1X por padrão (o preço por DPU é o mesmo e, com workers menores, sobram mais executores pelo mesmo custo); G.2X para query pesada com mais de 20 GB ou com muitas partições (driver maior); G.4X/G.8X para volumes de TB.

No Glue 4.0 com Lake Formation, a janela mostra também o tamanho necessário se a leitura cair no DynamicFrame. Quando o shuffle é grande, ela sugere `spark.sql.shuffle.partitions` (com o padrão, 200, cada tarefa ficaria grande demais e iria para o disco). Ao aplicar a orquestração, o tamanho sugerido e essa configuração passam a valer (dá para desmarcar); a sugestão também fica na barra lateral, com **Usar**. A estimativa acerta a ordem de grandeza, não o segundo: depois da primeira execução, o tempo real mostra se vale subir ou descer.

### 📝 Abrir orquestração

Mostra a orquestração em JSON para editar à mão, **salvar na máquina** ou **abrir um arquivo salvo**. Os erros de formato aparecem na hora, com linha e coluna. Exemplo:

```json
{
  "versao": 1,
  "database_padrao": "analytics",
  "limite_linhas": 100,
  "spark_conf": {"spark.sql.shuffle.partitions": "200"},
  "entradas": [
    {"alias": "vendas", "kind": "catalog", "source": "db_vendas.tb_vendas",
     "fmt": "parquet", "options": "", "filter": "dt BETWEEN '2026-10-01' AND '2026-10-31'", "cache": true}
  ]
}
```

### 🧾 Metadados do resultado

A aba **Metadados**, ao lado dos dados, monta o dicionário de dados a partir do schema que o Spark devolve: coluna, tipo no Spark, tipo no Athena e se aceita nulo, com uma coluna de descrição para preencher. Com **Calcular estatísticas** ligado em *Opções*, o cluster também calcula, sobre o **resultado completo**, nulos, % de nulos, distintos (aproximado), mínimo e máximo.

Dá para baixar o dicionário em CSV ou JSON e gerar o **DDL `CREATE EXTERNAL TABLE`** do Athena (Parquet, ORC, CSV ou JSON), com as colunas de partição escolhidas indo para `PARTITIONED BY` e as descrições virando `COMMENT`.

### Código gerado

**Ver código PySpark gerado** mostra exatamente o que vai rodar no cluster, com a query como foi escrita e a orquestração em texto normal: dá para copiar e rodar num notebook do Glue. A API do Glue aceita no máximo 68 mil caracteres de código por comando; se a query passar disso, ela vai comprimida (zlib) dentro do código, o que permite queries várias vezes maiores.

## Segurança

- O app só aceita conexões **deste computador** (`server.address = "localhost"` em `.streamlit/config.toml` e nos scripts de inicialização). Sem isso, o Streamlit aceitaria conexões de qualquer máquina da rede. Se ele for aberto de outro jeito e estiver exposto, a barra lateral avisa.
- A telemetria do Streamlit está desligada (`gatherUsageStats = false`).
- O `configuracao.toml` fica fora do git e do zip. Chaves de acesso só vão para ele se você pedir, e aí o arquivo fica legível só pelo seu usuário.
- O modo somente leitura vem ligado (veja acima).

## Permissões na AWS

- **Seu usuário/role** (quem roda o app): `iam_user_policy.json` (criar/consultar sessões Glue e `iam:PassRole`; para importar do Athena e estimar a sessão, só leitura: tabelas e partições do catálogo, histórico do Athena e lista dos seus buckets). Sem essas permissões o app funciona do mesmo jeito; a importação supõe as partições pelo nome e a estimativa usa o que conseguir ler.
- **Role do Glue** (assumida pela sessão): trust policy com `glue.amazonaws.com` + `iam_glue_role_policy.json` (S3, Glue Catalog e `lakeformation:GetDataAccess`). Se as tabelas usam Lake Formation, conceda também SELECT a essa role no Lake Formation.

Troque `SEU-BUCKET` e `SUA-CONTA` pelos valores reais.

## Custos

Uma Interactive Session é cobrada por DPU-hora enquanto estiver ativa (a barra lateral mostra os DPUs). Ela se encerra sozinha depois do tempo sem uso. Use **⏹️ Encerrar** quando terminar.

O Spark UI não vem ligado: em sessões interativas ele só funciona com um caminho de logs no S3 e um Spark History Server. Para ligar, use em **Argumentos extras do job** `--enable-spark-ui` = `true` e `--spark-event-logs-path` = `s3://seu-bucket/sparkui/`.

## Modo Spark local

Para testar a interface sem gastar com a AWS, instale o PySpark da mesma versão do Spark do seu Glue e escolha **Spark local**. Para o Glue 5.x: `pip install "pyspark==3.5.*"` (Python 3.11 e Java 8, 11 ou 17, como no Glue). Para o Glue 6.0: `pip install "pyspark==4.1.*"`, que exige Java 17 ou mais novo. Entradas `s3` funcionam com caminhos locais (ex.: `/dados/arquivo.parquet`). Para ler `s3a://` localmente, adicione `spark.jars.packages = org.apache.hadoop:hadoop-aws:3.3.4` nos argumentos extras. Como no Glue, a sessão local é criada uma vez e o cache das entradas vale entre as execuções.

## Atualizar o zip do projeto

O arquivo `dist/spark-query-studio.zip` faz parte do repositório. Ao alterar o projeto, gere o zip de novo antes do commit:

```bash
python scripts/gerar_zip.py
git add -A && git commit -m "..." && git push
```

O script só regrava o zip quando algum arquivo mudou. O GitHub Actions confere a cada push se o zip está em dia com o código e fica vermelho se alguém esquecer de gerar.

## Testes

```bash
pip install -r requirements-dev.txt
pytest                                # tudo que estiver instalado
ruff check .                          # verificador de código
pip install pyspark && pytest tests/test_local_spark.py   # integração com Spark real (Java 17)
```

Nenhum teste precisa de conta AWS. O motor Glue e a sessão do app são testados com um cliente falso em memória, que simula sessões provisionando, falhando, expirando ou ocupadas, além de statements com erro, cancelados ou com saída inválida. A estimativa do tamanho da sessão usa um catálogo, um S3 e um histórico do Athena falsos (os testes conferem que nenhum bucket de outra conta é chamado e que nada roda no Athena). O código PySpark gerado roda contra um Spark simulado e, no CI, contra um Spark local de verdade (inclusive o cache entre execuções e a leitura pelo GlueContext, com um GlueContext simulado). A interface é testada com o `AppTest` do próprio Streamlit, sem navegador: a tela inteira e o conteúdo das janelas de configuração, orquestração e Athena.

O GitHub Actions roda tudo a cada push: o verificador de código, os testes unitários e de tela em Python 3.10, 3.12 e 3.13, e os de Spark local com o mesmo Spark e Python de cada versão do Glue (4.0 com Spark 3.3, 5.x com Spark 3.5 e 6.0 com Spark 4.1).

## Estrutura

| Arquivo | Função |
|---|---|
| `app.py` | Entrada da interface: junta as partes da tela, na ordem |
| `ui/` | Partes da tela: barra lateral e sessão, janela de configuração, orquestração e estimativa da sessão, query e execução, resultado |
| `session_hub.py` | A sessão Spark do app: criada uma vez, conferida antes de cada execução e recriada se cair |
| `engines.py` | Motores: Glue Interactive Sessions e Spark local |
| `code_builder.py` | Gera o código PySpark a partir da orquestração e da query (cache entre execuções, leitura pelo GlueContext) e lê o resultado |
| `settings_store.py` | Lê, confere e grava o `configuracao.toml` |
| `athena_import.py` | Importa query do Athena: orquestração automática + tradução para Spark |
| `sizing.py` | Estimativa do tamanho da sessão (catálogo do Glue, S3 da sua conta e histórico do Athena) |
| `orchestration_io.py` | Formato de arquivo da orquestração (JSON) |
| `sql_refs.py` | Descobre as bases usadas pela query e confere com a orquestração |
| `spark_errors.py` | Resume os erros do Spark e do Java para a tela |
| `export.py` | Exportação do resultado em CSV (Excel em português, padrão, tabulação) |
| `metadata.py` | Dicionário de dados, tipos do Athena e DDL |
| `configuracao.exemplo.toml` | Modelo comentado do arquivo de configuração |
| `scripts/gerar_zip.py` | Gera `dist/spark-query-studio.zip` com o projeto completo |
| `tests/` | Testes (pytest); `fakes.py` tem o Spark, o GlueContext, o cliente Glue, o catálogo, o S3 e o histórico do Athena simulados |

## Próximos passos possíveis

- Gravar resultados maiores que o limite do Excel direto num bucket da própria conta (`df.write.parquet(...)`).
- Adicionar EMR Serverless como terceiro motor (mesma interface em `engines.py`).
- Histórico de queries executadas.

## Licença

MIT © 2026 Lucas Fracaro. Veja [LICENSE](LICENSE).
