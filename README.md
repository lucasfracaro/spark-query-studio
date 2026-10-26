# ⚡ Spark Query Studio

[![testes](https://github.com/lucasfracaro/spark-query-studio/actions/workflows/testes.yml/badge.svg)](https://github.com/lucasfracaro/spark-query-studio/actions/workflows/testes.yml)

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
.venv/bin/streamlit run app.py
```

O navegador abre em `http://localhost:8501`.

## Fluxo

1. **🔑 Configurar conta AWS** (barra lateral): região, autenticação (perfil do AWS CLI, chaves ou cadeia padrão/SSO) e o **ARN da IAM Role do Glue**. Use **Testar conexão** para conferir. As chaves de acesso ficam só na memória e nunca vão para o disco; o restante é salvo em `~/.spark_query_studio/config.json`.
2. **🖥️ Instância**: tipo de worker (G.1X, G.2X, …), número de workers, versão do Glue, idle timeout e argumentos extras do job (ex.: `--datalake-formats = iceberg`).
3. **Passo 1 – Orquestração**: cada linha de entrada vira uma *temp view*:
   - `catalog` + `database.tabela` → lê do Glue Data Catalog;
   - `s3` + `s3://bucket/caminho/` + formato + opções (`header=true;sep=;`).
   Também dá para aplicar um filtro antes de criar a view, ligar cache, definir configs `spark.*`, escolher um database padrão e o limite de linhas. A orquestração pode ser exportada ou importada em JSON.
4. **Passo 2 – Query**: escreva no editor ou importe um `.sql`/`.txt`. O conteúdo vai para o editor, onde pode ser alterado. Vários comandos separados por `;` são executados em ordem, e o resultado exibido é o do último.
5. **▶️ Executar**: na primeira vez a sessão Glue é criada (~30–90 s). As execuções seguintes reaproveitam a mesma sessão e levam segundos.
6. **Passo 3 – Resultado**: mostra as N primeiras linhas, o schema e o log. **Exportar CSV** baixa o arquivo para o seu PC; o separador `;` com UTF-8 BOM abre direto no Excel em português.

Use **Ver código PySpark gerado** para conferir exatamente o que vai rodar no cluster.

## Permissões na AWS

- **Seu usuário/role** (quem roda o app): `iam_user_policy.json` (criar/consultar sessões Glue e `iam:PassRole`).
- **Role do Glue** (assumida pela sessão): trust policy com `glue.amazonaws.com` + `iam_glue_role_policy.json` (S3 e Glue Catalog). Se as tabelas usam Lake Formation, conceda também SELECT a essa role.

Troque `SEU-BUCKET` e `SUA-CONTA` pelos valores reais.

## Custos

Uma Interactive Session é cobrada por DPU-hora enquanto estiver ativa (a barra lateral mostra os DPUs). Ela se encerra sozinha depois do *idle timeout*. Use **⏹️ Encerrar sessão** quando terminar.

## Modo Spark local

Para testar a interface sem gastar com a AWS, instale `pyspark` (requer Java 11/17) e escolha **Spark local**. Entradas `s3` funcionam com caminhos locais (ex.: `/dados/arquivo.parquet`). Para ler `s3a://` localmente, adicione `spark.jars.packages = org.apache.hadoop:hadoop-aws:3.3.4` nos argumentos extras.

## Testes

```bash
pip install -r requirements-dev.txt
pytest                                # tudo que estiver instalado
pip install pyspark && pytest tests/test_local_spark.py   # integração com Spark real (Java 17)
```

Nenhum teste precisa de conta AWS. O motor Glue é testado com um cliente falso em memória, que simula sessões provisionando, falhando ou expirando, além de statements com erro, cancelados ou com saída inválida. O código PySpark gerado roda contra um Spark simulado e, no CI, contra um Spark local de verdade. A interface é testada com o `AppTest` do próprio Streamlit, sem navegador.

O GitHub Actions roda tudo a cada push, em Python 3.10, 3.12 e 3.13.

## Estrutura

| Arquivo | Função |
|---|---|
| `app.py` | Interface (Streamlit) |
| `engines.py` | Motores: Glue Interactive Sessions e Spark local |
| `code_builder.py` | Gera o código PySpark a partir da orquestração e da query, e lê o resultado |
| `settings_store.py` | Salva e carrega a configuração (sem segredos) |
| `tests/` | Testes (pytest); `fakes.py` tem o Spark e o cliente Glue simulados |

## Próximos passos possíveis

- Exportar o resultado **completo** para `s3://` (`df.write.csv(...)`) além das 100 linhas.
- Adicionar EMR Serverless como terceiro motor (mesma interface em `engines.py`).
- Histórico de queries executadas.

## Licença

MIT © 2026 Lucas Fracaro. Veja [LICENSE](LICENSE).
