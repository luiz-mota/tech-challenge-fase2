# Estimativa de Custos (FinOps)

Este documento compara o custo **real medido** ao rodar a pipeline uma vez (cobertura
nacional completa, 2023-2024) com uma projeção de custo operando em produção, em
escala mensal, por um ano. Os números de bytes processados/armazenados vêm
diretamente dos logs da pipeline (dry-run do BigQuery e tamanho real dos arquivos
Parquet), não são estimativas teóricas.

## 1. Custo real medido (uma execução completa)

| Recurso | Uso medido | Custo |
|---|---|---|
| BigQuery — bytes processados (6 tabelas, todos os municípios, 2 anos) | ~259 MB | R$ 0,00 |
| GCS — armazenamento (bronze + silver, batch + streaming) | ~206 MB | R$ 0,00 |
| BigQuery — armazenamento das 3 tabelas Gold | ~7 MB (10.896 linhas × 3 tabelas) | R$ 0,00 |

Tudo dentro do free tier do GCP (1 TB de consultas BigQuery/mês, 5 GB de
armazenamento GCS/mês, 10 GB de armazenamento BigQuery/mês). A tabela `alunos`
(microdados de ~3,87 milhões de alunos) domina o uso: 256 MB dos 259 MB
processados e 68 MB dos 206 MB armazenados — é o recurso a observar se o
programa crescer.

## 2. Projeção em escala: execução mensal, cobertura nacional, por 1 ano

Como a pipeline já extrai **todos os municípios do Brasil** em cada execução (não
uma amostra), "escalar" aqui significa rodar com mais frequência ao longo do
tempo, não processar mais municípios.

**BigQuery (consultas):**
12 execuções/ano × 259 MB ≈ 3,1 GB/ano processado — **0,3% do free tier mensal**
(1 TB). Continuaria a custo zero mesmo rodando diariamente (365 × 259 MB ≈ 92 GB/ano,
ainda dentro de 1 TB/mês).

**GCS (armazenamento — camada Bronze, particionada por `data_ingestao`):**
A Bronze retém histórico (uma cópia por execução, por definição de "raw sem
transformação"). 12 execuções/ano × ~69 MB (batch, sem contar streaming, que é
poucos KB) ≈ **828 MB/ano acumulado** — ainda abaixo do free tier de 5 GB.

**GCS (armazenamento — camada Silver):**
Silver não é particionada por data (é sobrescrita a cada execução, representa o
"estado atual limpo"), então seu armazenamento fica constante em ~69 MB,
independente da frequência de execução.

**BigQuery (armazenamento — camada Gold):**
Tabelas Gold também são sobrescritas (`WRITE_TRUNCATE`) a cada execução — ~7 MB
constantes, não acumula com o tempo.

### Conclusão da projeção

Rodando mensalmente por um ano inteiro, cobrindo todos os municípios do Brasil,
o custo projetado continua **R$ 0,00** — tanto BigQuery quanto GCS permanecem
dentro do free tier, com folga superior a 100x na maioria dos recursos.

**Ponto de atenção real**: se a cadência subir para diária (não é o caso hoje —
o INEP publica o indicador ~1-2x/ano) e a retenção de partições Bronze não for
gerenciada, o acúmulo de histórico eventualmente ultrapassaria os 5 GB grátis de
GCS por volta de ~2-3 meses de execuções diárias. A mitigação é uma **política de
ciclo de vida (lifecycle) no bucket GCS** para expirar partições Bronze antigas
(ex: manter só os últimos 90 dias), o que é a prática padrão de FinOps para
camadas raw/append-only.

## 3. Por que Parquet + particionamento reduzem custo (não é só formato)

- **Parquet é colunar**: no BigQuery, o custo de consulta é proporcional aos
  *bytes das colunas referenciadas* na query, não à tabela inteira. Um formato
  orientado a linha (CSV/JSON) forçaria a leitura de todas as colunas mesmo que
  a análise use só 2 ou 3 — no `alunos` (12 colunas), isso poderia multiplicar o
  custo por até ~4-6x dependendo da query.
- **Parquet é comprimido**: os 68 MB do `alunos.parquet` (3,87M linhas) seriam
  significativamente maiores em CSV — menos armazenamento, menos bytes
  transferidos, menos bytes processados.
- **Particionamento por `data_ingestao`** na Bronze permite: (a) processar
  incrementalmente só a partição nova em vez de reprocessar todo o histórico,
  e (b) aplicar políticas de retenção/expiração por partição sem afetar dados
  mais recentes.

## 4. Ressalva

Os valores de "free tier" e preços referenciados (BigQuery on-demand:
US$ 6,25/TiB além de 1 TB grátis/mês; GCS Standard: ~US$ 0,02-0,023/GB/mês na
região `southamerica-east1`) são os praticados publicamente pelo GCP no momento
da escrita deste documento — preços podem mudar; consulte a
[calculadora oficial do GCP](https://cloud.google.com/products/calculator) para
valores atualizados antes de decisões orçamentárias.
