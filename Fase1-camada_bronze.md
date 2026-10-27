# Fase 1: Camada Bronze

Ingestão dos dados brutos de telemetria de transformadores no Databricks, preservando os dados exatamente como chegaram. Nenhuma limpeza é feita nesta fase, de propósito.

## Objetivo
Criar a base do lakehouse: gerar um dataset realista com falhas de qualidade, ingerir de forma incremental e idempotente em tabela Delta e medir os problemas que a Fase 2 vai resolver.

## Arquitetura da fase
```
Python (gerador) → CSV no Volume (landing) → Auto Loader → Delta (bronze.telemetria_raw)
```
- **Catálogo:** `project_lakehouse_telemetria_ele`
- **Schema:** `bronze`
- **Volume:** `landing`
- **Tabela:** `telemetria_raw`

## Notebooks
| Notebook | O que faz |
|---|---|
| `01_gerar_dados` | Gera 7 CSVs diários (50 transformadores) com tensão, corrente, temperatura e alarme, incluindo falhas propositais |
| `02_ingestao_bronze` | Ingestão incremental com Auto Loader (`availableNow`), colunas de rastreio `_ingest_ts` e `_source_file`, tudo como string |
| `03_exploracao_sql` | Consultas de sanidade: volume, nulos, sentinelas, duplicados e curva de carga |

## Decisões de projeto
- **Bronze guarda o dado bruto:** colunas como string, sem tipagem nem filtro. Se a regra de limpeza mudar, o dado original continua disponível.
- **Idempotência:** o checkpoint do Auto Loader impede reprocessar arquivos. Rodar o notebook duas vezes não altera a contagem.
- **Rastreabilidade:** cada linha sabe de qual arquivo veio e quando foi ingerida.
- **Falhas injetadas de propósito:** simulam problemas reais de campo, como falha de sensor, retransmissão e picos.

## Resultados
| Métrica | Valor |
|---|---|
| Linhas na Bronze | 1.055.250 |
| Linhas distintas (todas as colunas) | 1.050.000 |
| Arquivos ingeridos | 7 |
| Leituras com tensão nula | 10.546 (≈1,0%) |
| Leituras com corrente sentinela (-999) | 2.112 (≈0,2%) |
| Duplicados exatos (linha idêntica) | 5.250 (≈0,5%) |
| Chaves (device_id, event_ts) repetidas | 23.238 (≈2,2%, inclui colisões de timestamp) |
| Reingestão (2ª execução) | contagem inalterada ✅ |

**Validação física:** a temperatura média por hora segue a curva de carga esperada, com mínimo de 52,7 °C à 0h e pico de 67,7 °C às 12h.

## Aprendizados da fase
1. **Tipagem importa:** o valor sentinela `-999` chega como `-999.0` em texto. A comparação direta com `'-999'` retornou 0, e só o `CAST` para `DOUBLE` revelou as 2.112 ocorrências.
2. **A chave de deduplicação importa:** `device_id + event_ts` não é chave única, porque leituras diferentes do mesmo dispositivo podem cair no mesmo segundo. Usá-la apontaria 23.238 duplicados quando os reais são 5.250 (≈4,4x a mais), descartando leituras válidas.

## Problemas de qualidade identificados (entrada da Fase 2)
1. Nulos em tensão (10.546)
2. Valor sentinela `-999.0` em corrente (2.112)
3. Duplicados exatos (5.250), a serem removidos por linha completa
4. Picos de temperatura acima de 100 °C (alarme `SOBRETEMP`)
5. Tipos ainda em string (timestamp e numéricos)

## Próxima fase
**Fase 2: camada Silver.** Tipagem, deduplicação por linha completa, tratamento de nulos e sentinelas, regras de qualidade e SQL avançado com janelas.

## Como reproduzir
1. Criar o catálogo, os schemas e o volume (SQL no README principal).
2. Rodar os notebooks em ordem: `01` → `02` → `03`.
