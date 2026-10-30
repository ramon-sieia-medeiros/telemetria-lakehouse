# Fase 2: Camada Silver

Transformação da `bronze.telemetria_raw` em `silver.telemetria_clean`: dado tipado, sem duplicatas e com os problemas de qualidade tratados e sinalizados.

## Objetivo

Entregar uma tabela confiável para análise, sem perder rastreabilidade. A Silver **não apaga leituras ruins, ela as sinaliza**. Quem consome decide se filtra.

## Arquitetura

```
bronze.telemetria_raw  ──►  04_silver_transformacao  ──►  silver.telemetria_clean
   (1.055.250 linhas)                                      (1.050.000 linhas)
   tudo em string                       └──────────────►  silver.telemetria_quarentena
                                                           (chave inválida; 0 linhas)
```

- **Ferramentas:** PySpark, Delta Lake (`MERGE`), Databricks Free Edition
- **Chave da Silver:** `row_hash`, o SHA-256 da linha completa (6 colunas, nulos tratados como marcador)

## Notebooks

| Notebook | Função |
|---|---|
| `04_silver_transformacao` | Deduplicação, tipagem, flags de qualidade, quarentena e escrita idempotente |
| `05_validacao_silver` | Validação de contagem, unicidade, flags, tipos e quarentena |
| `06_sql_janelas` | SQL avançado (LAG, média móvel, gaps, RANK) sobre a Silver |

## Regras de qualidade

| Problema na Bronze | Tratamento na Silver |
|---|---|
| Todas as colunas em string | `try_cast` para `timestamp` e `double` |
| 5.250 duplicados exatos | Deduplicação pelo hash da linha completa |
| 2.112 correntes = -999 | Vira `NULL` e recebe a flag `corrente_sentinela` |
| 10.546 tensões nulas | Mantém `NULL` e recebe a flag `tensao_ausente` (sem imputar) |
| Temperatura > 100 °C | Mantém o dado e recebe a flag `temp_pico` |
| `device_id` ou `event_ts` inválido | Vai para `telemetria_quarentena` |

## Resultados: Bronze vs. Silver

| Métrica | Bronze | Silver |
|---|---|---|
| Linhas | 1.055.250 | 1.050.000 |
| Linhas distintas (`row_hash`) | 1.050.000 | 1.050.000 |
| Duplicados exatos | 5.250 | 0 |
| Tensão ausente | 10.546 | 10.500 (≈1,0%) |
| Corrente sentinela (-999) | 2.112 | 2.100 sinalizadas (≈0,2%) e 0 restantes no valor |
| Picos de temperatura (> 100 °C) | não medido | 3.150 (≈0,3%) |
| Linhas em quarentena | n/a | 0 |
| Tipos | tudo string | `timestamp`, `double`, `boolean`, `date` |

A diferença entre Bronze e Silver nas tensões ausentes (46) e nas correntes sentinela (12) vem de linhas que estavam repetidas entre os duplicados exatos.

## SQL de janelas (`06_sql_janelas`)

Análises feitas só com leituras sem pico de temperatura (`NOT temp_pico`), exceto o ranking de picos e os intervalos sem leitura.

| Consulta | Função de janela | Resultado |
|---|---|---|
| Maiores saltos de temperatura | `LAG` | Maior salto entre leituras consecutivas: 8,2 °C (TRF-002). Nenhum salto anômalo: o topo fica entre 7,5 e 8,2 °C |
| Média móvel de 10 leituras | `AVG ... ROWS BETWEEN 9 PRECEDING` | TRF-000 no primeiro dia: 3.000 leituras, média móvel em torno de 52,6 °C no início da madrugada |
| Maiores intervalos sem leitura | `LAG` sobre `event_ts` | Maior intervalo: 403 s (≈ 6,7 min, TRF-016). Nenhuma parada longa de sensor |
| Ranking de picos por transformador | `RANK` | TRF-027 lidera com 80 picos, seguido de TRF-042 e TRF-049 (77 cada). A média da frota é de 63 picos por transformador |

O ranking é a semente da Fase 3. Como os picos têm distribuição estreita (69 a 80 no top 10), a Gold deve combinar o número de picos com outros sinais antes de apontar um ativo como crítico.

## Aprendizados

- **Chave por linha completa.** `device_id + event_ts` não é único (colisões de timestamp). O hash das 6 colunas garantiu 1.050.000 linhas e 1.050.000 hashes distintos.
- **Modo ANSI.** No Databricks serverless, `cast` de valor inválido lança erro. O `try_cast` devolve `NULL` e o registro segue o fluxo normal, indo para a quarentena quando a chave é afetada.
- **Idempotência.** `MERGE` pelo `row_hash` na Silver e na quarentena: reexecutar o notebook não altera a contagem.
- **Sinalizar em vez de apagar.** Sentinela, tensão ausente e pico de temperatura viram flags, e o consumidor escolhe o filtro.
- **Empates em janelas.** Há leituras do mesmo dispositivo com o mesmo `event_ts`. Em `LAG` e na média móvel, a ordem entre elas não é determinística. Usar `ORDER BY event_ts, row_hash` como desempate deixa o resultado reproduzível. O efeito foi real: com o desempate, o maior salto de temperatura saiu de 8,4 °C (TRF-042) para 8,2 °C (TRF-002), e várias médias móveis de TRF-000 mudaram. Já os intervalos sem leitura não mudaram, porque dependem só do `event_ts`. No ranking, `RANK` repete a posição nos empates (2º, 2º e 4º lugares).
- **Notebook SQL.** O Databricks exibe apenas o resultado da última consulta de cada célula. Para ver todas, use uma consulta por célula.
- **Quarentena vazia é resultado válido.** Ela prova que a regra de chave inválida existe e foi testada.

## Próxima fase

**Fase 3: camada Gold.** A consulta de ranking de transformadores por picos de temperatura (`06_sql_janelas`) é a semente da tabela de ativos que pedem atenção.
