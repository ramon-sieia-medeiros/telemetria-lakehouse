# Telemetria Lakehouse: pipeline de monitoramento de ativos elétricos
**Problema:** transformadores geram leituras de tensão, corrente e temperatura
com falhas reais (nulos, duplicados, sentinelas, picos). Como transformar
isso em dados confiáveis e decisões de manutenção?

**Stack:** 
Databricks Free Edition · Delta Lake · PySpark · SQL

**Arquitetura:** 
Landing (CSV) → Bronze (Delta) → Silver → Gold

**Status:** ✅ Fase 1 Bronze · ⬜ Silver · ⬜ Gold · ⬜ Orquestração · ⬜ Streaming
