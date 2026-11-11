-- Consultas de validacao e negocio (Athena, workgroup mro_wg, banco mro_gold)
-- Tire print do resultado e do campo "Data scanned" de cada uma (evidencia de custo/particionamento).

-- 1) Volume por camada (conferir com o relatorio)
SELECT COUNT(*) AS linhas FROM mro_bronze.faturas;          -- esperado: 1.208.246 (apos lote 2)
SELECT COUNT(*) AS linhas FROM mro_silver.faturas;          -- observado: 1.198.606 (6 duplicadas entre cargas)
SELECT COUNT(*) AS linhas FROM mro_gold.fato_faturas;       -- observado: 1.198.606 (6 duplicadas entre cargas)

-- 2) Partition pruning: compare "Data scanned" desta consulta com a da consulta 3
SELECT SUM(valor_total) AS gasto_marco
FROM mro_gold.fato_faturas
WHERE ano = '2025' AND mes = '3';   -- particoes registradas como texto pelo crawler

-- 3) Mesma pergunta sem filtro de particao (le a tabela inteira)
SELECT SUM(valor_total) AS gasto_marco
FROM mro_gold.fato_faturas
WHERE sk_data BETWEEN 20250301 AND 20250331;

-- 4) Esquema estrela: gasto por fabricante e categoria no trimestre 2
SELECT a.fabricante, p.categoria,
       COUNT(*) AS faturas, ROUND(SUM(f.valor_total), 2) AS gasto
FROM mro_gold.fato_faturas f
JOIN mro_gold.dim_aeronave a ON f.sk_aeronave = a.sk_aeronave
JOIN mro_gold.dim_produto  p ON f.sk_produto  = p.sk_produto
JOIN mro_gold.dim_tempo    t ON f.sk_data     = t.sk_data
WHERE t.trimestre = 2
GROUP BY 1, 2
ORDER BY gasto DESC
LIMIT 15;

-- 5) Evolucao mensal a partir do agregado (consulta mais barata do modelo)
SELECT ano, mes, ROUND(SUM(valor_total), 2) AS gasto, SUM(qtd_faturas) AS faturas
FROM mro_gold.agg_gastos_mensais
GROUP BY 1, 2
ORDER BY 1, 2;

-- 6) Integridade referencial: faturas com aeronave nao identificada (membro -1)
SELECT COUNT(*) AS faturas_sem_aeronave
FROM mro_gold.fato_faturas
WHERE sk_aeronave = -1;                                     -- esperado: 2.383

-- 7) Quarentena: registros rejeitados pela qualidade (tabela criada pelo job visual)
SELECT motivo, COUNT(*) AS registros FROM mro_silver.quarentena_faturas GROUP BY motivo;  -- esperado: preco_nulo 4.799 / preco_nao_positivo 2.340

-- 8) Prova de PII protegida: o e-mail aparece apenas como hash
SELECT responsavel, email_responsavel_hash
FROM mro_gold.fato_faturas
WHERE email_responsavel_hash IS NOT NULL
LIMIT 5;
