"""
Teste local da LOGICA do pipeline (pandas, sem AWS e sem Spark).

Reproduz, passo a passo, as regras dos jobs para conferir os numeros esperados
antes de rodar no Glue:
  lote1 -> silver ; lote2 -> silver (incremental, como o Job Bookmark faria) ;
  gold calculada 2x a partir da mesma silver (prova de idempotencia).
Gera testes/resultado_teste_local.json, usado no relatorio como "valores esperados".

Uso: python3 testes/teste_local.py   (depois de rodar dados/gerar_dados.py)
"""
import hashlib
import json
import time
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
SAIDA = RAIZ / "dados" / "saida"


def ler_bronze(lote):
    arqs = sorted((SAIDA / lote / "bronze" / "faturas").rglob("*.json"))
    partes = []
    for a in arqs:
        d = pd.read_json(a, lines=True, dtype=False)
        d["ingest_date"] = a.parent.name.split("=")[1]
        partes.append(d)
    return pd.concat(partes, ignore_index=True), len(arqs), sum(a.stat().st_size for a in arqs)


def bronze_para_silver(lote):
    t0 = time.time()
    b, n_arq, n_bytes = ler_bronze(lote)
    for c in ("responsible", "responsible_email", "description"):
        if c not in b:
            b[c] = None
    df = pd.DataFrame({  # Change Schema
        "codigo_fatura": b["code"].astype("int64"),
        "data_fatura": pd.to_datetime(b["date"]),
        "item": b["item"],
        "preco_unitario": pd.to_numeric(b["price"]),
        "quantidade": b["quantity"].astype("Int64"),
        "codigo_aeronave": b["plane_code"].astype("Int64"),
        "criado_em": pd.to_datetime(b["created_at"]),
        "responsavel": b["responsible"],
        "email_responsavel": b["responsible_email"],
        "descricao": b["description"],
        "data_ingestao": b["ingest_date"],
    })
    df["chave_produto"] = df["item"].str.strip().str.lower()       # Derived Column
    df["ano"] = df["data_fatura"].dt.year
    df["mes"] = df["data_fatura"].dt.month
    ok = (df["preco_unitario"] > 0) & (df["quantidade"] > 0)
    quarentena, validos = df[~ok], df[ok]                           # SQL Query (quarentena)
    antes = len(validos)
    validos = validos.drop_duplicates("codigo_fatura")             # Drop Duplicates
    validos = validos.assign(email_responsavel=validos["email_responsavel"].map(  # hash SHA-256
        lambda e: hashlib.sha256(e.encode()).hexdigest() if isinstance(e, str) else None))
    res = {"lote": lote, "arquivos_bronze": n_arq, "mb_bronze": round(n_bytes / 2**20, 1),
           "kb_medio_arquivo": round(n_bytes / n_arq / 1024, 1), "registros_bronze": len(b),
           "quarentena": len(quarentena),
           "quarentena_preco_nulo": int(quarentena["preco_unitario"].isna().sum()),
           "quarentena_preco_negativo": int((quarentena["preco_unitario"] <= 0).sum()),
           "duplicados_removidos": antes - len(validos), "gravados_silver": len(validos),
           "particoes_ano_mes": int(validos.groupby(["ano", "mes"]).ngroups),
           "segundos": round(time.time() - t0, 1)}
    return validos, res


def dimensoes():
    p = pd.read_json(SAIDA / "lote1/bronze/produtos/produtos.json", lines=True, dtype=False)
    p = pd.DataFrame({"id_produto": p["id"], "nome_produto": p["product"], "categoria": p["category"],
                      "preco_lista": p["price"].astype(float), "disponivel": p["available"]})
    p["chave_produto"] = p["nome_produto"].str.strip().str.lower()
    a = pd.read_csv(SAIDA / "lote1/bronze/avioes/avioes.csv")
    a = a.rename(columns={"code": "codigo_aeronave", "plane": "modelo", "manufacturer": "fabricante"})
    return p, a


def gold(silver, prod, av):
    """Mesma regra de construir_modelo() (jobs/job_silver_gold_modelo.py)."""
    dim_p = pd.concat([prod.rename(columns={"id_produto": "sk_produto"}),
                       pd.DataFrame([{"sk_produto": -1, "chave_produto": "desconhecido",
                                      "nome_produto": "Produto não identificado",
                                      "categoria": "Não identificada"}])], ignore_index=True)
    dim_a = pd.concat([av.rename(columns={"codigo_aeronave": "sk_aeronave"}),
                       pd.DataFrame([{"sk_aeronave": -1, "modelo": "Aeronave não identificada",
                                      "fabricante": "Não identificado"}])], ignore_index=True)
    datas = pd.date_range(silver["data_fatura"].min(), silver["data_fatura"].max())
    dim_t = pd.DataFrame({"sk_data": datas.strftime("%Y%m%d").astype(int), "data": datas})
    f = silver.merge(dim_p[dim_p.sk_produto > 0][["sk_produto", "chave_produto"]], on="chave_produto", how="left")
    f["sk_aeronave"] = f["codigo_aeronave"].where(f["codigo_aeronave"].isin(av["codigo_aeronave"]), -1)
    f["sk_produto"] = f["sk_produto"].fillna(-1).astype(int)
    f["valor_total"] = (f["quantidade"].astype(float) * f["preco_unitario"]).round(2)
    fato = f[["codigo_fatura", "sk_produto", "sk_aeronave", "quantidade", "preco_unitario",
              "valor_total", "responsavel", "ano", "mes"]]
    agg = (fato.merge(dim_p[["sk_produto", "categoria"]], on="sk_produto")
               .merge(dim_a[["sk_aeronave", "fabricante"]], on="sk_aeronave")
               .groupby(["ano", "mes", "fabricante", "categoria"])
               .agg(qtd_faturas=("codigo_fatura", "nunique"), qtd_itens=("quantidade", "sum"),
                    valor_total=("valor_total", "sum")).reset_index())
    return {"dim_produto": dim_p, "dim_aeronave": dim_a, "dim_tempo": dim_t,
            "fato_faturas": fato, "agg_gastos_mensais": agg}


if __name__ == "__main__":
    r = {}
    prod, av = dimensoes()
    s1, r["lote1"] = bronze_para_silver("lote1")
    s2, r["lote2"] = bronze_para_silver("lote2")       # incremental: so arquivos novos
    silver = pd.concat([s1, s2], ignore_index=True)
    r["silver_total"] = len(silver)
    r["silver_codigos_unicos_entre_lotes"] = bool(silver["codigo_fatura"].is_unique)

    g1, g2 = gold(silver, prod, av), gold(silver, prod, av)
    fato = g1["fato_faturas"]
    r["gold"] = {n: len(d) for n, d in g1.items()}
    r["idempotente"] = bool(all(len(g1[n]) == len(g2[n]) for n in g1) and \
        round(g1["fato_faturas"]["valor_total"].sum(), 2) == round(g2["fato_faturas"]["valor_total"].sum(), 2))
    r["fato_sem_produto"] = int((fato.sk_produto == -1).sum())
    r["fato_sem_aeronave"] = int((fato.sk_aeronave == -1).sum())
    r["valor_total_fato"] = round(float(fato["valor_total"].sum()), 2)
    r["valor_total_agg"] = round(float(g1["agg_gastos_mensais"]["valor_total"].sum()), 2)
    r["dq_silver"] = {
        "RowCount": len(silver),
        "IsUnique codigo_fatura": bool(silver["codigo_fatura"].is_unique),
        "ColumnValues preco_unitario > 0": bool((silver["preco_unitario"] > 0).all()),
        "ColumnValues quantidade <= 100": bool((silver["quantidade"] <= 100).all()),
        "pct codigo_aeronave <= 30": round(float((silver["codigo_aeronave"] <= 30).mean() * 100), 3),
        "Completeness responsavel": round(float(silver["responsavel"].notna().mean()), 4),
    }
    r["exemplo_email_hash"] = silver.loc[silver["email_responsavel"].notna(), ["responsavel", "email_responsavel"]].iloc[0].to_dict()
    agg = g1["agg_gastos_mensais"]
    r["top_categorias"] = agg.groupby("categoria")["valor_total"].sum().round(2).sort_values(ascending=False).head(6).to_dict()
    r["top_fabricantes"] = agg.groupby("fabricante")["valor_total"].sum().round(2).sort_values(ascending=False).head(5).to_dict()
    r["gasto_mensal"] = {f"{a}-{m:02d}": round(v, 2) for (a, m), v in agg.groupby(["ano", "mes"])["valor_total"].sum().items()}
    r["faturas_por_mes"] = {f"{a}-{m:02d}": int(v) for (a, m), v in fato.groupby(["ano", "mes"]).size().items()}
    out = RAIZ / "testes" / "resultado_teste_local.json"
    out.write_text(json.dumps(r, indent=2, ensure_ascii=False, default=str))
    print(json.dumps(r, indent=2, ensure_ascii=False, default=str))
