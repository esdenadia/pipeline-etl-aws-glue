#!/usr/bin/env python3
"""
Gerador do corpus do projeto (AWS Glue Studio - Pipelines ETL).

Amplia os tres arquivos de base fornecidos na disciplina:
  - base/Faturas.json  -> ~1,2 milhao de faturas (JSON Lines, esquema variavel)
  - base/Produtos.json -> catalogo de 60 produtos (JSON Lines, preco como texto)
  - base/Avioes.csv    -> 30 aeronaves (CSV, copiado sem alteracao)

Cenario: um centro de manutencao aeronautica (MRO) registra cada compra de
ferramenta/equipamento como uma fatura, vinculada a aeronave atendida.

Saida (layout de "landing" que vai para a camada Bronze do S3):
  saida/lote1/bronze/faturas/ingest_date=AAAA-MM-DD/faturas_AAAA-MM-DD_HH.json  (jan-jun/2025)
  saida/lote2/bronze/faturas/ingest_date=AAAA-MM-DD/faturas_AAAA-MM-DD_HH.json  (jul-ago/2025)
  saida/lote1/bronze/produtos/produtos.json
  saida/lote1/bronze/avioes/avioes.csv

Os arquivos sao propositalmente PEQUENOS (1 por hora => ~5.800 arquivos de ~45 KB)
para reproduzir o problema de "muitos arquivos pequenos" tratado no Glue.
O lote 2 existe para demonstrar o processamento incremental com Job Bookmarks.

Tambem sao injetados registros "sujos" (para as regras de Data Quality):
  ~0,4% preco nulo, ~0,2% preco negativo, ~0,2% codigo duplicado,
  ~0,2% aeronave inexistente, nomes de item com espacos/caixa diferentes.

Uso:  python3 gerar_dados.py [--faturas-por-dia 5900] [--saida saida]
Sem dependencias externas (apenas biblioteca padrao). Reprodutivel (seed fixa).
"""
import argparse
import csv
import json
import random
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent / "base"

# Produtos novos (id 15..60) para completar o catalogo. Os ids 1-4 vem de
# Produtos.json e os ids 5-14 sao os itens que aparecem em Faturas.json.
CATALOGO_EXTRA = [
    ("Torquímetro de Estalo", "Medição", 389.90), ("Paquímetro Digital", "Medição", 145.00),
    ("Micrômetro Externo", "Medição", 210.50), ("Multímetro Digital", "Eletrônicos", 189.90),
    ("Boroscópio Industrial", "Eletrônicos", 2450.00), ("Calibrador de Pressão", "Medição", 640.00),
    ("Lanterna de Inspeção LED", "Eletrônicos", 89.90), ("Chave Torx", "Ferramentas Manuais", 34.90),
    ("Chave Allen", "Ferramentas Manuais", 28.50), ("Soquete Sextavado", "Ferramentas Manuais", 42.00),
    ("Catraca 1/2 Polegada", "Ferramentas Manuais", 96.00), ("Rebitadeira Pneumática", "Ferramentas Pneumáticas", 780.00),
    ("Furadeira Pneumática", "Ferramentas Pneumáticas", 920.00), ("Lixadeira Orbital", "Ferramentas Pneumáticas", 540.00),
    ("Compressor Portátil", "Ferramentas Pneumáticas", 1890.00), ("Arame de Frenagem", "Consumíveis", 19.90),
    ("Selante Aeronáutico", "Consumíveis", 149.00), ("Graxa Aeronáutica", "Consumíveis", 79.90),
    ("Fita de Alumínio", "Consumíveis", 39.90), ("Rebites de Alumínio", "Consumíveis", 59.00),
    ("Abraçadeira de Nylon", "Consumíveis", 14.90), ("Pano de Limpeza", "Consumíveis", 9.90),
    ("Protetor Auricular", "EPI", 12.50), ("Capacete de Segurança", "EPI", 48.00),
    ("Botina de Segurança", "EPI", 159.00), ("Cinto Paraquedista", "EPI", 289.00),
    ("Máscara Respiratória", "EPI", 65.00), ("Luvas Nitrílicas", "EPI", 32.00),
    ("Cabo de Teste", "Elétrica", 27.90), ("Terminal Elétrico", "Elétrica", 18.00),
    ("Alicate Crimpador", "Elétrica", 135.00), ("Ferro de Solda", "Elétrica", 99.90),
    ("Testador de Continuidade", "Elétrica", 74.90), ("Conector Aeronáutico", "Elétrica", 210.00),
    ("Headset de Comunicação", "Acessórios", 450.00), ("Carrinho de Ferramentas", "Acessórios", 1290.00),
    ("Maleta de Ferramentas", "Acessórios", 320.00), ("Escada de Manutenção", "Acessórios", 1750.00),
    ("Macaco Hidráulico", "Equipamentos de Solo", 3200.00), ("Barra de Reboque", "Equipamentos de Solo", 4100.00),
    ("Calço de Roda", "Equipamentos de Solo", 260.00), ("Fonte de Energia Externa", "Equipamentos de Solo", 15800.00),
    ("Tablet Robusto", "Eletrônicos", 3890.00), ("Leitor de Código de Barras", "Eletrônicos", 690.00),
    ("Impressora de Etiquetas", "Eletrônicos", 980.00), ("Câmera Térmica", "Eletrônicos", 5400.00),
]

CATEGORIA_FATURAS = {
    "Martelo": "Ferramentas Manuais", "Jogo de Chaves de Fenda": "Ferramentas Manuais",
    "Chave Inglesa": "Ferramentas Manuais", "Alicate": "Ferramentas Manuais",
    "Trena": "Medição", "Brocas": "Ferramentas Manuais", "Nível": "Medição",
    "Estilete": "Ferramentas Manuais", "Óculos de Proteção": "EPI", "Luvas": "EPI",
}

NOMES = ["Ana Souza", "Bruno Lima", "Carla Mendes", "Diego Rocha", "Elisa Martins",
         "Felipe Araújo", "Gabriela Nunes", "Henrique Costa", "Isabela Ferreira",
         "João Pereira", "Karina Alves", "Lucas Barbosa", "Mariana Ribeiro",
         "Nicolas Teixeira", "Olivia Cardoso", "Paulo Gomes", "Renata Dias",
         "Sérgio Moreira", "Tatiana Freitas", "Vinícius Ramos",
         # nomes que ja aparecem em Faturas.json
         "John Smith", "Jane Doe", "Peter Jones", "Mary Williams"]


def email_de(nome: str) -> str:
    tabela = str.maketrans("áàãâéêíóôõúçÁÉÍÓÚÇ", "aaaaeeiooouc" + "AEIOUC")
    usuario = nome.translate(tabela).lower().replace(" ", ".")
    return f"{usuario}@mro-exemplo.com.br"


def montar_catalogo():
    produtos = [json.loads(l) for l in (BASE / "Produtos.json").read_text(encoding="utf-8").splitlines() if l.strip()]
    faturas_base = [json.loads(l) for l in (BASE / "Faturas.json").read_text(encoding="utf-8").splitlines() if l.strip()]
    prox = max(p["id"] for p in produtos) + 1
    for f in faturas_base:
        produtos.append({"id": prox, "product": f["item"], "category": CATEGORIA_FATURAS[f["item"]],
                         "price": f"{f['price']:.2f}", "available": True})
        prox += 1
    for nome, cat, preco in CATALOGO_EXTRA:
        produtos.append({"id": prox, "product": nome, "category": cat,
                         "price": f"{preco:.2f}", "available": random.random() > 0.1})
        prox += 1
    descricoes = {f["item"]: f["description"] for f in faturas_base if "description" in f}
    return produtos, faturas_base, descricoes


def variar_nome(nome: str, rnd: random.Random) -> str:
    """Simula digitacao inconsistente na origem (resolvido na camada Silver)."""
    r = rnd.random()
    if r < 0.03:
        return f"  {nome} "
    if r < 0.05:
        return nome.upper()
    if r < 0.07:
        return nome.lower()
    return nome


def gerar(saida: Path, por_dia: int, seed: int = 42):
    rnd = random.Random(seed)
    random.seed(seed)
    produtos, faturas_base, descricoes = montar_catalogo()
    with open(BASE / "Avioes.csv", encoding="utf-8") as fh:
        avioes = [int(r["code"]) for r in csv.DictReader(fh)]

    if saida.exists():
        shutil.rmtree(saida)

    # Dimensoes (vao no lote 1)
    d_prod = saida / "lote1" / "bronze" / "produtos"
    d_av = saida / "lote1" / "bronze" / "avioes"
    d_prod.mkdir(parents=True)
    d_av.mkdir(parents=True)
    with open(d_prod / "produtos.json", "w", encoding="utf-8") as fh:
        for p in produtos:
            fh.write(json.dumps(p, ensure_ascii=False) + "\n")
    shutil.copy(BASE / "Avioes.csv", d_av / "avioes.csv")

    # Peso de compra por produto (consumiveis/EPI saem mais)
    pesos = []
    for p in produtos:
        cat = p["category"]
        pesos.append({"Consumíveis": 8, "EPI": 6, "Ferramentas Manuais": 5, "Elétrica": 4,
                      "Medição": 3, "Acessórios": 2, "Ferramentas Pneumáticas": 1.5,
                      "Eletrônicos": 1.5, "Equipamentos de Solo": 0.4}.get(cat, 1))

    # As 10 faturas originais entram como estao (code 1..10)
    code = 1
    stats = {"faturas": 0, "arquivos": 0, "bytes": 0}
    inicio, fim, corte = date(2025, 1, 1), date(2025, 8, 31), date(2025, 7, 1)
    originais = {f["date"]: [] for f in faturas_base}
    for f in faturas_base:
        originais[f["date"]].append(f)

    dia = inicio
    while dia <= fim:
        lote = "lote1" if dia < corte else "lote2"
        pasta = saida / lote / "bronze" / "faturas" / f"ingest_date={dia.isoformat()}"
        pasta.mkdir(parents=True)
        # Sazonalidade: menos compras no fim de semana, crescimento ao longo do ano
        fator = (0.45 if dia.weekday() >= 5 else 1.0) * (0.9 + 0.2 * (dia - inicio).days / (fim - inicio).days)
        n_dia = int(por_dia * fator * rnd.uniform(0.9, 1.1))
        registros_hora = [[] for _ in range(24)]

        for f in originais.get(dia.isoformat(), []):
            reg = dict(f)
            reg["code"] = code
            reg["plane_code"] = rnd.choice(avioes)
            reg["quantity"] = 1
            reg["created_at"] = f"{dia.isoformat()}T09:00:00"
            registros_hora[9].append(reg)
            code += 1

        ultimo = None
        for _ in range(n_dia):
            hora = min(23, max(0, int(rnd.gauss(13, 3.5))))
            p = rnd.choices(produtos, weights=pesos)[0]
            preco_base = float(p["price"])
            reg = {
                "code": code,
                "date": dia.isoformat(),
                "item": variar_nome(p["product"], rnd),
                "price": round(preco_base * rnd.uniform(0.92, 1.12), 2),
                "quantity": (rnd.choices([1, 2], weights=[85, 15])[0] if preco_base > 500 else
                             rnd.choices([1, 2, 3, 4, 5, 10, 20], weights=[50, 20, 10, 6, 6, 5, 3])[0]),
                "plane_code": rnd.choice(avioes),
                "created_at": f"{dia.isoformat()}T{hora:02d}:{rnd.randint(0,59):02d}:{rnd.randint(0,59):02d}",
            }
            # Campos opcionais (esquema variavel, como no Faturas.json original)
            if rnd.random() < 0.55:
                nome = rnd.choice(NOMES)
                reg["responsible"] = nome
                if rnd.random() < 0.8:
                    reg["responsible_email"] = email_de(nome)  # dado pessoal -> mascarar
            if p["product"] in descricoes and rnd.random() < 0.6:
                reg["description"] = descricoes[p["product"]]
            elif rnd.random() < 0.15:
                reg["description"] = f"Reposição de {p['product'].lower()} para o hangar {rnd.randint(1, 6)}"
            # Registros sujos
            r = rnd.random()
            if r < 0.004:
                reg["price"] = None
            elif r < 0.006:
                reg["price"] = -abs(reg["price"])
            elif r < 0.008:
                reg["plane_code"] = 99  # aeronave inexistente na dimensao
            code += 1
            registros_hora[hora].append(reg)
            # ~0,2% de reenvio duplicado do mesmo registro (mesmo code)
            if ultimo is not None and rnd.random() < 0.002:
                registros_hora[hora].append(dict(ultimo))
            ultimo = reg

        for h, regs in enumerate(registros_hora):
            if not regs:
                continue
            arq = pasta / f"faturas_{dia.isoformat()}_{h:02d}.json"
            with open(arq, "w", encoding="utf-8") as fh:
                for reg in regs:
                    fh.write(json.dumps(reg, ensure_ascii=False) + "\n")
            stats["faturas"] += len(regs)
            stats["arquivos"] += 1
            stats["bytes"] += arq.stat().st_size
        dia += timedelta(days=1)

    stats["produtos"] = len(produtos)
    stats["avioes"] = len(avioes)
    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--faturas-por-dia", type=int, default=5900)
    ap.add_argument("--saida", default=str(Path(__file__).resolve().parent / "saida"))
    a = ap.parse_args()
    s = gerar(Path(a.saida), a.faturas_por_dia)
    print(f"Faturas: {s['faturas']:,} em {s['arquivos']:,} arquivos "
          f"({s['bytes']/1024/1024:.1f} MB, media {s['bytes']/s['arquivos']/1024:.1f} KB/arquivo)")
    print(f"Produtos: {s['produtos']} | Aeronaves: {s['avioes']}")
