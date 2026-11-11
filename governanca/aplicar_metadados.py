#!/usr/bin/env python3
"""
Administracao de metadados tecnicos no Glue Data Catalog.

O que faz (idempotente, pode rodar quantas vezes quiser):
  1. Descricao de cada tabela + parametros de governanca (camada, dono, sensibilidade,
     frequencia, grao, fonte) em Table.Parameters.
  2. Comentario de cada coluna (dicionario de dados) em StorageDescriptor.Columns[].Comment.
  3. Na Bronze/faturas: groupFiles=inPartition e groupSize=128MB nas propriedades da tabela
     (o job visual le essas propriedades e agrupa os ~5.700 arquivos pequenos).
  4. (opcional, --lake-formation) Cria LF-Tags 'camada' e 'sensibilidade' e associa aos
     bancos/tabelas. Exige que voce seja administrador do Lake Formation.

Uso:
  python3 aplicar_metadados.py --prefixo mro --regiao us-east-2 [--lake-formation]
  (rode depois do crawler da Bronze e de novo depois da 1a execucao do workflow)
Requer: boto3 e credenciais AWS configuradas (aws configure).
"""
import argparse
import copy

import boto3
from botocore.exceptions import ClientError

TABELAS = {
    ("bronze", "faturas"): {
        "descricao": "Faturas brutas do ERP (JSON Lines, 1 arquivo por hora). Esquema variavel: description/responsible opcionais.",
        "params": {"grao": "1 linha por fatura (pode conter reenvios)", "fonte": "ERP compras MRO",
                   "frequencia": "horaria", "sensibilidade": "restrito", "contem_pii": "sim (responsible, responsible_email)",
                   "groupFiles": "inPartition", "groupSize": "134217728"},
        "colunas": {"code": "Codigo da fatura (chave natural)", "date": "Data da fatura (AAAA-MM-DD, texto)",
                    "item": "Nome do item comprado (texto livre, sem padronizacao)", "price": "Preco unitario (pode vir nulo/negativo)",
                    "quantity": "Quantidade comprada", "plane_code": "Codigo da aeronave atendida",
                    "created_at": "Data/hora de registro no ERP", "responsible": "Nome do responsavel (PII)",
                    "responsible_email": "E-mail do responsavel (PII)", "description": "Descricao livre (opcional)",
                    "ingest_date": "Particao: data de chegada do arquivo na Bronze"}},
    ("bronze", "produtos"): {
        "descricao": "Catalogo de produtos bruto (JSON Lines). Preco chega como texto.",
        "params": {"grao": "1 linha por produto", "fonte": "Cadastro de produtos", "frequencia": "sob demanda", "sensibilidade": "publico"},
        "colunas": {"id": "Id do produto", "product": "Nome do produto", "category": "Categoria",
                    "price": "Preco de lista (texto)", "available": "Disponivel para compra"}},
    ("bronze", "avioes"): {
        "descricao": "Cadastro de aeronaves (CSV com cabecalho).",
        "params": {"grao": "1 linha por aeronave", "fonte": "Cadastro de frota", "frequencia": "sob demanda", "sensibilidade": "publico"},
        "colunas": {"code": "Codigo da aeronave", "plane": "Modelo", "manufacturer": "Fabricante"}},
    ("silver", "faturas"): {
        "descricao": "Faturas limpas: tipadas, deduplicadas por codigo_fatura, e-mail em SHA-256, invalidas na quarentena. Parquet/Snappy particionado por ano/mes.",
        "params": {"grao": "1 linha por fatura valida", "sensibilidade": "interno", "contem_pii": "nome (e-mail pseudonimizado)",
                   "regras_dq": "governanca/regras_data_quality.dqdl", "carga": "incremental (Job Bookmark)"},
        "colunas": {"codigo_fatura": "Chave natural da fatura (unica)", "data_fatura": "Data da fatura",
                    "item": "Nome do item como veio da origem", "chave_produto": "lower(trim(item)) - chave de juncao com produtos",
                    "preco_unitario": "Preco unitario (> 0)", "quantidade": "Quantidade (1..100)",
                    "codigo_aeronave": "Codigo da aeronave", "criado_em": "Timestamp de registro no ERP",
                    "responsavel": "Nome do responsavel", "email_responsavel": "Hash SHA-256 do e-mail (pseudonimizado)",
                    "descricao": "Descricao livre", "data_ingestao": "Data de chegada na Bronze (linhagem)",
                    "ano": "Particao: ano da fatura", "mes": "Particao: mes da fatura"}},
    ("silver", "produtos"): {
        "descricao": "Produtos tipados (snapshots por data_carga).",
        "params": {"grao": "1 linha por produto por carga", "sensibilidade": "publico", "carga": "snapshot (Job Bookmark)"},
        "colunas": {"id_produto": "Id do produto", "nome_produto": "Nome", "categoria": "Categoria",
                    "preco_lista": "Preco de lista (double)", "disponivel": "Disponivel", "chave_produto": "Chave de juncao normalizada",
                    "data_carga": "Momento da carga (versao do snapshot)"}},
    ("silver", "avioes"): {
        "descricao": "Aeronaves tipadas (snapshots por data_carga).",
        "params": {"grao": "1 linha por aeronave por carga", "sensibilidade": "publico", "carga": "snapshot (Job Bookmark)"},
        "colunas": {"codigo_aeronave": "Codigo da aeronave", "modelo": "Modelo", "fabricante": "Fabricante",
                    "data_carga": "Momento da carga"}},
    ("gold", "fato_faturas"): {
        "descricao": "Fato de compras (esquema estrela). Grao: 1 linha por fatura. Recalculada a cada execucao (idempotente).",
        "params": {"tipo_modelo": "fato", "grao": "fatura", "sensibilidade": "interno"},
        "colunas": {"codigo_fatura": "Chave degenerada (fatura)", "sk_data": "FK dim_tempo (AAAAMMDD)",
                    "sk_produto": "FK dim_produto (-1 = nao identificado)", "sk_aeronave": "FK dim_aeronave (-1 = nao identificada)",
                    "quantidade": "Medida: quantidade", "preco_unitario": "Medida: preco unitario",
                    "valor_total": "Medida: quantidade x preco_unitario", "responsavel": "Atributo do responsavel",
                    "email_responsavel_hash": "E-mail pseudonimizado", "criado_em": "Timestamp de origem",
                    "ano": "Particao", "mes": "Particao"}},
    ("gold", "dim_produto"): {"descricao": "Dimensao Produto (ultimo snapshot + membro -1).",
                              "params": {"tipo_modelo": "dimensao", "scd": "tipo 1"}, "colunas": {}},
    ("gold", "dim_aeronave"): {"descricao": "Dimensao Aeronave (ultimo snapshot + membro -1).",
                               "params": {"tipo_modelo": "dimensao", "scd": "tipo 1"}, "colunas": {}},
    ("gold", "dim_tempo"): {"descricao": "Dimensao Tempo diaria gerada do intervalo dos fatos.",
                            "params": {"tipo_modelo": "dimensao"}, "colunas": {}},
    ("gold", "agg_gastos_mensais"): {"descricao": "Agregado mensal por fabricante e categoria (pronto para BI).",
                                     "params": {"tipo_modelo": "agregado", "grao": "ano x mes x fabricante x categoria"}, "colunas": {}},
}

CAMPOS_TABLE_INPUT = ["Name", "Description", "Owner", "Retention", "StorageDescriptor", "PartitionKeys",
                      "TableType", "Parameters", "ViewOriginalText", "ViewExpandedText", "TargetTable"]


def atualizar_tabela(glue, banco, tabela, meta):
    try:
        t = glue.get_table(DatabaseName=banco, Name=tabela)["Table"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            print(f"  - {banco}.{tabela}: ainda nao existe (rode o crawler/job e execute de novo)")
            return
        raise
    ti = {k: copy.deepcopy(t[k]) for k in CAMPOS_TABLE_INPUT if k in t}
    ti["Description"] = meta["descricao"]
    ti["Owner"] = "engenharia-dados"
    ti.setdefault("Parameters", {}).update(meta["params"])
    ti["Parameters"]["camada"] = banco.split("_")[-1]
    for col in ti.get("StorageDescriptor", {}).get("Columns", []) + ti.get("PartitionKeys", []):
        if col["Name"] in meta["colunas"]:
            col["Comment"] = meta["colunas"][col["Name"]]
    glue.update_table(DatabaseName=banco, TableInput=ti)
    print(f"  + {banco}.{tabela}: descricao, {len(meta['colunas'])} comentarios e parametros aplicados")


def lake_formation(lf, prefixo, conta):
    tags = {"camada": ["bronze", "silver", "gold"], "sensibilidade": ["publico", "interno", "restrito"]}
    for k, v in tags.items():
        try:
            lf.create_lf_tag(TagKey=k, TagValues=v)
            print(f"  + LF-Tag {k} criada")
        except ClientError as e:
            if e.response["Error"]["Code"] in ("AlreadyExistsException",):
                print(f"  = LF-Tag {k} ja existe")
            else:
                print(f"  ! LF-Tag {k}: {e.response['Error']['Message']}")
                return
    for camada, sens in (("bronze", "restrito"), ("silver", "interno"), ("gold", "interno")):
        try:
            lf.add_lf_tags_to_resource(
                Resource={"Database": {"CatalogId": conta, "Name": f"{prefixo}_{camada}"}},
                LFTags=[{"TagKey": "camada", "TagValues": [camada]}, {"TagKey": "sensibilidade", "TagValues": [sens]}])
            print(f"  + LF-Tags aplicadas em {prefixo}_{camada}")
        except ClientError as e:
            print(f"  ! {prefixo}_{camada}: {e.response['Error']['Message']}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefixo", default="mro")
    ap.add_argument("--regiao", default="us-east-2")
    ap.add_argument("--lake-formation", action="store_true")
    a = ap.parse_args()
    glue = boto3.client("glue", region_name=a.regiao)
    print("Aplicando metadados tecnicos no Glue Data Catalog...")
    for (camada, tabela), meta in TABELAS.items():
        atualizar_tabela(glue, f"{a.prefixo}_{camada}", tabela, meta)
    if a.lake_formation:
        conta = boto3.client("sts", region_name=a.regiao).get_caller_identity()["Account"]
        print("Aplicando LF-Tags (Lake Formation)...")
        lake_formation(boto3.client("lakeformation", region_name=a.regiao), a.prefixo, conta)
    print("Concluido.")
