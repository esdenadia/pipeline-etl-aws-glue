#!/usr/bin/env bash
# Automatiza as etapas de linha de comando do projeto. Requer AWS CLI v2 configurado (aws configure).
# Uso:  ./implantar.sh <etapa>
#   fundacao   -> cria a stack 01 (S3, IAM, Catalog, crawlers, Athena)
#   lote1      -> gera os dados (se preciso), envia lote 1 + scripts ao S3 e roda o crawler da Bronze
#   metadados  -> aplica descricoes/comentarios/propriedades no Data Catalog
#   orquestracao [true|false] -> cria a stack 02 (job Gold, workflow, gatilhos). false = cria jobs por script
#   rodar      -> inicia o workflow wf_mro_pipeline e acompanha
#   lote2      -> envia o lote 2 (jul-ago) para demonstrar o Job Bookmark; depois rode 'rodar' de novo
#   metricas   -> imprime tempo, DPU-segundos e status das ultimas execucoes (para o relatorio)
#   limpar     -> esvazia o bucket e apaga as duas stacks (evita custos depois da entrega)
set -euo pipefail

REGIAO="${AWS_REGION:-us-east-2}"
PREFIXO="${PREFIXO:-mro}"
STACK1="glue-pd-fundacao"
STACK2="glue-pd-orquestracao"
DIR="$(cd "$(dirname "$0")/.." && pwd)"

bucket() { aws cloudformation describe-stacks --region "$REGIAO" --stack-name "$STACK1" \
  --query "Stacks[0].Outputs[?OutputKey=='Bucket'].OutputValue" --output text; }

case "${1:-}" in
  fundacao)
    aws cloudformation deploy --region "$REGIAO" --stack-name "$STACK1" \
      --template-file "$DIR/infra/01_fundacao.yaml" --parameter-overrides Prefixo="$PREFIXO" \
      --capabilities CAPABILITY_NAMED_IAM
    echo "Bucket: $(bucket)";;
  lote1)
    B=$(bucket)
    [ -d "$DIR/dados/saida/lote1" ] || python3 "$DIR/dados/gerar_dados.py"
    aws s3 sync "$DIR/dados/saida/lote1/bronze/" "s3://$B/bronze/" --only-show-errors
    aws s3 cp "$DIR/jobs/" "s3://$B/scripts/" --recursive --exclude "*" --include "*.py" --only-show-errors
    aws glue start-crawler --region "$REGIAO" --name "crawler_${PREFIXO}_bronze"
    echo "Crawler iniciado. Acompanhe em Glue > Crawlers (leva ~2-3 min).";;
  metadados)
    python3 "$DIR/governanca/aplicar_metadados.py" --prefixo "$PREFIXO" --regiao "$REGIAO" ${2:-};;
  orquestracao)
    aws cloudformation deploy --region "$REGIAO" --stack-name "$STACK2" \
      --template-file "$DIR/infra/02_orquestracao.yaml" \
      --parameter-overrides StackFundacao="$STACK1" UsarJobsVisuais="${2:-true}";;
  rodar)
    RUN=$(aws glue start-workflow-run --region "$REGIAO" --name wf_mro_pipeline --query RunId --output text)
    echo "Workflow run: $RUN"
    while true; do
      S=$(aws glue get-workflow-run --region "$REGIAO" --name wf_mro_pipeline --run-id "$RUN" \
          --query "Run.[Status,Statistics.SucceededActions,Statistics.FailedActions,Statistics.RunningActions]" --output text)
      echo "$(date +%H:%M:%S)  status/sucesso/falha/rodando: $S"
      case "$S" in COMPLETED*|STOPPED*|ERROR*) break;; esac
      sleep 30
    done;;
  lote2)
    B=$(bucket)
    aws s3 sync "$DIR/dados/saida/lote2/bronze/" "s3://$B/bronze/" --only-show-errors
    echo "Lote 2 enviado. Rode './implantar.sh rodar' — os jobs devem ler SOMENTE os arquivos novos.";;
  metricas)
    for J in job_bronze_silver_faturas job_bronze_silver_dimensoes job_silver_gold_modelo; do
      echo "== $J"
      aws glue get-job-runs --region "$REGIAO" --job-name "$J" --max-items 5 \
        --query "JobRuns[].{inicio:StartedOn,status:JobRunState,seg:ExecutionTime,dpu_seg:DPUSeconds,workers:NumberOfWorkers}" \
        --output table || true
    done
    B=$(bucket)
    for C in bronze/faturas silver/faturas gold/fato_faturas; do
      echo "== s3://$B/$C"; aws s3 ls "s3://$B/$C/" --recursive --summarize | tail -2
    done;;
  limpar)
    B=$(bucket)
    aws s3 rm "s3://$B" --recursive --only-show-errors
    aws cloudformation delete-stack --region "$REGIAO" --stack-name "$STACK2"
    aws cloudformation wait stack-delete-complete --region "$REGIAO" --stack-name "$STACK2"
    aws cloudformation delete-stack --region "$REGIAO" --stack-name "$STACK1"
    echo "Lembre de apagar tambem os jobs visuais e o bucket aws-glue-assets-* se nao for usar mais.";;
  *) sed -n '2,13p' "$0";;
esac
