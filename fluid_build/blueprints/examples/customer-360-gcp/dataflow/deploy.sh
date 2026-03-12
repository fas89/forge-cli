#!/bin/bash

# Customer 360 GCP Dataflow Pipeline Deployment Script
# This script deploys the real-time customer events processing pipeline to Google Cloud Dataflow

set -e

# Configuration
PROJECT_ID=${1:-"your-project-id"}
REGION=${2:-"us-central1"}
ENVIRONMENT=${3:-"dev"}

# Derived variables
JOB_NAME="customer-events-pipeline-${ENVIRONMENT}"
TEMP_LOCATION="gs://${PROJECT_ID}-dataflow-temp-${ENVIRONMENT}"
STAGING_LOCATION="gs://${PROJECT_ID}-dataflow-staging-${ENVIRONMENT}"
SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/customer-events-dataflow-${ENVIRONMENT}"
OUTPUT_TABLE="${PROJECT_ID}:customer360_raw_${ENVIRONMENT}.real_time_customer_events"
BACKUP_BUCKET="${PROJECT_ID}-customer-events-backup-${ENVIRONMENT}"

echo "Deploying Customer Events Dataflow Pipeline..."
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Environment: ${ENVIRONMENT}"
echo ""

# Check if required buckets exist, create if not
echo "Checking/Creating required GCS buckets..."
gsutil ls gs://${PROJECT_ID}-dataflow-temp-${ENVIRONMENT} >/dev/null 2>&1 || {
    echo "Creating temp bucket..."
    gsutil mb -l ${REGION} gs://${PROJECT_ID}-dataflow-temp-${ENVIRONMENT}
}

gsutil ls gs://${PROJECT_ID}-dataflow-staging-${ENVIRONMENT} >/dev/null 2>&1 || {
    echo "Creating staging bucket..."
    gsutil mb -l ${REGION} gs://${PROJECT_ID}-dataflow-staging-${ENVIRONMENT}
}

gsutil ls gs://${BACKUP_BUCKET} >/dev/null 2>&1 || {
    echo "Creating backup bucket..."
    gsutil mb -l ${REGION} gs://${BACKUP_BUCKET}
}

# Deploy the pipeline
echo "Deploying Dataflow pipeline..."
python customer_events_pipeline.py \
    --runner DataflowRunner \
    --project ${PROJECT_ID} \
    --region ${REGION} \
    --temp_location ${TEMP_LOCATION} \
    --staging_location ${STAGING_LOCATION} \
    --job_name ${JOB_NAME} \
    --input_subscription ${SUBSCRIPTION} \
    --output_table ${OUTPUT_TABLE} \
    --backup_bucket ${BACKUP_BUCKET} \
    --setup_file ./setup.py \
    --requirements_file ./requirements.txt \
    --machine_type n1-standard-2 \
    --max_num_workers 10 \
    --disk_size_gb 50 \
    --use_public_ips false \
    --streaming

echo ""
echo "Pipeline deployment initiated!"
echo "Job Name: ${JOB_NAME}"
echo "Monitor progress at: https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "To stop the pipeline:"
echo "gcloud dataflow jobs cancel ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"