#!/bin/bash

# FLUID Customer 360 GCP - One-Command Deployment Script
# This script deploys the complete Customer 360 analytics platform with minimal commands

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID=${1:-""}
REGION=${2:-"us-central1"}
ENVIRONMENT=${3:-"dev"}

print_header() {
    echo -e "${BLUE}"
    echo "================================================================================================"
    echo "  FLUID Customer 360 GCP - Enterprise Analytics Platform Deployment"
    echo "================================================================================================"
    echo -e "${NC}"
}

print_step() {
    echo -e "${GREEN}>>> $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

print_error() {
    echo -e "${RED}ERROR: $1${NC}"
}

check_prerequisites() {
    print_step "Checking prerequisites..."
    
    # Check if PROJECT_ID is provided
    if [ -z "$PROJECT_ID" ]; then
        print_error "Project ID is required!"
        echo "Usage: $0 <PROJECT_ID> [REGION] [ENVIRONMENT]"
        echo "Example: $0 my-gcp-project us-central1 dev"
        exit 1
    fi
    
    # Check required tools
    local tools=("gcloud" "terraform" "dbt" "fluid")
    for tool in "${tools[@]}"; do
        if ! command -v $tool &> /dev/null; then
            print_error "$tool is not installed or not in PATH"
            exit 1
        fi
    done
    
    # Check gcloud authentication
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null; then
        print_error "No active gcloud authentication found"
        echo "Please run: gcloud auth login"
        exit 1
    fi
    
    # Set gcloud project
    gcloud config set project $PROJECT_ID
    
    echo -e "${GREEN}✓ All prerequisites met${NC}"
}

enable_apis() {
    print_step "Enabling required GCP APIs..."
    
    local apis=(
        "bigquery.googleapis.com"
        "bigqueryml.googleapis.com"
        "storage.googleapis.com"
        "pubsub.googleapis.com"
        "dataflow.googleapis.com"
        "cloudfunctions.googleapis.com"
        "cloudscheduler.googleapis.com"
        "secretmanager.googleapis.com"
    )
    
    for api in "${apis[@]}"; do
        echo "Enabling $api..."
        gcloud services enable $api --project=$PROJECT_ID
    done
    
    echo -e "${GREEN}✓ APIs enabled${NC}"
}

deploy_infrastructure() {
    print_step "Deploying infrastructure with Terraform..."
    
    cd terraform
    
    # Create terraform.tfvars if it doesn't exist
    if [ ! -f terraform.tfvars ]; then
        print_step "Creating terraform.tfvars..."
        cat > terraform.tfvars << EOF
project_id        = "$PROJECT_ID"
region           = "$REGION"
bigquery_location = "US"
environment      = "$ENVIRONMENT"
EOF
    fi
    
    # Initialize and deploy
    terraform init
    terraform plan -out=tfplan
    terraform apply tfplan
    
    cd ..
    echo -e "${GREEN}✓ Infrastructure deployed${NC}"
}

setup_dbt_profile() {
    print_step "Setting up dbt profile..."
    
    # Create dbt profiles directory if it doesn't exist
    mkdir -p ~/.dbt
    
    # Create or update profiles.yml
    cat > ~/.dbt/profiles.yml << EOF
customer_360_gcp:
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: $PROJECT_ID
      dataset: customer360_staging_$ENVIRONMENT
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
    
    prod:
      type: bigquery
      method: oauth
      project: $PROJECT_ID
      dataset: customer360_staging_prod
      threads: 8
      timeout_seconds: 300
      location: US
      priority: interactive
  
  target: $ENVIRONMENT
EOF
    
    echo -e "${GREEN}✓ dbt profile configured${NC}"
}

deploy_analytics() {
    print_step "Deploying analytics models with dbt..."
    
    cd dbt_project
    
    # Install dependencies
    dbt deps
    
    # Test connection
    dbt debug
    
    # Run models in proper order
    print_step "Running staging models..."
    dbt run --models staging
    
    print_step "Running intermediate models..."
    dbt run --models intermediate
    
    print_step "Running marts models..."
    dbt run --models marts
    
    print_step "Training ML models..."
    dbt run --models ml
    
    print_step "Running data quality tests..."
    dbt test
    
    print_step "Generating documentation..."
    dbt docs generate
    
    cd ..
    echo -e "${GREEN}✓ Analytics models deployed${NC}"
}

deploy_streaming() {
    print_step "Deploying real-time streaming pipeline..."
    
    cd dataflow
    
    # Make deployment script executable
    chmod +x deploy.sh
    
    # Deploy Dataflow pipeline
    ./deploy.sh $PROJECT_ID $REGION $ENVIRONMENT
    
    cd ..
    echo -e "${GREEN}✓ Streaming pipeline deployed${NC}"
}

export_to_opds() {
    print_step "Exporting FLUID contract to Open Data Product Specification (OPDS)..."
    
    # Create exports directory if it doesn't exist
    mkdir -p exports
    
    # Export contract to OPDS format
    fluid export-opds contract.fluid.yaml --output exports/customer-360-gcp.opds.json --format json
    
    # Also export to YAML format
    fluid export-opds contract.fluid.yaml --output exports/customer-360-gcp.opds.yaml --format yaml
    
    print_step "OPDS exports created:"
    echo "  - exports/customer-360-gcp.opds.json"
    echo "  - exports/customer-360-gcp.opds.yaml"
    
    echo -e "${GREEN}✓ OPDS export completed${NC}"
}

load_sample_data() {
    print_step "Loading sample data..."
    
    cd sample_data
    
    # Load customer data
    if [ -f customers.csv ]; then
        print_step "Loading customer data..."
        bq load \
            --source_format=CSV \
            --skip_leading_rows=1 \
            --replace \
            ${PROJECT_ID}:customer360_raw_${ENVIRONMENT}.crm_customers \
            customers.csv \
            customer_id:STRING,email:STRING,first_name:STRING,last_name:STRING,phone:STRING,address_line_1:STRING,city:STRING,state:STRING,zip_code:STRING,country:STRING,date_of_birth:DATE,gender:STRING,customer_since:DATE,customer_status:STRING,marketing_opt_in:BOOLEAN,created_at:TIMESTAMP,updated_at:TIMESTAMP
    fi
    
    # Load transaction data
    if [ -f transactions.csv ]; then
        print_step "Loading transaction data..."
        bq load \
            --source_format=CSV \
            --skip_leading_rows=1 \
            --replace \
            ${PROJECT_ID}:customer360_raw_${ENVIRONMENT}.transactions \
            transactions.csv \
            transaction_id:STRING,customer_id:STRING,order_id:STRING,transaction_date:DATE,transaction_timestamp:TIMESTAMP,amount:FLOAT,currency:STRING,payment_method:STRING,transaction_type:STRING,status:STRING,product_category:STRING,product_id:STRING,quantity:INTEGER,unit_price:FLOAT,channel:STRING,created_at:TIMESTAMP,updated_at:TIMESTAMP
    fi
    
    cd ..
    echo -e "${GREEN}✓ Sample data loaded${NC}"
}

run_end_to_end_test() {
    print_step "Running end-to-end pipeline test..."
    
    cd dbt_project
    
    # Run complete pipeline
    dbt run
    dbt test
    
    # Test key queries
    print_step "Testing customer segmentation..."
    bq query --use_legacy_sql=false --format=table \
        "SELECT customer_segment, COUNT(*) as count 
         FROM \`${PROJECT_ID}.customer360_marts_${ENVIRONMENT}.customer_profiles_enterprise\` 
         GROUP BY customer_segment 
         ORDER BY count DESC 
         LIMIT 10"
    
    print_step "Testing cohort analysis..."
    bq query --use_legacy_sql=false --format=table \
        "SELECT cohort_month, period_number, retention_percentage 
         FROM \`${PROJECT_ID}.customer360_marts_${ENVIRONMENT}.customer_cohort_analysis\` 
         WHERE period_number <= 6 
         ORDER BY cohort_month DESC, period_number 
         LIMIT 10"
    
    cd ..
    echo -e "${GREEN}✓ End-to-end test completed${NC}"
}

generate_deployment_report() {
    print_step "Generating deployment report..."
    
    cat > deployment_report.md << EOF
# Customer 360 GCP Deployment Report

**Deployment Date:** $(date)
**Project ID:** $PROJECT_ID
**Region:** $REGION
**Environment:** $ENVIRONMENT

## Deployed Components

### Infrastructure
- ✅ BigQuery datasets: customer360_raw_$ENVIRONMENT, customer360_staging_$ENVIRONMENT, customer360_marts_$ENVIRONMENT, customer360_ml_$ENVIRONMENT
- ✅ Pub/Sub topics: customer-events-$ENVIRONMENT
- ✅ Cloud Storage buckets: ${PROJECT_ID}-customer-events-raw-$ENVIRONMENT
- ✅ Dataflow pipeline: customer-events-pipeline-$ENVIRONMENT
- ✅ Service accounts and IAM roles

### Analytics Models
- ✅ Staging models: Customer and transaction data cleaning
- ✅ Intermediate models: RFM analysis and cohort calculations
- ✅ Marts models: Customer profiles and cohort analysis
- ✅ ML models: Churn prediction and CLV forecasting

### Data Products
- ✅ Customer Profiles Enterprise
- ✅ Real-time Customer Events
- ✅ Customer Cohort Analysis

### OPDS Exports
- ✅ JSON format: exports/customer-360-gcp.opds.json
- ✅ YAML format: exports/customer-360-gcp.opds.yaml

## Access URLs

- **BigQuery Console:** https://console.cloud.google.com/bigquery?project=$PROJECT_ID
- **Dataflow Console:** https://console.cloud.google.com/dataflow?project=$PROJECT_ID
- **Pub/Sub Console:** https://console.cloud.google.com/cloudpubsub?project=$PROJECT_ID

## Next Steps

1. **Explore Data Products:**
   \`\`\`sql
   -- View customer segments
   SELECT customer_segment, COUNT(*) as customers
   FROM \`$PROJECT_ID.customer360_marts_$ENVIRONMENT.customer_profiles_enterprise\`
   GROUP BY customer_segment;
   
   -- View cohort retention
   SELECT cohort_month, period_number, retention_percentage
   FROM \`$PROJECT_ID.customer360_marts_$ENVIRONMENT.customer_cohort_analysis\`
   WHERE period_number <= 12;
   \`\`\`

2. **Send Test Events:**
   \`\`\`bash
   gcloud pubsub topics publish customer-events-$ENVIRONMENT \\
     --message-body='{"event_id":"test-001","customer_id":"CUST001","event_type":"PURCHASE","amount":99.99}'
   \`\`\`

3. **Monitor Pipeline:**
   \`\`\`bash
   # Check Dataflow job status
   gcloud dataflow jobs list --region=$REGION
   
   # View dbt documentation
   cd dbt_project && dbt docs serve
   \`\`\`

4. **Customize for Your Business:**
   - Modify customer segments in \`models/intermediate/int_customer_rfm.sql\`
   - Add new data sources in \`terraform/main.tf\`
   - Extend ML models in \`models/ml/\`

## Cost Monitoring

- Set up billing alerts in GCP Console
- Monitor BigQuery slot usage
- Review Dataflow worker utilization
- Optimize queries based on performance insights

## Support

- Review README.md for detailed documentation
- Check \`docs/blueprints/\` for architecture guides
- Use \`fluid blueprint describe customer-360-gcp\` for help
EOF
    
    echo -e "${GREEN}✓ Deployment report generated: deployment_report.md${NC}"
}

main() {
    print_header
    
    echo "Project ID: $PROJECT_ID"
    echo "Region: $REGION"
    echo "Environment: $ENVIRONMENT"
    echo ""
    
    # Main deployment steps
    check_prerequisites
    enable_apis
    deploy_infrastructure
    setup_dbt_profile
    deploy_analytics
    deploy_streaming
    export_to_opds
    load_sample_data
    run_end_to_end_test
    generate_deployment_report
    
    print_header
    echo -e "${GREEN}🎉 DEPLOYMENT COMPLETED SUCCESSFULLY! 🎉${NC}"
    echo ""
    echo -e "${BLUE}Your Customer 360 analytics platform is now ready!${NC}"
    echo ""
    echo -e "${YELLOW}Next steps:${NC}"
    echo "1. Review the deployment report: deployment_report.md"
    echo "2. Explore your data products in BigQuery"
    echo "3. View dbt documentation: cd dbt_project && dbt docs serve"
    echo "4. Monitor the Dataflow pipeline in GCP Console"
    echo "5. Send test events to validate real-time processing"
    echo ""
    echo -e "${BLUE}Happy analyzing! 📊${NC}"
}

# Run main function
main "$@"