# Customer 360 GCP Enterprise Blueprint

An enterprise-grade customer analytics data product built for Google Cloud Platform, featuring advanced ML-driven insights, real-time streaming, and comprehensive customer lifecycle analytics.

## Overview

This blueprint creates a complete Customer 360 analytics platform that demonstrates:

- **Advanced Customer Segmentation**: RFM analysis with behavioral patterns
- **Predictive Analytics**: ML models for churn prediction and lifetime value
- **Real-time Processing**: Streaming customer events via Dataflow
- **Cohort Analysis**: Retention and lifecycle tracking
- **Enterprise Infrastructure**: Terraform-managed GCP resources
- **Data Quality**: Comprehensive validation and monitoring

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Streaming  │    │   Storage &     │
│                 │    │              │    │   Processing    │
│ • CRM System    │───▶│ Pub/Sub      │───▶│ BigQuery        │
│ • Web Events    │    │ • Events     │    │ • Raw Data      │
│ • Mobile App    │    │ • Schema Val │    │ • Staged Data   │
│ • E-commerce    │    │              │    │ • Analytics     │
└─────────────────┘    └──────────────┘    └─────────────────┘
                              │                       │
                              ▼                       ▼
                    ┌──────────────┐    ┌─────────────────┐
                    │   Dataflow   │    │   dbt Models    │
                    │              │    │                 │
                    │ • Real-time  │    │ • Staging       │
                    │ • Enrichment │    │ • Intermediate  │
                    │ • Validation │    │ • Marts         │
                    │ • Error Hand │    │ • ML Features   │
                    └──────────────┘    └─────────────────┘
                                                  │
                                                  ▼
                                        ┌─────────────────┐
                                        │   BigQuery ML   │
                                        │                 │
                                        │ • Churn Model   │
                                        │ • LTV Model     │
                                        │ • Propensity    │
                                        │ • Auto-retrain  │
                                        └─────────────────┘
```

## Features

### 🎯 Advanced Customer Analytics
- **RFM Segmentation**: Champions, loyal customers, at-risk, hibernating segments
- **Behavioral Analysis**: Purchase patterns, channel preferences, seasonality
- **Cohort Analysis**: Retention tracking with performance benchmarks
- **Geographic & Demographic**: Multi-dimensional customer clustering

### 🤖 Machine Learning
- **Churn Prediction**: BigQuery ML logistic regression with auto-retraining
- **Customer Lifetime Value**: Linear regression for CLV forecasting  
- **Propensity Scoring**: Cross-sell and upsell recommendations
- **Next Best Action**: ML-driven customer engagement recommendations

### ⚡ Real-time Processing
- **Streaming Events**: Pub/Sub to BigQuery via Dataflow
- **Event Enrichment**: Customer context and behavioral scoring
- **Data Validation**: Schema enforcement and quality checks
- **Error Handling**: Dead letter queues and retry logic

### 🏗️ Enterprise Infrastructure
- **Infrastructure as Code**: Complete Terraform configuration
- **Security**: IAM, service accounts, data encryption
- **Monitoring**: Cost controls, performance alerts, data quality
- **Scalability**: Auto-scaling Dataflow, BigQuery slots

## Data Products

### 1. Customer Profiles Enterprise
Comprehensive customer profiles with ML predictions:
- Demographics and lifecycle stage
- RFM scores and segment assignment
- Churn probability and risk tier
- Predicted lifetime value
- Next best action recommendations
- Behavioral pattern analysis

### 2. Real-time Customer Events
Streaming behavioral events:
- Page views, purchases, interactions
- Device and channel attribution
- UTM campaign tracking
- Real-time enrichment
- Privacy-compliant data handling

### 3. Customer Cohort Analysis  
Retention and lifecycle analysis:
- Monthly cohort tracking
- Retention rate benchmarks
- Revenue per cohort analysis
- LTV progression tracking
- Performance tier classification

## Quick Start

### Prerequisites
- GCP Project with billing enabled
- Terraform >= 1.0
- gcloud CLI configured
- FLUID CLI installed (`pip install fluid-forge`)

### One-Command Deployment

Deploy the complete Customer 360 platform with a single command:

```bash
# Clone or create from blueprint
fluid blueprint create customer-360-gcp --name my-customer-analytics
cd my-customer-analytics

# Deploy everything
./deploy.sh YOUR_PROJECT_ID us-central1 dev
```

This single command will:
1. ✅ Check prerequisites and enable required APIs
2. ✅ Deploy infrastructure with Terraform (BigQuery, Pub/Sub, Storage, IAM)
3. ✅ Run analytics models with dbt (staging, intermediate, marts, ML)
4. ✅ Deploy real-time Dataflow pipeline
5. ✅ Export FLUID contract to Open Data Product Specification (OPDS)
6. ✅ Load sample data and run validation tests
7. ✅ Generate deployment report with access URLs

**Total deployment time: 20-30 minutes**

### What You Get

After deployment, you'll have:
- **3 Data Products** ready for consumption
- **Real-time streaming** processing customer events
- **ML models** for churn prediction and CLV forecasting
- **BigQuery datasets** with 100+ sample customers and 500+ transactions
- **OPDS exports** for data catalog integration
- **Monitoring dashboards** and cost alerts
- **Complete documentation** with usage examples

## Configuration

### Terraform Variables
```hcl
project_id        = "your-gcp-project"
region           = "us-central1"
bigquery_location = "US"
environment      = "dev"
```

### dbt Variables
```yaml
vars:
  rfm_analysis_months: 12
  churn_prediction_days: 90
  high_value_threshold: 1000
  ml_training_sample_size: 100000
```

## Customization Examples

### Adding New Customer Segments
```sql
-- In int_customer_rfm.sql
CASE
  WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 
    THEN 'vip_champions'
  WHEN your_custom_logic
    THEN 'your_segment'
  -- ... existing segments
END AS customer_segment
```

### Custom ML Features
```sql
-- Add to ML model training data
SELECT 
  -- existing features
  custom_feature_1,
  custom_feature_2,
  CASE WHEN custom_condition THEN 1 ELSE 0 END AS custom_binary_feature
FROM training_data
```

### New Event Types
```python
# In dataflow/customer_events_pipeline.py
def _transform_event(self, event_data):
    if event_data['event_type'] == 'CUSTOM_EVENT':
        # Custom event processing logic
        processed_event['custom_field'] = extract_custom_data(event_data)
    
    return processed_event
```

## Monitoring & Alerting

### Built-in Monitoring

All blueprints include comprehensive monitoring:

- **Data Quality Dashboards**: Track data freshness, completeness, and accuracy
- **Performance Metrics**: Monitor query performance and resource usage  
- **Business Metrics**: Track key business KPIs and trends
- **Cost Tracking**: Monitor and optimize cloud resource costs
- **OPDS Exports**: Automatically exported for data catalog integration

### Real-time Observability

- **Dataflow Monitoring**: Pipeline latency, throughput, error rates
- **BigQuery Performance**: Slot utilization, query costs, data scanned
- **ML Model Accuracy**: Automated model performance tracking
- **Data Freshness**: SLA compliance monitoring

### OPDS Integration

The blueprint automatically exports data product specifications to Open Data Product Specification (OPDS) format:

```bash
# Exports are generated during deployment
ls exports/
# customer-360-gcp.opds.json  # JSON format for API consumption
# customer-360-gcp.opds.yaml  # YAML format for human readability
```

These exports enable:
- **Data Catalog Integration**: Import into DataHub, Apache Atlas, etc.
- **API Documentation**: Auto-generated API specs
- **Governance Compliance**: Automated lineage and impact analysis
- **Cross-Platform Discovery**: Standard format for data marketplace

## Cost Management

### Estimated Monthly Costs (US region)
- **BigQuery Storage**: ~$50-200 (depends on data volume)
- **BigQuery Compute**: ~$100-500 (depends on query frequency)
- **Dataflow**: ~$300-800 (streaming pipeline, 2-4 workers)
- **Pub/Sub**: ~$10-50 (message volume dependent)
- **Cloud Storage**: ~$20-100 (backup and staging)

**Total Estimated**: $480-1,650/month

### Cost Optimization
- Use BigQuery slots reservation for predictable workloads
- Implement table partitioning and clustering
- Set up cost alerts and budgets
- Archive old data to cheaper storage classes

## Security & Compliance

### Data Privacy
- PII hashing (emails, IP addresses)
- Configurable data retention
- Access control by role and dataset
- Audit logging enabled

### Compliance Features
- GDPR data handling patterns
- Data lineage tracking
- Schema evolution support
- Backup and recovery procedures

## Extension Points

### Adding New Data Sources
1. Extend Terraform with new connections
2. Add staging models in dbt
3. Update data lineage documentation
4. Add quality tests

### Custom ML Models
1. Create new model in `models/ml/`
2. Add training data preparation
3. Update prediction integration
4. Add model monitoring

### Additional Analytics
1. Add intermediate models for new metrics
2. Create new mart tables
3. Add to data product contract
4. Update documentation

## Support & Troubleshooting

### Common Issues
1. **Permission Errors**: Verify service account IAM roles
2. **Schema Mismatch**: Check BigQuery table schemas
3. **Pipeline Failures**: Review Dataflow logs
4. **Model Training**: Validate training data quality

### Debugging
```bash
# Check Terraform state
terraform show

# dbt debugging
dbt debug
dbt run --models +model_name+

# Dataflow monitoring
gcloud dataflow jobs list --region=us-central1

# BigQuery query history
bq ls -j --max_results=10
```

## Contributing

This blueprint is designed for extension and customization:

1. Fork the blueprint structure
2. Modify configurations for your use case
3. Add custom business logic
4. Extend with additional data sources
5. Share improvements back to the blueprint registry

## License

This blueprint is provided under the MIT License. See LICENSE file for details.