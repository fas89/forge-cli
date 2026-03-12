# Customer 360 Analytics Platform

## Overview

The Customer 360 Blueprint creates a comprehensive customer analytics platform that unifies customer data, generates behavioral insights, and enables data-driven decision making.

## What You Get

✅ **Complete FLUID Contract** - Data product definitions with schemas and governance  
✅ **Working dbt Project** - Dimensional models with incremental processing  
✅ **Sample Data** - Ready-to-use test datasets  
✅ **Data Quality Tests** - Comprehensive test suite  
✅ **Documentation** - Complete setup and usage guide  

## Features

### 📊 Customer Profiles
- Unified customer view with demographics and behavior
- Customer segmentation (high_value, regular, at_risk, dormant)
- Lifetime value calculation
- Engagement scoring

### 💳 Transaction Analytics
- Complete transaction history with context
- First purchase flags and customer journey tracking
- Channel and payment method analysis
- Incremental processing for performance

### 🎯 Customer Segmentation
- Segment-level analytics and metrics
- Revenue attribution by segment
- Churn risk analysis
- Marketing targeting insights

## Quick Start

### 1. Deploy the Blueprint

```bash
# Using FLUID Forge
python3 -m fluid_build forge --blueprint customer-360 --target-dir ./my-customer-360

# Or using Blueprint command
python3 -m fluid_build blueprint create customer-360 --target-dir ./my-customer-360
```

### 2. Set Up Your Data

```bash
cd my-customer-360

# Load sample data (for testing)
dbt seed

# Or configure your data sources in profiles.yml
# Point to your actual customer and order tables
```

### 3. Build the Data Product

```bash
# Run the complete pipeline
dbt run

# Run tests to ensure quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### 4. Validate with FLUID

```bash
# Validate the contract
python3 -m fluid_build validate

# Run quality checks
python3 -m fluid_build test

# Generate reports
python3 -m fluid_build plan
```

## Data Architecture

```
Raw Data Sources
├── customers (CRM data)
└── orders (e-commerce transactions)
    ↓
Staging Layer (stg_*)
├── stg_customers - Clean customer data
└── stg_orders - Clean order data
    ↓
Intermediate Layer (int_*)
├── int_customer_order_metrics - Order aggregations
├── int_customer_engagement - Engagement scoring
├── int_customer_segmentation - Segment assignment
└── int_customer_transaction_context - Transaction enrichment
    ↓
Marts Layer (Data Products)
├── customer_profiles - Unified customer view
├── customer_transactions - Enriched transaction history
└── customer_segments - Segment analytics
```

## Customization

### Segmentation Rules

Edit `int_customer_segmentation.sql` to customize:

```sql
-- Modify these thresholds in dbt_project.yml
vars:
  high_value_ltv_threshold: 5000  # High-value customer threshold
  regular_value_ltv_threshold: 1000  # Regular customer threshold
  churn_risk_days_threshold: 90  # Days for churn risk
```

### Engagement Scoring

Customize engagement calculation in `int_customer_engagement.sql`:
- Recency weight (default: 40%)
- Frequency weight (default: 40%) 
- Consistency weight (default: 20%)

### Data Sources

Update `models/sources.yml` to point to your actual tables:

```yaml
sources:
  - name: raw
    tables:
      - name: customers
        identifier: your_customer_table
      - name: orders
        identifier: your_order_table
```

## Use Cases

### Marketing Teams
- **Customer Segmentation**: Target high-value vs at-risk customers
- **Campaign Optimization**: Understand channel preferences
- **Retention Programs**: Identify churn risks early

### Product Teams  
- **User Behavior**: Analyze purchase patterns and engagement
- **Feature Adoption**: Track product category preferences
- **Customer Journey**: Understand first purchase to loyalty progression

### Finance Teams
- **Revenue Analytics**: Segment contribution and LTV analysis
- **Forecasting**: Predict revenue by customer segment
- **Pricing Strategy**: Optimize based on customer value

## Performance

- **Incremental Processing**: Transactions update incrementally for efficiency
- **Indexed Tables**: Optimized for common query patterns
- **Partitioning Ready**: Easily add date partitioning for scale

## Next Steps

1. **Integrate Additional Data**: Add marketing touchpoints, support tickets, product usage
2. **Advanced Analytics**: Implement ML models for churn prediction, recommendation engines
3. **Real-time Features**: Add streaming data for real-time customer scoring
4. **Automation**: Set up Airflow DAGs for scheduled refreshes

## Support

- 📖 **Documentation**: Full dbt docs generated locally
- 🧪 **Tests**: Comprehensive data quality and business logic tests
- 🔧 **Customizable**: Easily modify for your specific business rules
- 🚀 **Production Ready**: Follows data engineering best practices