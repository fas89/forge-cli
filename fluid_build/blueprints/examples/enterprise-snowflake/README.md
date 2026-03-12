# Enterprise Snowflake Data Product Blueprint

## Overview

This blueprint demonstrates production-grade patterns for deploying data products on Snowflake, including comprehensive security, performance optimization, and multi-environment deployment.

## Features

### 🔒 **Security & Compliance**
- **Role-Based Access Control (RBAC)**: Granular permissions by role
- **Dynamic Data Masking**: PII protection for emails, phones, names
- **Row-Level Security**: Regional data isolation policies
- **Column-Level Grants**: Fine-grained access control
- **Audit Logging**: Complete query history and access tracking

### ⚡ **Performance Optimization**
- **Clustering Keys**: Optimized for `region` and `customer_segment`
- **Materialized Views**: Pre-aggregated analytical views
- **Incremental Processing**: Merge strategy for efficient updates
- **Query Tags**: Cost attribution and performance tracking
- **Warehouse Sizing**: Environment-appropriate compute resources

### 💰 **Cost Management**
- **Auto-Suspend/Resume**: Automatic warehouse management
- **Resource Monitors**: Prevent runaway costs with alerts
- **Incremental Models**: Process only changed data
- **Warehouse Sizing**: Right-sized compute per environment
- **Query Optimization**: Clustering and partitioning strategies

### 🌍 **Multi-Environment**
- **Development**: Small warehouse, quick iteration
- **Staging**: Production clone for testing
- **Production**: High availability, monitoring, alerts

### ✅ **Data Quality**
- **Uniqueness Checks**: Primary key validation
- **Completeness Checks**: Required field validation
- **Freshness Checks**: SLA monitoring
- **Value Validation**: Business rule checks
- **Automated Alerting**: Slack, Email, PagerDuty

## Quick Start

### Prerequisites

```bash
# Install dependencies
pip install dbt-snowflake snowflake-connector-python

# Set Snowflake credentials
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-user"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_ROLE="SYSADMIN"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
```

### Deploy to Development

```bash
# Initialize from blueprint
fluid init --blueprint enterprise-snowflake --output ./my-customer-analytics

cd my-customer-analytics

# Configure environment
cp .env.example .env.dev
# Edit .env.dev with your Snowflake credentials

# Validate contract
fluid validate contract.fluid.yaml --provider snowflake

# Run policy checks
fluid policy-check contract.fluid.yaml --strict

# Deploy to dev
fluid apply contract.fluid.yaml \
  --provider snowflake \
  --environment dev \
  --var database=DEV_ANALYTICS \
  --var warehouse=DEV_WH
```

### Deploy to Production

```bash
# Copy production environment config
cp .env.example .env.prod

# Deploy with production settings
fluid apply contract.fluid.yaml \
  --provider snowflake \
  --environment prod \
  --var database=PROD_ANALYTICS \
  --var warehouse=PROD_WH \
  --var resource_monitor=PROD_ANALYTICS_MONITOR \
  --require-approval
```

## Architecture

### Data Flow

```
Bronze Layer (Raw Data)
  ├── customer_raw_v1
  ├── purchase_events_v1
  └── behavioral_events_v1
        ↓
Silver Layer (Cleaned)
  ├── customer_standardized
  ├── transactions_cleaned
  └── events_processed
        ↓
Gold Layer (Analytics)
  ├── customer_360_view ← Primary Output
  ├── customer_segments
  └── customer_ltv
```

### Security Model

```
Roles:
  CUSTOMER_ANALYTICS_ADMIN
    └── Full access (read/write)
  
  CUSTOMER_ANALYTICS_READ
    └── Read access with data masking
  
  MARKETING_ANALYST
    └── Read aggregated views only (no PII)
  
  DATA_SCIENTIST
    └── Read with hashed PII for ML models

Policies:
  - Dynamic masking: email, phone, names
  - Row-level security: regional isolation
  - Column-level grants: role-based
```

## Configuration

### Environment Variables

```bash
# Required
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password  # Or use key-pair auth

# Optional
SNOWFLAKE_ROLE=CUSTOMER_ANALYTICS_ADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=PROD_ANALYTICS
SNOWFLAKE_SCHEMA=CUSTOMER_ANALYTICS

# Key-pair authentication (recommended for production)
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/snowflake_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your-passphrase
```

### Warehouse Configuration

```yaml
# Development
dev:
  warehouse: "DEV_WH"
  size: "X-SMALL"
  auto_suspend: 60  # seconds
  auto_resume: true
  
# Staging
staging:
  warehouse: "STAGING_WH"
  size: "SMALL"
  auto_suspend: 300
  auto_resume: true
  
# Production
prod:
  warehouse: "PROD_WH"
  size: "MEDIUM"
  auto_suspend: 600
  auto_resume: true
  resource_monitor: "PROD_ANALYTICS_MONITOR"
  max_cluster_count: 3
```

## Security Setup

### 1. Create Roles

```sql
-- Admin role (full access)
CREATE ROLE IF NOT EXISTS CUSTOMER_ANALYTICS_ADMIN;
GRANT USAGE ON DATABASE PROD_ANALYTICS TO ROLE CUSTOMER_ANALYTICS_ADMIN;
GRANT ALL ON SCHEMA PROD_ANALYTICS.CUSTOMER_ANALYTICS TO ROLE CUSTOMER_ANALYTICS_ADMIN;

-- Read role (with masking)
CREATE ROLE IF NOT EXISTS CUSTOMER_ANALYTICS_READ;
GRANT USAGE ON DATABASE PROD_ANALYTICS TO ROLE CUSTOMER_ANALYTICS_READ;
GRANT USAGE ON SCHEMA PROD_ANALYTICS.CUSTOMER_ANALYTICS TO ROLE CUSTOMER_ANALYTICS_READ;
GRANT SELECT ON ALL TABLES IN SCHEMA PROD_ANALYTICS.CUSTOMER_ANALYTICS TO ROLE CUSTOMER_ANALYTICS_READ;

-- Marketing role (aggregated views only)
CREATE ROLE IF NOT EXISTS MARKETING_ANALYST;
GRANT USAGE ON DATABASE PROD_ANALYTICS TO ROLE MARKETING_ANALYST;
GRANT USAGE ON SCHEMA PROD_ANALYTICS.CUSTOMER_ANALYTICS TO ROLE MARKETING_ANALYST;
GRANT SELECT ON TABLE PROD_ANALYTICS.CUSTOMER_ANALYTICS.CUSTOMER_SEGMENTS TO ROLE MARKETING_ANALYST;
```

### 2. Apply Data Masking

```sql
-- Email masking policy
CREATE OR REPLACE MASKING POLICY email_mask AS (val STRING) 
RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('CUSTOMER_ANALYTICS_ADMIN') THEN val
    ELSE CONCAT(SUBSTRING(val, 1, 2), '****@', SPLIT_PART(val, '@', 2))
  END;

ALTER TABLE PROD_ANALYTICS.CUSTOMER_ANALYTICS.CUSTOMER_360_VIEW 
  MODIFY COLUMN email SET MASKING POLICY email_mask;

-- Phone masking policy
CREATE OR REPLACE MASKING POLICY phone_mask AS (val STRING)
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('CUSTOMER_ANALYTICS_ADMIN') THEN val
    ELSE CONCAT('XXX-XXX-', SUBSTRING(val, -4, 4))
  END;

ALTER TABLE PROD_ANALYTICS.CUSTOMER_ANALYTICS.CUSTOMER_360_VIEW
  MODIFY COLUMN phone SET MASKING POLICY phone_mask;
```

### 3. Row-Level Security

```sql
-- Row-level security policy for regional access
CREATE OR REPLACE ROW ACCESS POLICY region_access_policy 
AS (region STRING) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() IN ('CUSTOMER_ANALYTICS_ADMIN') THEN TRUE
    WHEN region = CURRENT_SESSION_PARAMETER('user_region') THEN TRUE
    ELSE FALSE
  END;

ALTER TABLE PROD_ANALYTICS.CUSTOMER_ANALYTICS.CUSTOMER_360_VIEW
  ADD ROW ACCESS POLICY region_access_policy ON (region);
```

## Performance Tuning

### Clustering Strategy

```sql
-- Cluster by region and segment for optimal query performance
ALTER TABLE PROD_ANALYTICS.CUSTOMER_ANALYTICS.CUSTOMER_360_VIEW
  CLUSTER BY (region, customer_segment);

-- Monitor clustering health
SELECT 
  SYSTEM$CLUSTERING_INFORMATION('CUSTOMER_360_VIEW', '(region, customer_segment)');
```

### Query Optimization

```sql
-- Add query tags for cost attribution
ALTER SESSION SET QUERY_TAG = 'customer_analytics_daily_refresh';

-- Use materialized views for frequently accessed aggregations
CREATE MATERIALIZED VIEW CUSTOMER_SEGMENTS_MV AS
SELECT 
  customer_segment,
  COUNT(*) as customer_count,
  AVG(lifetime_value) as avg_ltv,
  SUM(total_purchases) as total_purchases
FROM CUSTOMER_360_VIEW
GROUP BY customer_segment;
```

## Cost Optimization

### Resource Monitor Setup

```sql
-- Create resource monitor to prevent runaway costs
CREATE RESOURCE MONITOR PROD_ANALYTICS_MONITOR
  WITH CREDIT_QUOTA = 1000  -- Monthly credit limit
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- Assign to warehouse
ALTER WAREHOUSE PROD_WH SET RESOURCE_MONITOR = PROD_ANALYTICS_MONITOR;
```

### Warehouse Optimization

```sql
-- Auto-suspend after 10 minutes of inactivity
ALTER WAREHOUSE PROD_WH SET AUTO_SUSPEND = 600;

-- Auto-resume when queries arrive
ALTER WAREHOUSE PROD_WH SET AUTO_RESUME = TRUE;

-- Set appropriate size
ALTER WAREHOUSE PROD_WH SET WAREHOUSE_SIZE = 'MEDIUM';
```

## Monitoring & Alerts

### Data Quality Monitoring

```bash
# Run data quality checks
fluid test contract.fluid.yaml --provider snowflake

# Expected output:
# ✓ customer_id_unique: PASS (100% unique)
# ✓ email_not_null: PASS (98.5% complete)
# ✓ ltv_positive: PASS (100% valid)
# ✓ data_freshness: PASS (updated 2 hours ago)
```

### Query Performance

```sql
-- Monitor slow queries
SELECT 
  query_id,
  query_text,
  total_elapsed_time/1000 as elapsed_seconds,
  warehouse_name,
  user_name
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE warehouse_name = 'PROD_WH'
  AND start_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
  AND total_elapsed_time > 60000  -- > 60 seconds
ORDER BY total_elapsed_time DESC
LIMIT 20;
```

### Cost Tracking

```sql
-- Daily credit usage by warehouse
SELECT 
  DATE(start_time) as usage_date,
  warehouse_name,
  SUM(credits_used) as total_credits,
  SUM(credits_used) * 3.0 as estimated_cost_usd  -- Assuming $3/credit
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
```

## CI/CD Integration

### GitHub Actions

```yaml
# .github/workflows/deploy-snowflake.yml
name: Deploy to Snowflake

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Install FLUID CLI
        run: pip install fluid-forge
        
      - name: Validate Contract
        run: fluid validate contract.fluid.yaml --provider snowflake
        
      - name: Policy Check
        run: fluid policy-check contract.fluid.yaml --strict
        
      - name: Deploy to Staging
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
        run: |
          fluid apply contract.fluid.yaml \
            --provider snowflake \
            --environment staging
            
      - name: Run Tests
        run: fluid test contract.fluid.yaml --provider snowflake
        
      - name: Deploy to Production
        if: github.ref == 'refs/heads/main'
        run: |
          fluid apply contract.fluid.yaml \
            --provider snowflake \
            --environment prod \
            --require-approval
```

## Disaster Recovery

### Time Travel

```sql
-- Query historical data (up to 90 days)
SELECT * FROM CUSTOMER_360_VIEW 
AT(TIMESTAMP => '2025-12-01 00:00:00'::TIMESTAMP);

-- Restore from before accidental deletion
CREATE TABLE CUSTOMER_360_VIEW_RESTORED 
AS SELECT * FROM CUSTOMER_360_VIEW 
AT(OFFSET => -3600);  -- 1 hour ago

-- Recover dropped table
UNDROP TABLE CUSTOMER_360_VIEW;
```

### Zero-Copy Cloning

```sql
-- Clone production database for testing
CREATE DATABASE STAGING_ANALYTICS 
CLONE PROD_ANALYTICS;

-- Clone specific table
CREATE TABLE CUSTOMER_360_VIEW_TEST 
CLONE CUSTOMER_360_VIEW;
```

## Troubleshooting

### Common Issues

**1. Authentication Failure**
```bash
# Check credentials
echo $SNOWFLAKE_ACCOUNT
echo $SNOWFLAKE_USER

# Test connection
fluid doctor --provider snowflake
```

**2. Permission Denied**
```sql
-- Check current role
SELECT CURRENT_ROLE();

-- Check grants
SHOW GRANTS TO ROLE CUSTOMER_ANALYTICS_READ;
```

**3. Slow Queries**
```sql
-- Check clustering
SELECT SYSTEM$CLUSTERING_INFORMATION('CUSTOMER_360_VIEW');

-- Analyze query profile
SELECT * FROM TABLE(GET_QUERY_OPERATOR_STATS('query_id'));
```

## Best Practices

1. **Always use role-based access control** - Never grant directly to users
2. **Enable data masking for PII** - Protect sensitive data by default
3. **Use clustering keys strategically** - Based on query patterns
4. **Implement resource monitors** - Prevent runaway costs
5. **Monitor data quality continuously** - Catch issues early
6. **Use Time Travel for DR** - Easy recovery from mistakes
7. **Tag queries for cost attribution** - Track cost by team/project
8. **Right-size warehouses** - Start small, scale as needed
9. **Use incremental models** - Process only changed data
10. **Document everything** - Policies, procedures, access patterns

## Support

- **Documentation**: https://docs.company.com/customer-analytics
- **Slack**: #customer-analytics
- **Email**: customer-analytics@company.com
- **Runbook**: https://wiki.company.com/customer-analytics/runbook

## License

MIT
