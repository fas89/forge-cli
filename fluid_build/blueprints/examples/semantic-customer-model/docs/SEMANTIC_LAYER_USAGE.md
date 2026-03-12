# Example: Querying the Semantic Layer

This guide demonstrates how to use the semantic layer for business analytics.

## Overview

The semantic layer provides business-friendly abstractions over the physical data model, enabling self-service analytics without SQL expertise.

## Architecture

```
Physical Tables          Semantic Layer              Business Users
┌──────────────┐        ┌──────────────────┐       ┌──────────────┐
│ dim_customer │───────►│ Customer Entity  │──────►│ Analysts     │
│ (SCD Type 2) │        │ - Dimensions     │       │ Executives   │
└──────────────┘        │ - Measures       │       │ Data Sci     │
                        └──────────────────┘       └──────────────┘
┌──────────────┐        ┌──────────────────┐
│ fact_trans   │───────►│ Metrics          │
└──────────────┘        │ - LTV            │
                        │ - Churn Risk     │
┌──────────────┐        │ - AOV            │
│ fact_interact│───────►│ - Frequency      │
└──────────────┘        └──────────────────┘
```

## Setup

### 1. Install dbt Semantic Interfaces

```bash
pip install dbt-semantic-interfaces dbt-metricflow
```

### 2. Initialize Semantic Layer

```bash
cd dbt_project
dbt parse
dbt compile
```

## Query Methods

### Method 1: SQL (Direct)

Query the semantic layer using SQL:

```sql
-- Total LTV by customer segment
SELECT 
  customer_segment,
  SUM(lifetime_value) as total_ltv,
  COUNT(DISTINCT customer_id) as customer_count,
  AVG(lifetime_value) as avg_ltv
FROM {{ ref('semantic_customer_model') }}
GROUP BY customer_segment
ORDER BY total_ltv DESC;
```

### Method 2: dbt Metrics

Define and query metrics:

```bash
# Query a metric
dbt run-operation query_metric \
  --args '{
    "metric": "customer_lifetime_value",
    "grain": "customer_segment"
  }'
```

### Method 3: MetricFlow (Recommended)

Use MetricFlow for semantic queries:

```python
from metricflow import MetricFlowClient

client = MetricFlowClient.from_config()

# Query metrics
result = client.query(
    metrics=["customer_lifetime_value", "customer_count"],
    group_by=["customer_segment"],
    where=["region = 'US'"]
)

print(result.df)
```

### Method 4: Python API

```python
from dbt_semantic_interfaces import SemanticLayerClient
import pandas as pd

# Initialize client
client = SemanticLayerClient(
    account="your-account",
    environment="prod",
    token="your-token"
)

# Simple metric query
df = client.query(
    metrics=["total_lifetime_value", "customer_count"],
    group_by=["customer_segment"]
)

print(df)
```

## Example Queries

### 1. Customer Lifetime Value by Segment

```python
# Query LTV by segment
result = client.query(
    metrics=[
        "customer_lifetime_value",
        "customer_count",
        "average_customer_ltv"
    ],
    group_by=["customer_segment"],
    order_by=["customer_lifetime_value DESC"]
)

print(result.df)
```

**Output:**
```
customer_segment  customer_lifetime_value  customer_count  average_customer_ltv
VIP               $5,234,000               1,234          $4,241
Premium           $3,876,000               2,567          $1,510
Standard          $2,345,000               8,901          $263
At-Risk           $567,000                 1,890          $300
```

### 2. Churn Risk Analysis by Region

```python
result = client.query(
    metrics=[
        "customer_count",
        "avg_churn_risk",
        "churn_risk_rate"
    ],
    group_by=["region"],
    where=["churn_risk_score > 0.7"],
    order_by=["avg_churn_risk DESC"]
)
```

### 3. Revenue Trends Over Time

```python
result = client.query(
    metrics=[
        "total_revenue",
        "customer_count",
        "average_order_value"
    ],
    group_by=["first_order_date__month"],
    where=["first_order_date >= '2024-01-01'"],
    order_by=["first_order_date__month"]
)
```

### 4. Product Category Affinity

```python
result = client.query(
    metrics=[
        "customer_count",
        "total_revenue"
    ],
    group_by=[
        "top_product_category",
        "customer_segment"
    ],
    order_by=["total_revenue DESC"]
)
```

### 5. Customer Acquisition Cohorts

```python
result = client.query(
    metrics=[
        "customer_count",
        "average_customer_ltv",
        "avg_churn_risk"
    ],
    group_by=[
        "first_order_date__quarter",
        "customer_segment"
    ],
    where=["first_order_date >= '2024-01-01'"]
)
```

## Time Intelligence

### Time Grains

```python
# Daily
result = client.query(
    metrics=["customer_count"],
    group_by=["first_order_date__day"]
)

# Weekly
result = client.query(
    metrics=["customer_count"],
    group_by=["first_order_date__week"]
)

# Monthly
result = client.query(
    metrics=["customer_count"],
    group_by=["first_order_date__month"]
)

# Quarterly
result = client.query(
    metrics=["customer_count"],
    group_by=["first_order_date__quarter"]
)

# Yearly
result = client.query(
    metrics=["customer_count"],
    group_by=["first_order_date__year"]
)
```

### Time Filters

```python
# Last 30 days
result = client.query(
    metrics=["customer_count"],
    where=["first_order_date >= DATEADD(day, -30, CURRENT_DATE())"]
)

# Specific date range
result = client.query(
    metrics=["customer_count"],
    where=["first_order_date BETWEEN '2024-01-01' AND '2024-12-31'"]
)

# Year-to-date
result = client.query(
    metrics=["customer_count"],
    where=["first_order_date >= DATE_TRUNC('year', CURRENT_DATE())"]
)
```

## Dimension Hierarchies

### Geographic Hierarchy

```python
# Country level
result = client.query(
    metrics=["customer_count", "total_revenue"],
    group_by=["country"]
)

# Drill down to state
result = client.query(
    metrics=["customer_count", "total_revenue"],
    group_by=["country", "state"],
    where=["country = 'USA'"]
)

# Drill down to city
result = client.query(
    metrics=["customer_count", "total_revenue"],
    group_by=["country", "state", "city"],
    where=["state = 'California'"]
)
```

## Calculated Metrics

### On-the-Fly Calculations

```python
# Calculate LTV to revenue ratio
result = client.query(
    metrics=[
        "total_lifetime_value",
        "total_revenue",
        "ltv_to_revenue_ratio"  # Defined as derived metric
    ],
    group_by=["customer_segment"]
)
```

### Custom Aggregations

```python
# Percentile calculations
result = client.query(
    metrics=[
        "customer_count",
        "PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY lifetime_value) as median_ltv",
        "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lifetime_value) as p95_ltv"
    ],
    group_by=["customer_segment"]
)
```

## Filtering

### Simple Filters

```python
result = client.query(
    metrics=["customer_count"],
    where=["customer_segment = 'VIP'"]
)
```

### Multiple Conditions

```python
result = client.query(
    metrics=["customer_count", "total_revenue"],
    where=[
        "customer_segment IN ('VIP', 'Premium')",
        "region = 'US'",
        "churn_risk_score < 0.5"
    ]
)
```

### Complex Filters

```python
result = client.query(
    metrics=["customer_count"],
    where=[
        "(customer_segment = 'VIP' OR lifetime_value > 10000)",
        "AND churn_risk_score < 0.3",
        "AND first_order_date >= '2024-01-01'"
    ]
)
```

## BI Tool Integration

### Tableau

1. Connect to Snowflake
2. Use semantic views as data source
3. Create calculated fields referencing metrics

```sql
-- Custom SQL in Tableau
SELECT 
  customer_segment,
  customer_lifetime_value,
  customer_count,
  average_customer_ltv
FROM {{ semantic_customer_model }}
GROUP BY customer_segment
```

### Power BI

1. Get Data → Snowflake
2. Use semantic layer views
3. Create measures using DAX

```dax
Total LTV = SUM('semantic_customer_model'[customer_lifetime_value])
Avg LTV = AVERAGE('semantic_customer_model'[average_customer_ltv])
```

### Looker

```lookml
view: customer_metrics {
  sql_table_name: semantic_customer_model ;;

  dimension: customer_segment {
    type: string
    sql: ${TABLE}.customer_segment ;;
  }

  measure: total_ltv {
    type: sum
    sql: ${TABLE}.customer_lifetime_value ;;
    value_format: "$#,##0"
  }

  measure: customer_count {
    type: count_distinct
    sql: ${TABLE}.customer_id ;;
  }
}
```

## Performance Tips

### 1. Use Aggregated Views

For better performance, query pre-aggregated tables:

```sql
-- Instead of aggregating on-the-fly
SELECT customer_segment, SUM(lifetime_value)
FROM customer_intelligence_v2
WHERE is_current = TRUE
GROUP BY customer_segment

-- Use pre-aggregated view
SELECT * FROM customer_segment_metrics
```

### 2. Leverage Materialization

```yaml
# In semantic_models.yml
semantic_models:
  - name: customer_entity
    config:
      materialized: table  # Pre-compute for performance
```

### 3. Filter Early

```python
# Good: Filter before aggregation
result = client.query(
    metrics=["customer_count"],
    where=["region = 'US'"],
    group_by=["customer_segment"]
)

# Avoid: Filtering after aggregation in app code
```

## Troubleshooting

### Issue: Metric Not Found

```python
# Check available metrics
metrics = client.list_metrics()
print(metrics)
```

### Issue: Slow Query Performance

```sql
-- Check query execution
SELECT 
  query_id,
  query_text,
  execution_time,
  bytes_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%semantic_customer_model%'
ORDER BY execution_time DESC
LIMIT 10;
```

### Issue: Inconsistent Results

```python
# Validate metric calculations
result = client.query(
    metrics=["customer_lifetime_value"],
    group_by=["customer_id"],
    where=["customer_id = 'C001'"]
)

# Compare with raw data
raw = pd.read_sql("""
    SELECT lifetime_value 
    FROM customer_intelligence_v2 
    WHERE customer_id = 'C001' AND is_current = TRUE
""", conn)
```

## Best Practices

1. **Use Semantic Names**: Query by business concepts, not table columns
2. **Leverage Dimensions**: Use hierarchies for drill-down analysis
3. **Filter Appropriately**: Apply filters to reduce data scanned
4. **Cache Results**: Cache frequently-used queries
5. **Document Metrics**: Maintain clear metric definitions
6. **Test Calculations**: Validate metric logic regularly

## Examples Repository

See `/examples/semantic_queries/` for more examples:
- Customer segmentation analysis
- Cohort analysis
- Retention curves
- Revenue attribution
- Churn prediction dashboards

## Support

- **Documentation**: [docs/semantic-layer](../docs/semantic_layer)
- **Slack**: #semantic-layer-support
- **Email**: semantic-layer@company.com
