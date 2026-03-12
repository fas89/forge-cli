# Test Configuration

This directory contains tests for the Customer 360 blueprint.

## Test Categories

### Data Quality Tests
- Customer profile validation
- Transaction integrity checks
- Segmentation logic verification

### Business Logic Tests  
- Lifetime value calculation
- Engagement scoring
- Churn probability logic

### Integration Tests
- End-to-end pipeline testing
- Data lineage validation
- Performance benchmarks

## Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test category
pytest tests/test_data_quality.py
pytest tests/test_business_logic.py
pytest tests/test_integration.py

# Run with coverage
pytest tests/ --cov=customer_360
```