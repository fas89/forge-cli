# Customer 360 GCP Tests

This directory contains tests for the Customer 360 GCP blueprint.

## Test Structure

- `unit/` - Unit tests for individual components
- `integration/` - Integration tests for the complete pipeline
- `data_quality/` - Data quality tests using dbt
- `performance/` - Performance and load tests

## Running Tests

```bash
# Run all tests
./run_tests.sh

# Run specific test categories
pytest unit/
pytest integration/
cd dbt_project && dbt test
```

## Test Data

Tests use the sample data provided in `sample_data/` directory.

## CI/CD Integration

Tests are automatically run in the deployment pipeline to ensure data quality and system reliability.