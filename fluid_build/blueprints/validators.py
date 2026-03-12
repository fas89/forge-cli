# Copyright 2024-2026 Agentics Transformation Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Blueprint validation utilities
"""

from typing import List, Dict, Any
from pathlib import Path
try:
    import yaml
except ImportError:
    yaml = None
import json
from .base import Blueprint


class BlueprintValidator:
    """Validates blueprint structure and content"""
    
    def __init__(self, blueprint: Blueprint):
        self.blueprint = blueprint
    
    def validate_structure(self) -> List[str]:
        """Validate blueprint directory structure"""
        return self.blueprint.validate()
    
    def validate_contract(self) -> List[str]:
        """Validate FLUID contract syntax and structure"""
        errors = []
        
        if not self.blueprint.contract_path.exists():
            return ["Contract file missing"]
        
        if yaml is None:
            return ["PyYAML required for contract validation. Install with: pip install PyYAML"]
        
        try:
            with open(self.blueprint.contract_path, 'r') as f:
                contract = yaml.safe_load(f)
            
            # Basic contract validation
            required_fields = ['version', 'metadata', 'products']
            for field in required_fields:
                if field not in contract:
                    errors.append(f"Contract missing required field: {field}")
            
            # Validate products section
            if 'products' in contract:
                for product_name, product in contract['products'].items():
                    if 'metadata' not in product:
                        errors.append(f"Product '{product_name}' missing metadata")
                    if 'schema' not in product:
                        errors.append(f"Product '{product_name}' missing schema")
        
        except yaml.YAMLError as e:
            errors.append(f"Contract YAML syntax error: {e}")
        except Exception as e:
            errors.append(f"Contract validation error: {e}")
        
        return errors
    
    def validate_dbt_project(self) -> List[str]:
        """Validate dbt project structure"""
        errors = []
        
        if 'dbt' not in self.blueprint.metadata.runtimes:
            return []
        
        dbt_path = self.blueprint.dbt_path
        if not dbt_path.exists():
            return ["dbt_project directory missing"]
        
        # Check for dbt_project.yml
        dbt_project_file = dbt_path / "dbt_project.yml"
        if not dbt_project_file.exists():
            errors.append("dbt_project.yml missing")
        else:
            try:
                with open(dbt_project_file, 'r') as f:
                    dbt_config = yaml.safe_load(f)
                
                required_fields = ['name', 'version', 'profile']
                for field in required_fields:
                    if field not in dbt_config:
                        errors.append(f"dbt_project.yml missing field: {field}")
            
            except yaml.YAMLError as e:
                errors.append(f"dbt_project.yml syntax error: {e}")
        
        # Check for models directory
        models_path = dbt_path / "models"
        if not models_path.exists():
            errors.append("dbt models directory missing")
        
        return errors
    
    def validate_tests(self) -> List[str]:
        """Validate test structure"""
        errors = []
        
        if not self.blueprint.metadata.has_tests:
            return []
        
        tests_path = self.blueprint.tests_path
        if not tests_path.exists():
            return ["tests directory missing"]
        
        # Look for test files
        test_files = list(tests_path.glob("**/*.py"))
        if not test_files:
            errors.append("No test files found in tests directory")
        
        return errors
    
    def validate_sample_data(self) -> List[str]:
        """Validate sample data"""
        errors = []
        
        if not self.blueprint.metadata.has_sample_data:
            return []
        
        sample_data_path = self.blueprint.sample_data_path
        if not sample_data_path.exists():
            return ["sample_data directory missing"]
        
        # Check for data files
        data_files = list(sample_data_path.glob("**/*"))
        data_files = [f for f in data_files if f.is_file()]
        
        if not data_files:
            errors.append("No sample data files found")
        
        return errors
    
    def validate_documentation(self) -> List[str]:
        """Validate documentation"""
        errors = []
        
        if not self.blueprint.metadata.has_docs:
            return []
        
        docs_path = self.blueprint.docs_path
        if not docs_path.exists():
            return ["docs directory missing"]
        
        # Check for README
        readme_files = list(docs_path.glob("README.*"))
        if not readme_files:
            errors.append("No README file found in docs")
        
        return errors
    
    def validate_all(self) -> Dict[str, List[str]]:
        """Run all validations"""
        return {
            'structure': self.validate_structure(),
            'contract': self.validate_contract(),
            'dbt_project': self.validate_dbt_project(),
            'tests': self.validate_tests(),
            'sample_data': self.validate_sample_data(),
            'documentation': self.validate_documentation()
        }