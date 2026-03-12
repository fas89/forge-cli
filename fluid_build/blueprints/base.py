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
Blueprint base classes and metadata structures
"""

from typing import Dict, List, Optional, Any
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum

try:
    import yaml
except ImportError:
    yaml = None


class BlueprintComplexity(Enum):
    """Blueprint complexity levels"""
    BEGINNER = "beginner"
    INTERMEDIATE = "intermediate"  
    ADVANCED = "advanced"


class BlueprintCategory(Enum):
    """Blueprint categories"""
    ANALYTICS = "analytics"
    ML_AI = "ml-ai"
    REAL_TIME = "real-time"
    BATCH = "batch"
    STREAMING = "streaming"
    REPORTING = "reporting"


@dataclass
class BlueprintDependency:
    """External dependency required by blueprint"""
    name: str
    version: Optional[str] = None
    provider: Optional[str] = None
    required: bool = True


@dataclass
class BlueprintMetadata:
    """Blueprint metadata and configuration"""
    
    # Basic Information
    name: str
    title: str
    description: str
    version: str
    
    # Classification
    category: BlueprintCategory
    complexity: BlueprintComplexity
    tags: List[str] = field(default_factory=list)
    
    # Technical Details
    providers: List[str] = field(default_factory=list)  # snowflake, bigquery, etc.
    runtimes: List[str] = field(default_factory=list)   # dbt, airflow, etc.
    dependencies: List[BlueprintDependency] = field(default_factory=list)
    
    # Documentation
    use_cases: List[str] = field(default_factory=list)
    best_practices: List[str] = field(default_factory=list)
    setup_time: str = "30 minutes"  # Estimated setup time
    
    # Authoring
    author: str = "FLUID Team"
    created_at: str = ""
    updated_at: str = ""
    
    # Blueprint Structure
    has_sample_data: bool = True
    has_tests: bool = True
    has_docs: bool = True
    has_cicd: bool = True
    

class Blueprint:
    """
    A complete, working data product blueprint
    
    Blueprints contain:
    - FLUID contract
    - Working code (dbt, Airflow, etc.)
    - Test suites
    - Documentation
    - Sample data
    - Deployment configs
    """
    
    def __init__(self, path: Path):
        self.path = path
        self.metadata: Optional[BlueprintMetadata] = None
        self._load_metadata()
    
    def _load_metadata(self):
        """Load blueprint metadata from blueprint.yaml"""
        metadata_file = self.path / "blueprint.yaml"
        if not metadata_file.exists():
            raise ValueError(f"Blueprint metadata not found: {metadata_file}")
        
        if yaml is None:
            raise ImportError("PyYAML is required for blueprint support. Install with: pip install PyYAML")
        
        with open(metadata_file, 'r') as f:
            data = yaml.safe_load(f)
        
        # Convert to BlueprintMetadata object
        self.metadata = BlueprintMetadata(
            name=data['name'],
            title=data['title'],
            description=data['description'],
            version=data['version'],
            category=BlueprintCategory(data['category']),
            complexity=BlueprintComplexity(data['complexity']),
            tags=data.get('tags', []),
            providers=data.get('providers', []),
            runtimes=data.get('runtimes', []),
            use_cases=data.get('use_cases', []),
            best_practices=data.get('best_practices', []),
            setup_time=data.get('setup_time', '30 minutes'),
            author=data.get('author', 'FLUID Team'),
            created_at=data.get('created_at', ''),
            updated_at=data.get('updated_at', ''),
            has_sample_data=data.get('has_sample_data', True),
            has_tests=data.get('has_tests', True),
            has_docs=data.get('has_docs', True),
            has_cicd=data.get('has_cicd', True)
        )
        
        # Load dependencies
        deps_data = data.get('dependencies', [])
        self.metadata.dependencies = [
            BlueprintDependency(**dep) for dep in deps_data
        ]
    
    @property 
    def contract_path(self) -> Path:
        """Path to FLUID contract"""
        return self.path / "contract.fluid.yaml"
    
    @property
    def dbt_path(self) -> Path:
        """Path to dbt project"""
        return self.path / "dbt_project"
    
    @property
    def airflow_path(self) -> Path:
        """Path to Airflow DAGs"""
        return self.path / "airflow_dags"
    
    @property
    def tests_path(self) -> Path:
        """Path to tests"""
        return self.path / "tests"
    
    @property
    def docs_path(self) -> Path:
        """Path to documentation"""
        return self.path / "docs"
    
    @property
    def sample_data_path(self) -> Path:
        """Path to sample data"""
        return self.path / "sample_data"
    
    def validate(self) -> List[str]:
        """Validate blueprint structure and contents"""
        errors = []
        
        # Check required files
        if not self.contract_path.exists():
            errors.append("Missing contract.fluid.yaml")
        
        if self.metadata.has_tests and not self.tests_path.exists():
            errors.append("Blueprint claims to have tests but tests/ directory missing")
        
        if self.metadata.has_docs and not self.docs_path.exists():
            errors.append("Blueprint claims to have docs but docs/ directory missing")
        
        if self.metadata.has_sample_data and not self.sample_data_path.exists():
            errors.append("Blueprint claims to have sample data but sample_data/ directory missing")
        
        # Validate runtime-specific directories
        if 'dbt' in self.metadata.runtimes and not self.dbt_path.exists():
            errors.append("Blueprint uses dbt runtime but dbt_project/ directory missing")
        
        if 'airflow' in self.metadata.runtimes and not self.airflow_path.exists():
            errors.append("Blueprint uses airflow runtime but airflow_dags/ directory missing")
        
        return errors
    
    def generate_project(self, target_dir: Path, customizations: Dict[str, Any] = None) -> None:
        """Generate a new project from this blueprint"""
        from shutil import copytree, ignore_patterns
        
        # Copy blueprint to target directory
        copytree(
            self.path,
            target_dir,
            ignore=ignore_patterns('blueprint.yaml', '*.pyc', '__pycache__')
        )
        
        if customizations:
            self._apply_customizations(target_dir, customizations)
    
    def _apply_customizations(self, target_dir: Path, customizations: Dict[str, Any]):
        """Apply user customizations to generated project"""
        # This would handle variable substitution, file renaming, etc.
        pass