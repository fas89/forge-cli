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
FLUID Build Blueprints - Opinionated Data Product Templates

This module provides a blueprint system for creating complete, working data products
that follow best practices and can be quickly deployed by teams.

Blueprints are more than templates - they are fully functional data products with:
- Complete FLUID contracts
- Working pipeline code (dbt, Airflow, etc.)
- Test suites
- Documentation
- Sample data
- Deployment configurations
"""

from .registry import BlueprintRegistry
from .base import Blueprint, BlueprintMetadata
from .validators import BlueprintValidator

__all__ = [
    'BlueprintRegistry',
    'Blueprint', 
    'BlueprintMetadata',
    'BlueprintValidator'
]

# Global registry instance
registry = BlueprintRegistry()