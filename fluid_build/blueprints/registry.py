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
Blueprint registry for discovering and managing available blueprints
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional

from .base import Blueprint, BlueprintCategory, BlueprintComplexity

logger = logging.getLogger(__name__)


class BlueprintRegistry:
    """Registry for discovering and managing blueprints"""

    def __init__(self, blueprint_dirs: List[Path] = None):
        self.blueprints: Dict[str, Blueprint] = {}
        self.blueprint_dirs = blueprint_dirs or []

        # Add default blueprint directory
        default_dir = Path(__file__).parent / "examples"
        if default_dir.exists():
            self.blueprint_dirs.append(default_dir)

        self.refresh()

    def refresh(self):
        """Discover and load all blueprints"""
        self.blueprints.clear()

        for blueprint_dir in self.blueprint_dirs:
            if not blueprint_dir.exists():
                continue

            for path in blueprint_dir.iterdir():
                if path.is_dir() and (path / "blueprint.yaml").exists():
                    try:
                        blueprint = Blueprint(path)
                        self.blueprints[blueprint.metadata.name] = blueprint
                        logger.debug(f"Loaded blueprint: {blueprint.metadata.name}")
                    except Exception as e:
                        logger.warning(f"Failed to load blueprint from {path}: {e}")

    def list_blueprints(
        self,
        category: Optional[BlueprintCategory] = None,
        complexity: Optional[BlueprintComplexity] = None,
        provider: Optional[str] = None,
    ) -> List[Blueprint]:
        """List blueprints with optional filtering"""
        blueprints = list(self.blueprints.values())

        if category:
            blueprints = [bp for bp in blueprints if bp.metadata.category == category]

        if complexity:
            blueprints = [bp for bp in blueprints if bp.metadata.complexity == complexity]

        if provider:
            blueprints = [bp for bp in blueprints if provider in bp.metadata.providers]

        return sorted(blueprints, key=lambda bp: bp.metadata.name)

    def get_blueprint(self, name: str) -> Optional[Blueprint]:
        """Get blueprint by name"""
        return self.blueprints.get(name)

    def search_blueprints(self, query: str) -> List[Blueprint]:
        """Search blueprints by name, title, description, or tags"""
        query = query.lower()
        results = []

        for blueprint in self.blueprints.values():
            metadata = blueprint.metadata

            # Search in name, title, description
            if (
                query in metadata.name.lower()
                or query in metadata.title.lower()
                or query in metadata.description.lower()
            ):
                results.append(blueprint)
                continue

            # Search in tags
            if any(query in tag.lower() for tag in metadata.tags):
                results.append(blueprint)
                continue

            # Search in use cases
            if any(query in use_case.lower() for use_case in metadata.use_cases):
                results.append(blueprint)

        return sorted(results, key=lambda bp: bp.metadata.name)

    def get_categories(self) -> List[BlueprintCategory]:
        """Get all available categories"""
        categories = set()
        for blueprint in self.blueprints.values():
            categories.add(blueprint.metadata.category)
        return sorted(list(categories), key=lambda c: c.value)

    def get_providers(self) -> List[str]:
        """Get all available providers"""
        providers = set()
        for blueprint in self.blueprints.values():
            providers.update(blueprint.metadata.providers)
        return sorted(list(providers))

    def validate_all(self) -> Dict[str, List[str]]:
        """Validate all blueprints and return errors"""
        validation_results = {}

        for name, blueprint in self.blueprints.items():
            errors = blueprint.validate()
            if errors:
                validation_results[name] = errors

        return validation_results
