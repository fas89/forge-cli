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
FLUID Contract Field Adapter

Provides utilities for accessing contract fields in a version-agnostic way.
This allows the codebase to work with schema 0.5.7 while maintaining
clean abstraction for future schema versions.
"""
from typing import Any, Dict, List, Optional, Union


def get_expose_id(expose: Dict[str, Any]) -> Optional[str]:
    """
    Get the expose ID from an expose object.
    
    Schema 0.5.7+: exposeId
    Schema 0.4.0: id
    
    Args:
        expose: The expose dictionary
        
    Returns:
        The expose ID or None
    """
    return expose.get('exposeId') or expose.get('id')


def get_expose_kind(expose: Dict[str, Any]) -> Optional[str]:
    """
    Get the expose kind/type from an expose object.
    
    Schema 0.5.7+: kind
    Schema 0.4.0: type
    
    Args:
        expose: The expose dictionary
        
    Returns:
        The expose kind/type or None
    """
    return expose.get('kind') or expose.get('type')


def get_expose_binding(expose: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Get the expose binding/location from an expose object.
    
    Schema 0.5.7+: binding (object with provider, location, etc.)
    Schema 0.4.0: location (string)
    
    Args:
        expose: The expose dictionary
        
    Returns:
        The binding object or None
    """
    binding = expose.get('binding')
    if binding:
        return binding
    
    # Fallback: convert old location string to binding object
    location = expose.get('location')
    if location and isinstance(location, str):
        return {'location': location}
    
    return None


def get_expose_location(expose: Dict[str, Any]) -> Optional[str]:
    """
    Get the physical location string from an expose object.
    
    Schema 0.5.7+: binding.location
    Schema 0.4.0: location
    
    Args:
        expose: The expose dictionary
        
    Returns:
        The location string or None
    """
    binding = get_expose_binding(expose)
    if binding and isinstance(binding, dict):
        return binding.get('location')
    
    # Direct fallback
    return expose.get('location')


def get_builds(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Get the builds array from a contract.
    
    Schema 0.5.7+: builds (array)
    Schema 0.4.0: build (single object)
    
    Args:
        contract: The contract dictionary
        
    Returns:
        List of build objects (may be empty)
    """
    builds = contract.get('builds')
    if builds and isinstance(builds, list):
        return builds
    
    # Fallback: wrap single build in array
    build = contract.get('build')
    if build and isinstance(build, dict):
        return [build]
    
    return []


def get_primary_build(contract: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Get the primary/first build from a contract.
    
    Args:
        contract: The contract dictionary
        
    Returns:
        The first build object or None
    """
    builds = get_builds(contract)
    return builds[0] if builds else None


def get_build_engine(build: Dict[str, Any]) -> Optional[str]:
    """
    Get the build engine from a build object.
    
    Args:
        build: The build dictionary
        
    Returns:
        The engine name (e.g., 'dbt', 'dataform', 'spark')
    """
    return build.get('engine') or build.get('type')


def get_contract_version(contract: Dict[str, Any]) -> Optional[str]:
    """
    Get the FLUID schema version from a contract.
    
    Args:
        contract: The contract dictionary
        
    Returns:
        The fluidVersion string (e.g., '0.5.7')
    """
    return contract.get('fluidVersion')


def get_expose_schema(expose: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Get the schema definition from an expose object.
    
    Args:
        expose: The expose dictionary
        
    Returns:
        The schema object or None
    """
    return expose.get('schema')


def get_expose_contract(expose: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Get the contract section from an expose object (0.5.7 format).
    
    In 0.5.7, schema and dq are nested under a 'contract' key.
    In 0.4.0, they are at the top level.
    
    Args:
        expose: The expose dictionary
        
    Returns:
        The contract section object or None
    """
    return expose.get('contract')


def get_expose_format(expose: Dict[str, Any]) -> Optional[str]:
    """
    Get the data format from an expose object.
    
    Args:
        expose: The expose dictionary
        
    Returns:
        The format string (e.g., 'parquet', 'avro', 'json')
    """
    return expose.get('format')


def normalize_expose(expose: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize an expose object to 0.5.7 field names.
    
    This creates a new dictionary with normalized field names,
    useful for templates and output generation.
    
    Args:
        expose: The expose dictionary
        
    Returns:
        Normalized expose dictionary
    """
    normalized = expose.copy()
    
    # Normalize ID field
    if 'id' in normalized and 'exposeId' not in normalized:
        normalized['exposeId'] = normalized.pop('id')
    
    # Normalize kind field
    if 'type' in normalized and 'kind' not in normalized:
        normalized['kind'] = normalized.pop('type')
    
    # Normalize binding field
    if 'location' in normalized and 'binding' not in normalized:
        location = normalized.pop('location')
        normalized['binding'] = {'location': location}
    
    return normalized


def normalize_contract(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a contract to 0.5.7 structure.
    
    This creates a new dictionary with normalized structure,
    useful for templates and output generation.
    
    Args:
        contract: The contract dictionary
        
    Returns:
        Normalized contract dictionary
    """
    normalized = contract.copy()
    
    # Normalize builds field
    if 'build' in normalized and 'builds' not in normalized:
        build = normalized.pop('build')
        normalized['builds'] = [build]
    
    # Normalize exposes
    if 'exposes' in normalized:
        normalized['exposes'] = [
            normalize_expose(exp) for exp in normalized['exposes']
        ]
    
    return normalized
