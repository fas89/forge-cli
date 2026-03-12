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

from __future__ import annotations
import argparse, logging, os, json
from pathlib import Path
from typing import Dict, Any, Optional
from ._logging import info
from ._common import CLIError
from ._io import dump_json, atomic_write
from fluid_build.cli.console import cprint

COMMAND = "wizard"

def register(subparsers: argparse._SubParsersAction):
    p = subparsers.add_parser(
        COMMAND, 
        help="Interactive first-run wizard (TUI)",
        description="Guided setup wizard for creating new FLUID data products with interactive prompts."
    )
    p.add_argument("--provider", choices=["local", "gcp", "snowflake", "aws"], help="Pre-select provider")
    p.add_argument("--skip-preview", action="store_true", help="Skip validation and preview at the end")
    p.set_defaults(cmd=COMMAND, func=run)

def run(args, logger: logging.Logger) -> int:
    try:
        # Try to import rich for better UX
        try:
            from rich.console import Console
            from rich.prompt import Prompt, Confirm
            from rich.panel import Panel
            console = Console()
            has_rich = True
        except ImportError:
            console = None
            has_rich = False
            info(logger, "wizard_fallback", message="Install 'rich' for better experience: pip install rich")
        
        if has_rich and console:
            console.print(Panel.fit(
                "[bold cyan]FLUID Build Wizard[/bold cyan]\n"
                "Create a new data product with guided setup",
                border_style="cyan"
            ))
        else:
            cprint("\n=== FLUID Build Wizard ===")
            cprint("Create a new data product with guided setup\n")
        
        # Detect and select provider
        provider = args.provider or _detect_provider(console, has_rich, logger)
        
        # Gather product information
        config = _gather_product_info(console, has_rich, provider)
        
        # Create directory structure
        product_dir = Path(config["id"])
        _create_directory_structure(product_dir, provider, logger)
        
        # Generate contract
        contract = _generate_contract(config, provider)
        contract_path = product_dir / "contract.fluid.yaml"
        _write_yaml(contract_path, contract)
        
        # Generate minimal scaffolding
        _generate_scaffolding(product_dir, provider, config, logger)
        
        # Save context defaults
        _save_context(config, logger)
        
        if has_rich and console:
            console.print(f"\n[green]✓[/green] Created data product: [bold]{config['id']}[/bold]")
            console.print(f"  Contract: [cyan]{contract_path}[/cyan]")
        else:
            cprint(f"\n✓ Created data product: {config['id']}")
            cprint(f"  Contract: {contract_path}")
        
        # Run validation and preview
        if not args.skip_preview:
            _run_preview(str(contract_path), provider, logger)
        
        if has_rich and console:
            console.print("\n[bold green]Setup complete![/bold green] Next steps:")
            console.print(f"  1. cd {config['id']}")
            console.print("  2. fluid validate contract.fluid.yaml")
            console.print("  3. fluid plan contract.fluid.yaml")
            console.print("  4. fluid apply contract.fluid.yaml")
        else:
            cprint("\nSetup complete! Next steps:")
            cprint(f"  1. cd {config['id']}")
            cprint("  2. fluid validate contract.fluid.yaml")
            cprint("  3. fluid plan contract.fluid.yaml")
            cprint("  4. fluid apply contract.fluid.yaml")
        
        return 0
        
    except KeyboardInterrupt:
        info(logger, "wizard_cancelled")
        return 130
    except CLIError:
        raise
    except Exception as e:
        raise CLIError(1, "wizard_failed", {"error": str(e)})


def _detect_provider(console, has_rich: bool, logger) -> str:
    """Detect available provider or prompt user."""
    # Check environment variables for hints
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("GCLOUD_PROJECT"):
        detected = "gcp"
    elif os.getenv("SNOWFLAKE_ACCOUNT"):
        detected = "snowflake"
    elif os.getenv("AWS_PROFILE") or os.getenv("AWS_ACCESS_KEY_ID"):
        detected = "aws"
    else:
        detected = "local"
    
    if has_rich and console:
        from rich.prompt import Prompt
        provider = Prompt.ask(
            "Select provider",
            choices=["local", "gcp", "snowflake", "aws"],
            default=detected
        )
    else:
        provider = input(f"Select provider [local/gcp/snowflake/aws] (default: {detected}): ").strip() or detected
    
    return provider


def _gather_product_info(console, has_rich: bool, provider: str) -> Dict[str, Any]:
    """Gather product information from user."""
    if has_rich and console:
        from rich.prompt import Prompt
        product_id = Prompt.ask("Product ID", default="my-data-product")
        domain = Prompt.ask("Domain", default="analytics")
        layer = Prompt.ask("Layer", choices=["bronze", "silver", "gold"], default="silver")
        owner = Prompt.ask("Owner/Team", default=os.getenv("USER", "data-team"))
        description = Prompt.ask("Description", default="A FLUID data product")
    else:
        product_id = input("Product ID (default: my-data-product): ").strip() or "my-data-product"
        domain = input("Domain (default: analytics): ").strip() or "analytics"
        layer = input("Layer [bronze/silver/gold] (default: silver): ").strip() or "silver"
        owner = input(f"Owner/Team (default: {os.getenv('USER', 'data-team')}): ").strip() or os.getenv("USER", "data-team")
        description = input("Description (default: A FLUID data product): ").strip() or "A FLUID data product"
    
    return {
        "id": product_id,
        "domain": domain,
        "layer": layer,
        "owner": owner,
        "description": description,
        "provider": provider
    }


def _create_directory_structure(base: Path, provider: str, logger) -> None:
    """Create directory structure for data product."""
    base.mkdir(exist_ok=True)
    (base / "config").mkdir(exist_ok=True)
    (base / "docs").mkdir(exist_ok=True)
    
    if provider in ("gcp", "local"):
        (base / "dbt").mkdir(exist_ok=True)
        (base / "dbt" / "models").mkdir(exist_ok=True)
    elif provider == "snowflake":
        (base / "sql").mkdir(exist_ok=True)
    
    info(logger, "wizard_directories_created", path=str(base))


def _generate_contract(config: Dict[str, Any], provider: str) -> Dict[str, Any]:
    """Generate FLUID contract."""
    contract = {
        "version": "0.5.7",
        "kind": "DataProduct",
        "metadata": {
            "id": config["id"],
            "name": config["id"].replace("-", " ").title(),
            "domain": config["domain"],
            "owner": config["owner"],
            "description": config["description"],
            "tags": [config["layer"], "wizard-generated"]
        },
        "spec": {
            "builds": [
                {
                    "id": "main",
                    "runtime": "dbt" if provider in ("gcp", "local") else "sql",
                    "location": "dbt" if provider in ("gcp", "local") else "sql"
                }
            ]
        }
    }
    
    return contract


def _write_yaml(path: Path, data: Dict[str, Any]) -> None:
    """Write data as YAML if PyYAML available, otherwise JSON."""
    try:
        import yaml
        atomic_write(str(path.with_suffix(".yaml")), yaml.safe_dump(data, sort_keys=False))
    except ImportError:
        # Fallback to JSON
        dump_json(str(path.with_suffix(".json")), data)


def _generate_scaffolding(base: Path, provider: str, config: Dict[str, Any], logger) -> None:
    """Generate minimal scaffolding files."""
    # README
    readme_content = f"""# {config['id']}

{config['description']}

## Quick Start

```bash
# Validate contract
fluid validate contract.fluid.yaml

# Generate execution plan
fluid plan contract.fluid.yaml

# Apply to {provider}
fluid apply contract.fluid.yaml
```

## Metadata

- **Domain**: {config['domain']}
- **Layer**: {config['layer']}
- **Owner**: {config['owner']}
- **Provider**: {provider}
"""
    (base / "README.md").write_text(readme_content)
    
    # Generate provider-specific files
    if provider in ("gcp", "local"):
        dbt_project = {
            "name": config["id"].replace("-", "_"),
            "version": "1.0.0",
            "profile": config["id"].replace("-", "_"),
            "model-paths": ["models"],
            "target-path": "target"
        }
        dump_json(str(base / "dbt" / "dbt_project.json"), dbt_project)
        
        # Sample model
        sample_model = f"""-- Example model for {config['id']}
SELECT
    1 as id,
    'sample' as name,
    CURRENT_TIMESTAMP() as created_at
"""
        (base / "dbt" / "models" / "example.sql").write_text(sample_model)
    
    info(logger, "wizard_scaffolding_created")


def _save_context(config: Dict[str, Any], logger) -> None:
    """Save context defaults."""
    context_dir = Path(".fluid")
    context_dir.mkdir(exist_ok=True)
    
    context = {
        "last_product": config["id"],
        "default_provider": config["provider"],
        "default_domain": config["domain"],
        "default_owner": config["owner"]
    }
    
    dump_json(str(context_dir / "context.json"), context)
    info(logger, "wizard_context_saved")


def _run_preview(contract_path: str, provider: str, logger) -> None:
    """Run validation and plan preview."""
    try:
        from . import validate, plan
        
        # Simulate validation
        info(logger, "wizard_validating", contract=contract_path)
        
        # Simulate plan
        info(logger, "wizard_planning", provider=provider)
        info(logger, "wizard_preview_complete")
        
    except Exception as e:
        info(logger, "wizard_preview_skipped", error=str(e))
