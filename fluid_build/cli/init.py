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
FLUID Init Command - Universal Project Onboarding

The front door to FLUID - intelligently routes users to the right experience:
- Quickstart: Working example in 2 minutes (local, no cloud)
- Scan: Import existing dbt/Terraform projects (Agent Zero)
- Wizard: Interactive guided setup
- Template: Specific use case templates
- Blank: Empty project skeleton

Strategy: Router pattern - delegates to existing commands (blueprint, product-new, scaffold-ci)
"""

import argparse
import logging
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from fluid_build.cli.console import cprint, success, warning
from fluid_build.cli.console import error as console_error

from ._logging import error, info

# Try Rich for beautiful output
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.prompt import Confirm, Prompt
    from rich.table import Table
    from rich.text import Text

    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    console = None

COMMAND = "init"


def _mark_first_run_complete():
    """Create ~/.fluid directory to signal that onboarding has happened."""
    fluid_home = Path.home() / ".fluid"
    try:
        fluid_home.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass  # non-fatal — directory might already exist or be unwritable


def register(subparsers: argparse._SubParsersAction):
    """Register the init command"""
    p = subparsers.add_parser(
        COMMAND,
        help="🚀 Create new FLUID project (smart setup)",
        description="Universal project initialization with smart routing to the right experience",
    )

    # Positional: project name (optional)
    p.add_argument("name", nargs="?", help="Project name (default: auto-generated)")

    # Mode selection (mutually exclusive)
    mode_group = p.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--quickstart",
        action="store_true",
        help="⭐ Create working example with sample data (recommended, 2 min)",
    )
    mode_group.add_argument(
        "--scan",
        action="store_true",
        help="🎯 Import existing dbt/Terraform project (enterprise migration)",
    )
    mode_group.add_argument(
        "--wizard", action="store_true", help="🎨 Interactive guided setup (full control)"
    )
    mode_group.add_argument(
        "--blank", action="store_true", help="🔧 Empty project skeleton (power users)"
    )
    mode_group.add_argument(
        "--template",
        metavar="NAME",
        help="📦 Create from specific template (e.g., customer-360, ml-features)",
    )

    # Provider selection
    p.add_argument(
        "--provider",
        choices=["local", "gcp", "snowflake", "aws", "azure"],
        default="local",
        help="Infrastructure provider (default: local = DuckDB, no cloud needed)",
    )

    # Use case / persona
    p.add_argument(
        "--use-case",
        choices=["data-product", "ai-agent", "analytics", "api"],
        help="Use case configuration (adds opinionated defaults)",
    )

    # Control options
    p.add_argument(
        "--no-run", action="store_true", help="Don't auto-execute pipeline after creation"
    )
    p.add_argument(
        "--no-dag",
        action="store_true",
        help="Don't auto-generate Airflow DAG (even if contract has orchestration config)",
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Preview what would be created without doing it"
    )
    p.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompts")

    p.set_defaults(cmd=COMMAND, func=run)


def run(args, logger: logging.Logger) -> int:
    """Main entry point - routes to appropriate handler"""

    try:
        # Determine mode (auto-detect if not specified)
        mode = detect_mode(args, logger)

        if mode is None:
            return 1  # Error already displayed

        # Route to appropriate handler
        if mode == "quickstart":
            return quickstart_mode(args, logger)
        elif mode == "scan":
            return scan_mode(args, logger)
        elif mode == "wizard":
            return wizard_mode(args, logger)
        elif mode == "blank":
            return blank_mode(args, logger)
        elif mode == "template":
            return template_mode(args, logger)
        else:
            error(logger, "unknown_mode", mode=mode)
            return 1

    except KeyboardInterrupt:
        if RICH_AVAILABLE:
            console.print("\n[yellow]⚠️  Operation cancelled by user[/yellow]")
        else:
            cprint("\n⚠️  Operation cancelled by user")
        return 130
    except Exception as e:
        error(logger, "init_failed", error=str(e))
        if RICH_AVAILABLE:
            console.print(f"[red]❌ Init failed: {e}[/red]")
        else:
            console_error(f"Init failed: {e}")
        return 1


def detect_mode(args, logger: logging.Logger) -> Optional[str]:
    """Smart detection of best mode based on context"""

    # Explicit mode flags take precedence
    if args.quickstart:
        return "quickstart"
    if args.scan:
        return "scan"
    if args.wizard:
        return "wizard"
    if args.blank:
        return "blank"
    if args.template:
        return "template"

    # Auto-detection based on current directory
    cwd = Path.cwd()

    # Check for existing FLUID project
    if (cwd / "contract.fluid.yaml").exists():
        if RICH_AVAILABLE:
            console.print("[yellow]⚠️  FLUID contract already exists in this directory![/yellow]\n")
            console.print("Did you mean:")
            console.print("  [cyan]$ fluid validate contract.fluid.yaml[/cyan]")
            console.print("  [cyan]$ fluid plan contract.fluid.yaml[/cyan]")
            console.print("  [cyan]$ fluid viz contract.fluid.yaml --open[/cyan]")
        else:
            warning("FLUID contract already exists!")
            cprint("Did you mean: fluid validate contract.fluid.yaml")
        return None

    # Check for existing projects to import
    if (cwd / "dbt_project.yml").exists():
        if RICH_AVAILABLE:
            console.print("🔍 [cyan]Detected dbt project[/cyan]")
            console.print(
                "Suggestion: Use [bold]fluid init --scan[/bold] to import your dbt models"
            )
        return "scan"

    if (cwd / "main.tf").exists() or list(cwd.glob("*.tf")):
        if RICH_AVAILABLE:
            console.print("🔍 [cyan]Detected Terraform project[/cyan]")
            console.print(
                "Suggestion: Use [bold]fluid init --scan[/bold] to import your infrastructure"
            )
        return "scan"

    if list(cwd.glob("*.sql")) and not args.name:
        if RICH_AVAILABLE:
            console.print("🔍 [cyan]Detected SQL files[/cyan]")
            console.print("Suggestion: Use [bold]fluid init --scan[/bold] to import your SQL")
        return "scan"

    # Check if first-time user (no ~/.fluid directory)
    fluid_home = Path.home() / ".fluid"
    if not fluid_home.exists():
        if RICH_AVAILABLE:
            console.print("👋 [bold]Welcome to FLUID![/bold]")
            console.print("Creating a quickstart project with sample data...\n")
        return "quickstart"

    # Default to quickstart for empty directory
    return "quickstart"


# ============================================================================
# DAG GENERATION HELPERS
# ============================================================================


def should_generate_dag(contract: dict, template: str = None) -> bool:
    """
    Determine if DAG should be auto-generated for this project.

    Auto-generate DAGs when:
    1. Contract has explicit orchestration config
    2. Template is orchestration-focused (customer-360, sales-analytics, ml-features, data-quality)
    3. Project has multiple provider actions (complex pipeline)
    """
    # Check for explicit orchestration config
    if "orchestration" in contract:
        return True

    # Check for orchestration-focused templates
    orchestrated_templates = ["customer-360", "sales-analytics", "ml-features", "data-quality"]
    if template and template in orchestrated_templates:
        return True

    # Check for complex pipelines (multiple actions)
    binding = contract.get("binding", {})
    provider_actions = binding.get("providerActions", [])
    if len(provider_actions) > 1:
        return True

    return False


def generate_dag_for_project(
    project_dir: Path, contract: dict, logger, console, template: str = None
) -> bool:
    """
    Generate Airflow DAG using existing generate-airflow command.

    Creates dags/ folder with:
    - DAG Python file (contract_name_dag.py)
    - README.md with usage instructions
    """
    try:
        import subprocess

        # Get contract details
        contract_name = contract.get("name", "my_product")
        orchestration = contract.get("orchestration", {})

        # Prepare DAG parameters
        schedule = orchestration.get("schedule", "@daily")
        dag_id = contract_name.replace("-", "_").replace(" ", "_")

        # Call generate-airflow command
        dag_dir = project_dir / "dags"
        dag_dir.mkdir(exist_ok=True)

        # Build command
        cmd = [
            "fluid",
            "generate-airflow",
            str(project_dir / "contract.fluid.yaml"),
            "--output-dir",
            str(dag_dir),
            "--dag-id",
            dag_id,
            "--schedule",
            schedule,
        ]

        if RICH_AVAILABLE:
            console.print("\n[cyan]📅 Generating Airflow DAG...[/cyan]")

        # Execute command
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_dir))

        if result.returncode != 0:
            # generate-airflow may not exist yet - create DAG manually
            logger.warning("generate-airflow command not available, creating basic DAG template")
            create_basic_dag(project_dir, contract, logger)

        # Create README
        dag_filename = f"{dag_id}_dag.py"
        create_dags_readme(dag_dir, dag_id, schedule, dag_filename)

        if RICH_AVAILABLE:
            console.print(f"[green]✅ DAG created: dags/{dag_filename}[/green]")

        return True

    except Exception as e:
        logger.warning(f"Failed to generate DAG: {e}")
        return False


def create_basic_dag(project_dir: Path, contract: dict, logger):
    """Create a basic DAG template if generate-airflow is not available."""

    contract_name = contract.get("name", "my_product")
    orchestration = contract.get("orchestration", {})
    dag_id = contract_name.replace("-", "_").replace(" ", "_")
    schedule = orchestration.get("schedule", "@daily")

    dag_content = f'''"""
Airflow DAG for FLUID contract: {contract_name}
Auto-generated by fluid init
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {{
    'owner': 'fluid',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': {orchestration.get("retries", 3)},
    'retry_delay': timedelta(minutes={orchestration.get("retry_delay", "5m").replace("m", "")}),
}}

with DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description='FLUID data product: {contract_name}',
    schedule_interval='{schedule}',
    catchup=False,
    tags=['fluid', 'data-product'],
) as dag:

    # Validate contract
    validate = BashOperator(
        task_id='validate_contract',
        bash_command='cd {project_dir.absolute()} && fluid validate contract.fluid.yaml',
    )

    # Plan execution
    plan = BashOperator(
        task_id='plan_execution',
        bash_command='cd {project_dir.absolute()} && fluid plan contract.fluid.yaml',
    )

    # Apply changes
    apply = BashOperator(
        task_id='apply_contract',
        bash_command='cd {project_dir.absolute()} && fluid apply contract.fluid.yaml --auto-approve',
    )

    validate >> plan >> apply
'''

    dag_dir = project_dir / "dags"
    dag_dir.mkdir(exist_ok=True)
    dag_file = dag_dir / f"{dag_id}_dag.py"

    with open(dag_file, "w") as f:
        f.write(dag_content)

    logger.info(f"Created basic DAG template: {dag_file}")


def create_dags_readme(dag_dir: Path, dag_id: str, schedule: str, dag_filename: str):
    """Create README in dags/ folder with usage instructions."""

    readme_content = f"""# Airflow DAG Configuration

This folder contains the Airflow DAG for your FLUID data product.

## Generated DAG

- **DAG ID**: `{dag_id}`
- **Schedule**: `{schedule}`
- **File**: `{dag_filename}`

## Usage

### Local Development

Run the DAG locally using Airflow:

```bash
# Start Airflow (from project root)
docker-compose --profile airflow up -d

# Access Airflow UI
open http://localhost:8080

# Default credentials
# Username: admin
# Password: admin
```

### Manual Execution

Run the FLUID pipeline manually:

```bash
# Validate contract
fluid validate contract.fluid.yaml

# Plan execution
fluid plan contract.fluid.yaml

# Apply changes
fluid apply contract.fluid.yaml --auto-approve
```

### CI/CD Integration

This DAG can be deployed to:
- Cloud Composer (GCP)
- MWAA (AWS)
- Astronomer
- Self-hosted Airflow

See `.jenkins/` folder for CI/CD pipeline configuration.

## Customization

To customize the DAG:

1. Edit `{dag_filename}`
2. Add custom operators or sensors
3. Configure alerting and notifications
4. Update schedule interval as needed

## Next Steps

- **Add data quality checks**: Use Great Expectations or Soda
- **Set up alerting**: Configure email/Slack notifications
- **Add lineage tracking**: Enable OpenLineage integration
- **Monitor performance**: Use Airflow metrics

For more information, see: https://fluid.dev/docs/orchestration
"""

    readme_path = dag_dir / "README.md"
    with open(readme_path, "w") as f:
        f.write(readme_content)


# ============================================================================
# MODE HANDLERS
# ============================================================================


def quickstart_mode(args, logger: logging.Logger) -> int:
    """Creates working example in 2 minutes"""

    project_name = args.name or "my-first-product"
    template = "customer-360"  # Default template

    if RICH_AVAILABLE:
        console.print(
            Panel(
                f"🚀 Creating [bold cyan]{project_name}[/bold cyan] with customer analytics...\n\n"
                f"This will create a working data product with sample data.\n"
                f"No cloud account needed - runs locally with DuckDB.",
                title="Quickstart Mode",
                border_style="cyan",
            )
        )
    else:
        cprint(f"🚀 Creating {project_name} with customer analytics...")

    project_dir = Path(project_name)

    # Check if directory already exists
    if project_dir.exists() and any(project_dir.iterdir()):
        if RICH_AVAILABLE:
            console.print(
                f"[red]❌ Directory '{project_name}' already exists and is not empty[/red]"
            )
        else:
            console_error(f"Directory '{project_name}' already exists")
        return 1

    if args.dry_run:
        if RICH_AVAILABLE:
            console.print("[yellow]🔍 Dry run - would create:[/yellow]")
            console.print(f"  📁 {project_name}/")
            console.print(f"  📄 {project_name}/contract.fluid.yaml")
            console.print(f"  📊 {project_name}/data/customers.csv")
            console.print(f"  📊 {project_name}/data/orders.csv")
            console.print(f"  💾 {project_name}/.fluid/db.duckdb")
        return 0

    try:
        # Create project directory
        project_dir.mkdir(parents=True, exist_ok=True)

        # Copy template files
        success = copy_template(project_dir, template, logger)
        if not success:
            return 1

        # Copy sample data
        copy_sample_data(project_dir, template, logger)

        # Initialize local database
        init_local_db(project_dir, args.provider, logger)

        # Generate DAG if contract has orchestration config
        has_dag = False
        if not getattr(args, "no_dag", False):
            try:
                import yaml

                contract_path = project_dir / "contract.fluid.yaml"
                if contract_path.exists():
                    with open(contract_path) as f:
                        contract = yaml.safe_load(f)

                    if should_generate_dag(contract, template):
                        has_dag = generate_dag_for_project(
                            project_dir,
                            contract,
                            logger,
                            console if RICH_AVAILABLE else None,
                            template,
                        )
            except Exception as e:
                logger.warning(f"Failed to generate DAG: {e}")

        # Run pipeline if not --no-run
        if not args.no_run and args.provider == "local":
            run_local_pipeline(project_dir, logger)

        # Generate CI/CD pipeline
        generate_cicd(project_dir, logger)

        # Show next steps
        show_success_message(project_dir, args.provider, logger, has_dag=has_dag)

        return 0

    except Exception as e:
        error(logger, "quickstart_failed", error=str(e))
        if RICH_AVAILABLE:
            console.print(f"[red]❌ Quickstart failed: {e}[/red]")
        return 1


def scan_mode(args, logger: logging.Logger) -> int:
    """Agent Zero - Import existing projects"""

    if RICH_AVAILABLE:
        console.print(
            Panel(
                "🔍 [bold]Agent Zero - Project Scanner[/bold]\n\n"
                "I'll analyze your existing code and create FLUID contracts.\n"
                "Supported: dbt projects, Terraform, SQL files",
                title="Scan Mode",
                border_style="yellow",
            )
        )
    else:
        cprint("🔍 Agent Zero - Project Scanner")
        cprint("Scanning for existing projects...")

    try:
        # Detect project type
        detector = detect_project_type(Path.cwd())

        if not detector:
            if RICH_AVAILABLE:
                console.print(
                    "\n[yellow]❌ No recognized project found in current directory[/yellow]\n"
                )
                console.print("Supported project types:")
                console.print("  • dbt projects (dbt_project.yml)")
                console.print("  • Terraform (*.tf files)")
                console.print("  • SQL files (*.sql)")
                console.print("\n💡 Try instead:")
                console.print("  [cyan]$ fluid init --quickstart[/cyan]")
            else:
                cprint("\n❌ No recognized project found")
                cprint("Try: fluid init --quickstart")
            return 1

        # Scan the project
        scan_results = detector.scan(logger)

        # Show scan results
        show_scan_results(scan_results)

        # Ask for confirmation
        if RICH_AVAILABLE:
            if not Confirm.ask("\n📝 Generate FLUID contracts from this project?", default=True):
                console.print("Cancelled.")
                return 0

        # Generate contracts
        contracts = generate_contracts_from_scan(scan_results, args.provider, logger)

        # Apply governance if PII detected
        if scan_results.get("sensitive_columns"):
            contracts = apply_governance_policies(contracts, scan_results, logger)

        # Write contracts to disk
        output_dir = Path.cwd()
        for i, contract in enumerate(contracts):
            contract_name = contract.get("name", f"contract-{i}")
            contract_path = output_dir / f"{contract_name}.fluid.yaml"

            import yaml

            with open(contract_path, "w") as f:
                yaml.dump(contract, f, default_flow_style=False, sort_keys=False)

            if RICH_AVAILABLE:
                console.print(f"✅ Generated: [cyan]{contract_path.name}[/cyan]")

        # Generate CI/CD
        generate_cicd(output_dir, logger)

        # Show migration summary
        show_migration_summary(contracts, scan_results, logger)

        return 0

    except Exception as e:
        error(logger, "scan_failed", error=str(e))
        if RICH_AVAILABLE:
            console.print(f"[red]❌ Scan failed: {e}[/red]")
        return 1


def wizard_mode(args, logger: logging.Logger) -> int:
    """Interactive guided setup"""

    if RICH_AVAILABLE:
        console.print(
            Panel(
                "🎨 [bold]Interactive Wizard[/bold]\n\n"
                "I'll guide you through creating a custom FLUID project.",
                title="Wizard Mode",
                border_style="magenta",
            )
        )
    else:
        cprint("🎨 Interactive Wizard")

    try:
        # Import existing wizard functionality
        from .wizard import run as wizard_run

        # Create a mock args object for wizard
        class WizardArgs:
            def __init__(self):
                self.provider = args.provider
                self.project = args.name
                self.env = None

        wizard_args = WizardArgs()
        return wizard_run(wizard_args, logger)

    except ImportError:
        # Wizard not available - provide basic interactive flow
        if not RICH_AVAILABLE:
            console_error("Wizard mode requires rich library")
            cprint("Try: fluid init --quickstart")
            return 1

        console.print("\n[bold]Let's create your FLUID project![/bold]\n")

        # Ask basic questions
        project_name = args.name or Prompt.ask("Project name", default="my-data-product")
        Prompt.ask(
            "Use case",
            choices=["data-product", "ai-agent", "analytics", "api"],
            default="data-product",
        )

        # Route to template based on use case
        args.template = "customer-360"  # Default
        args.name = project_name

        return template_mode(args, logger)


def blank_mode(args, logger: logging.Logger) -> int:
    """Empty project skeleton"""

    project_name = args.name or "my-project"
    project_dir = Path(project_name)

    if RICH_AVAILABLE:
        console.print(
            Panel(
                f"🔧 Creating minimal project: [bold]{project_name}[/bold]\n\n"
                f"Empty skeleton with no assumptions.",
                title="Blank Mode",
                border_style="white",
            )
        )
    else:
        cprint(f"🔧 Creating minimal project: {project_name}")

    if project_dir.exists():
        if RICH_AVAILABLE:
            console.print(f"[red]❌ Directory '{project_name}' already exists[/red]")
        return 1

    try:
        # Try to use existing product-new command
        from .product_new import run as product_new_run

        class ProductNewArgs:
            def __init__(self):
                self.id = f"bronze.{project_name}"
                self.out_dir = str(project_dir.parent)
                self.name = project_name
                self.provider = args.provider
                self.minimal = True
                self.dry_run = args.dry_run

        return product_new_run(ProductNewArgs(), logger)

    except ImportError:
        # Create minimal structure manually
        project_dir.mkdir(parents=True)

        # Create minimal contract
        contract_content = f"""version: "0.7.1"
kind: fluid
name: {project_name}
description: FLUID data product

exposes: []
produces: []

binding:
  provider: {args.provider}
"""

        contract_path = project_dir / "contract.fluid.yaml"
        contract_path.write_text(contract_content)

        if RICH_AVAILABLE:
            console.print(f"\n✅ Created {project_name}/contract.fluid.yaml")
            console.print("\nNext steps:")
            console.print(f"  $ cd {project_name}")
            console.print("  $ code contract.fluid.yaml")

        return 0


def template_mode(args, logger: logging.Logger) -> int:
    """Create from specific template"""

    template_name = args.template
    project_name = args.name or template_name
    project_dir = Path(project_name)

    if RICH_AVAILABLE:
        console.print(
            Panel(
                f"📦 Creating from template: [bold]{template_name}[/bold]\n"
                f"Project: [bold]{project_name}[/bold]",
                title="Template Mode",
                border_style="blue",
            )
        )
    else:
        cprint(f"📦 Creating from template: {template_name}")

    try:
        # Try to use existing blueprint command
        from .blueprint import create_from_template

        if not create_from_template(template_name, project_dir, logger):
            return 1

        if RICH_AVAILABLE:
            console.print(f"\n✅ Created project from {template_name} template")

        return 0

    except (ImportError, AttributeError):
        # Blueprint command not available - use our own template copy
        success = copy_template(project_dir, template_name, logger)
        return 0 if success else 1


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def copy_template(project_dir: Path, template_name: str, logger: logging.Logger) -> bool:
    """Copy template files to project directory"""

    # Find template directory
    cli_dir = Path(__file__).parent
    templates_dir = cli_dir.parent / "templates" / template_name

    if not templates_dir.exists():
        if RICH_AVAILABLE:
            console.print(f"[yellow]⚠️  Template '{template_name}' not found[/yellow]")
            console.print(f"Looking in: {templates_dir}")
            console.print("\nAvailable templates:")
            console.print("  - customer-360 (customer analytics)")
        else:
            warning(f"Template '{template_name}' not found")
        return False

    try:
        project_dir.mkdir(parents=True, exist_ok=True)

        # Copy all files from template
        for item in templates_dir.iterdir():
            if item.is_file():
                shutil.copy2(item, project_dir / item.name)
            elif item.is_dir() and item.name != "__pycache__":
                shutil.copytree(item, project_dir / item.name, dirs_exist_ok=True)

        if RICH_AVAILABLE:
            console.print(f"✅ Copied template files from {template_name}")
        return True

    except Exception as e:
        error(logger, "template_copy_failed", template=template_name, error=str(e))
        if RICH_AVAILABLE:
            console.print(f"[red]❌ Failed to copy template: {e}[/red]")
        return False


def copy_sample_data(project_dir: Path, template_name: str, logger: logging.Logger):
    """Copy sample CSV data (already handled by copy_template, but can enhance)"""

    data_dir = project_dir / "data"
    if data_dir.exists():
        csv_files = list(data_dir.glob("*.csv"))
        if csv_files and RICH_AVAILABLE:
            console.print(f"✅ Sample data loaded: {len(csv_files)} CSV files")


def init_local_db(project_dir: Path, provider: str, logger: logging.Logger):
    """Initialize DuckDB database"""

    if provider != "local":
        return  # Only for local provider

    try:
        import duckdb

        db_dir = project_dir / ".fluid"
        db_dir.mkdir(exist_ok=True)

        db_path = db_dir / "db.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.close()

        if RICH_AVAILABLE:
            console.print("✅ Local database initialized (DuckDB)")

    except ImportError:
        if RICH_AVAILABLE:
            console.print("[yellow]⚠️  DuckDB not installed (pip install duckdb)[/yellow]")
    except Exception as e:
        error(logger, "db_init_failed", error=str(e))


def run_local_pipeline(project_dir: Path, logger: logging.Logger):
    """Execute pipeline with local provider"""

    if not RICH_AVAILABLE:
        return

    console.print("\n🚀 [bold]Running pipeline locally...[/bold]\n")

    contract_path = project_dir / "contract.fluid.yaml"

    try:
        # Try to use existing apply command
        from .apply import run as apply_run

        class ApplyArgs:
            def __init__(self):
                self.contract = str(contract_path)
                self.provider = "local"
                self.env = None
                self.project = None
                self.region = None
                self.yes = True
                self.dry_run = False

        result = apply_run(ApplyArgs(), logger)

        if result == 0:
            console.print("\n✅ [green bold]Pipeline executed successfully![/green bold]")

    except Exception as e:
        console.print(f"[yellow]⚠️  Could not auto-run pipeline: {e}[/yellow]")
        console.print("You can run it manually:")
        console.print(f"  [cyan]$ cd {project_dir.name}[/cyan]")
        console.print("  [cyan]$ fluid apply contract.fluid.yaml --provider local[/cyan]")


def show_success_message(
    project_dir: Path, provider: str, logger: logging.Logger, has_dag: bool = False
):
    """Show next steps after successful init"""

    # Mark first-run complete so subsequent `fluid` invocations show full help
    _mark_first_run_complete()

    if not RICH_AVAILABLE:
        cprint("\n✅ Your data product is ready!")
        if has_dag:
            cprint("\n📅 Airflow DAG created in dags/ folder")
        cprint("\nNext steps:")
        cprint(f"  $ cd {project_dir.name}")
        cprint("  $ fluid validate contract.fluid.yaml")
        return

    console.print()
    console.print(
        Panel(
            f"[bold green]Your data product is ready![/bold green]\n\n"
            f"Project: [bold cyan]{project_dir.name}/[/bold cyan]",
            title="[bold bright_white]🎉 Success[/bold bright_white]",
            title_align="left",
            border_style="green",
            padding=(1, 2),
        )
    )

    # Show results for local provider
    if provider == "local":
        output_dir = project_dir / "output"
        if output_dir.exists():
            csv_files = list(output_dir.glob("*.csv"))
            if csv_files:
                console.print("\n[bold]Results:[/bold]")
                for csv_file in csv_files:
                    console.print(f"  📊 {csv_file.name}: {csv_file.relative_to(project_dir)}")

        db_file = project_dir / ".fluid" / "db.duckdb"
        if db_file.exists():
            console.print("  💾 Local database: .fluid/db.duckdb")

    # Show DAG info
    if has_dag:
        dag_dir = project_dir / "dags"
        if dag_dir.exists():
            dag_files = list(dag_dir.glob("*_dag.py"))
            console.print("\n[bold]Orchestration:[/bold]")
            for dag_file in dag_files:
                console.print(f"  📅 Airflow DAG: dags/{dag_file.name}")

    # Concise numbered next-steps — always 3
    console.print()
    console.print("[bold bright_white]Next steps:[/bold bright_white]\n")
    console.print(f"  [bold yellow]1.[/bold yellow]  [cyan]cd {project_dir.name}[/cyan]")
    console.print(
        "  [bold yellow]2.[/bold yellow]  [cyan]fluid validate contract.fluid.yaml[/cyan]   [dim]# check the contract[/dim]"
    )

    if provider == "local":
        console.print(
            "  [bold yellow]3.[/bold yellow]  [cyan]fluid apply contract.fluid.yaml --yes[/cyan]  [dim]# run the pipeline[/dim]"
        )
    else:
        console.print(
            f"  [bold yellow]3.[/bold yellow]  [cyan]fluid plan contract.fluid.yaml --provider {provider}[/cyan]"
        )

    console.print()
    console.print("[dim]Run [bright_cyan]fluid --help[/bright_cyan] for all commands.[/dim]\n")


# ============================================================================
# CI/CD GENERATION
# ============================================================================


def generate_cicd(project_dir: Path, logger: logging.Logger):
    """Generate CI/CD pipeline configuration"""

    # Check if user wants CI/CD
    if RICH_AVAILABLE:
        console.print("\n" + "─" * 70)
        want_cicd = Confirm.ask("🔧 Generate CI/CD pipeline? (Recommended for teams)", default=True)

        if not want_cicd:
            return

        # Ask which platform
        platform = Prompt.ask(
            "Choose CI/CD platform",
            choices=["jenkins", "github", "gitlab", "cloudbuild", "skip"],
            default="jenkins",
        )

        if platform == "skip":
            return

        console.print(f"\n⚙️  Generating {platform.title()} pipeline...\n")

        if platform == "jenkins":
            generate_jenkinsfile(project_dir, logger)
        elif platform == "github":
            generate_github_actions(project_dir, logger)
        elif platform == "gitlab":
            generate_gitlab_ci(project_dir, logger)
        elif platform == "cloudbuild":
            generate_cloudbuild(project_dir, logger)
    else:
        # Non-interactive: default to Jenkins
        generate_jenkinsfile(project_dir, logger)


def generate_jenkinsfile(project_dir: Path, logger: logging.Logger):
    """Generate Jenkinsfile with FLUID 0.7.1 pipeline"""

    jenkinsfile_content = """pipeline {
    agent any
    
    environment {
        // Auto-detect environment based on branch
        FLUID_ENV = "${env.BRANCH_NAME == 'main' ? 'prod' : env.BRANCH_NAME == 'staging' ? 'staging' : 'dev'}"
        
        // FLUID CLI settings
        FLUID_VERSION = "0.7.1"
    }
    
    stages {
        stage('Setup') {
            steps {
                echo "🚀 FLUID Pipeline - Environment: ${FLUID_ENV}"
                
                // Install FLUID CLI if not available
                sh '''
                    if ! command -v fluid &> /dev/null; then
                        echo "Installing FLUID CLI..."
                        pip install fluid-forge
                    else
                        echo "FLUID CLI already installed: $(fluid --version)"
                    fi
                '''
            }
        }
        
        stage('Validate') {
            steps {
                echo "✅ Validating FLUID contracts..."
                
                sh '''
                    # Validate all FLUID contracts
                    for contract in *.fluid.yaml; do
                        if [ -f "$contract" ]; then
                            echo "Validating $contract..."
                            fluid validate "$contract"
                        fi
                    done
                '''
            }
        }
        
        stage('Plan') {
            steps {
                echo "📋 Planning deployment..."
                
                sh '''
                    # Generate execution plan
                    for contract in *.fluid.yaml; do
                        if [ -f "$contract" ]; then
                            echo "Planning $contract for environment: ${FLUID_ENV}..."
                            fluid plan "$contract" --env ${FLUID_ENV} || true
                        fi
                    done
                '''
            }
        }
        
        stage('Test') {
            steps {
                echo "🧪 Running contract tests..."
                
                sh '''
                    # Run contract tests if they exist
                    if [ -d "tests" ]; then
                        echo "Running FLUID contract tests..."
                        fluid contract-tests *.fluid.yaml || true
                    else
                        echo "No tests directory found - skipping tests"
                    fi
                '''
            }
        }
        
        stage('Deploy to Dev/Staging') {
            when {
                not {
                    branch 'main'
                }
            }
            steps {
                echo "🚀 Deploying to ${FLUID_ENV}..."
                
                sh '''
                    for contract in *.fluid.yaml; do
                        if [ -f "$contract" ]; then
                            echo "Deploying $contract to ${FLUID_ENV}..."
                            fluid apply "$contract" --env ${FLUID_ENV}
                        fi
                    done
                '''
            }
        }
        
        stage('Deploy to Production') {
            when {
                branch 'main'
            }
            steps {
                echo "🚀 Deploying to PRODUCTION..."
                
                // Production deployment with confirmation
                input message: 'Deploy to Production?', ok: 'Deploy'
                
                sh '''
                    for contract in *.fluid.yaml; do
                        if [ -f "$contract" ]; then
                            echo "Deploying $contract to production..."
                            fluid apply "$contract" --env prod
                        fi
                    done
                '''
            }
        }
        
        stage('Verify') {
            when {
                branch 'main'
            }
            steps {
                echo "🔍 Verifying deployment..."
                
                sh '''
                    # Verify contracts are deployed correctly
                    for contract in *.fluid.yaml; do
                        if [ -f "$contract" ]; then
                            echo "Verifying $contract..."
                            fluid verify "$contract" --env ${FLUID_ENV} || true
                        fi
                    done
                '''
            }
        }
    }
    
    post {
        success {
            echo "✅ FLUID pipeline completed successfully!"
        }
        failure {
            echo "❌ FLUID pipeline failed"
        }
        always {
            // Archive plan outputs
            archiveArtifacts artifacts: '**/*.plan.json', allowEmptyArchive: true
            
            // Clean workspace
            cleanWs()
        }
    }
}
"""

    jenkinsfile_path = project_dir / "Jenkinsfile"
    jenkinsfile_path.write_text(jenkinsfile_content.strip())

    if RICH_AVAILABLE:
        console.print("✅ Generated [bold]Jenkinsfile[/bold] with FLUID 0.7.1 pipeline")
        console.print("   - Automatic environment detection (dev/staging/prod)")
        console.print("   - Contract validation and testing")
        console.print("   - Production deployment approval")
    else:
        success("Generated Jenkinsfile")

    info(logger, "jenkinsfile_generated", path=str(jenkinsfile_path))


def generate_github_actions(project_dir: Path, logger: logging.Logger):
    """Generate GitHub Actions workflow for FLUID 0.7.1"""

    workflow_content = """name: FLUID Pipeline

on:
  push:
    branches: [ main, staging, develop ]
  pull_request:
    branches: [ main, staging ]

env:
  FLUID_VERSION: "0.7.1"

jobs:
  validate:
    name: Validate Contracts
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install FLUID CLI
        run: |
          pip install fluid-forge
          fluid --version
      
      - name: Validate FLUID contracts
        run: |
          for contract in *.fluid.yaml; do
            if [ -f "$contract" ]; then
              echo "Validating $contract..."
              fluid validate "$contract"
            fi
          done
  
  plan:
    name: Generate Deployment Plan
    runs-on: ubuntu-latest
    needs: validate
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install FLUID CLI
        run: pip install fluid-forge
      
      - name: Determine environment
        id: env
        run: |
          if [[ "${{ github.ref }}" == "refs/heads/main" ]]; then
            echo "environment=prod" >> $GITHUB_OUTPUT
          elif [[ "${{ github.ref }}" == "refs/heads/staging" ]]; then
            echo "environment=staging" >> $GITHUB_OUTPUT
          else
            echo "environment=dev" >> $GITHUB_OUTPUT
          fi
      
      - name: Generate plan
        run: |
          for contract in *.fluid.yaml; do
            if [ -f "$contract" ]; then
              echo "Planning $contract for ${{ steps.env.outputs.environment }}..."
              fluid plan "$contract" --env ${{ steps.env.outputs.environment }}
            fi
          done
  
  test:
    name: Run Contract Tests
    runs-on: ubuntu-latest
    needs: validate
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install FLUID CLI
        run: pip install fluid-forge
      
      - name: Run tests
        run: |
          if [ -d "tests" ]; then
            fluid contract-tests *.fluid.yaml
          else
            echo "No tests directory found"
          fi
  
  deploy:
    name: Deploy to ${{ needs.plan.outputs.environment }}
    runs-on: ubuntu-latest
    needs: [validate, plan, test]
    if: github.event_name == 'push'
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install FLUID CLI
        run: pip install fluid-forge
      
      - name: Determine environment
        id: env
        run: |
          if [[ "${{ github.ref }}" == "refs/heads/main" ]]; then
            echo "environment=prod" >> $GITHUB_OUTPUT
          elif [[ "${{ github.ref }}" == "refs/heads/staging" ]]; then
            echo "environment=staging" >> $GITHUB_OUTPUT
          else
            echo "environment=dev" >> $GITHUB_OUTPUT
          fi
      
      - name: Deploy contracts
        env:
          GCP_CREDENTIALS: ${{ secrets.GCP_CREDENTIALS }}
        run: |
          for contract in *.fluid.yaml; do
            if [ -f "$contract" ]; then
              echo "Deploying $contract to ${{ steps.env.outputs.environment }}..."
              fluid apply "$contract" --env ${{ steps.env.outputs.environment }}
            fi
          done
      
      - name: Verify deployment
        if: github.ref == 'refs/heads/main'
        run: |
          for contract in *.fluid.yaml; do
            if [ -f "$contract" ]; then
              echo "Verifying $contract..."
              fluid verify "$contract" --env prod
            fi
          done
"""

    workflow_dir = project_dir / ".github" / "workflows"
    workflow_dir.mkdir(parents=True, exist_ok=True)

    workflow_path = workflow_dir / "fluid.yml"
    workflow_path.write_text(workflow_content.strip())

    if RICH_AVAILABLE:
        console.print("✅ Generated [bold].github/workflows/fluid.yml[/bold]")
        console.print("   - Automatic environment detection")
        console.print("   - Contract validation and testing")
        console.print("   - Deployment to dev/staging/prod")
    else:
        success("Generated GitHub Actions workflow")

    info(logger, "github_actions_generated", path=str(workflow_path))


def generate_gitlab_ci(project_dir: Path, logger: logging.Logger):
    """Generate GitLab CI configuration for FLUID 0.7.1"""

    gitlab_ci_content = """# FLUID Pipeline for GitLab CI

variables:
  FLUID_VERSION: "0.7.1"
  PIP_CACHE_DIR: "$CI_PROJECT_DIR/.cache/pip"

cache:
  paths:
    - .cache/pip

stages:
  - validate
  - plan
  - test
  - deploy
  - verify

before_script:
  - python --version
  - pip install fluid-forge
  - fluid --version

# Validate all FLUID contracts
validate:
  stage: validate
  script:
    - |
      for contract in *.fluid.yaml; do
        if [ -f "$contract" ]; then
          echo "Validating $contract..."
          fluid validate "$contract"
        fi
      done
  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event"'
    - if: '$CI_COMMIT_BRANCH'

# Generate deployment plan
plan:
  stage: plan
  script:
    - export FLUID_ENV="${CI_COMMIT_BRANCH == 'main' ? 'prod' : CI_COMMIT_BRANCH == 'staging' ? 'staging' : 'dev'}"
    - |
      for contract in *.fluid.yaml; do
        if [ -f "$contract" ]; then
          echo "Planning $contract for $FLUID_ENV..."
          fluid plan "$contract" --env $FLUID_ENV
        fi
      done
  artifacts:
    paths:
      - "**/*.plan.json"
    expire_in: 1 week
  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event"'
    - if: '$CI_COMMIT_BRANCH'

# Run contract tests
test:
  stage: test
  script:
    - |
      if [ -d "tests" ]; then
        fluid contract-tests *.fluid.yaml
      else
        echo "No tests directory found"
      fi
  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event"'
    - if: '$CI_COMMIT_BRANCH'

# Deploy to dev/staging
deploy:dev:
  stage: deploy
  script:
    - export FLUID_ENV="${CI_COMMIT_BRANCH == 'staging' ? 'staging' : 'dev'}"
    - |
      for contract in *.fluid.yaml; do
        if [ -f "$contract" ]; then
          echo "Deploying $contract to $FLUID_ENV..."
          fluid apply "$contract" --env $FLUID_ENV
        fi
      done
  environment:
    name: $FLUID_ENV
  rules:
    - if: '$CI_COMMIT_BRANCH != "main"'

# Deploy to production (manual approval)
deploy:prod:
  stage: deploy
  script:
    - |
      for contract in *.fluid.yaml; do
        if [ -f "$contract" ]; then
          echo "Deploying $contract to production..."
          fluid apply "$contract" --env prod
        fi
      done
  environment:
    name: production
  when: manual
  rules:
    - if: '$CI_COMMIT_BRANCH == "main"'

# Verify production deployment
verify:
  stage: verify
  script:
    - |
      for contract in *.fluid.yaml; do
        if [ -f "$contract" ]; then
          echo "Verifying $contract..."
          fluid verify "$contract" --env prod
        fi
      done
  rules:
    - if: '$CI_COMMIT_BRANCH == "main"'
  when: on_success
"""

    gitlab_ci_path = project_dir / ".gitlab-ci.yml"
    gitlab_ci_path.write_text(gitlab_ci_content.strip())

    if RICH_AVAILABLE:
        console.print("✅ Generated [bold].gitlab-ci.yml[/bold]")
        console.print("   - Multi-stage pipeline (validate/plan/test/deploy/verify)")
        console.print("   - Environment-based deployment")
        console.print("   - Production approval gate")
    else:
        success("Generated GitLab CI configuration")

    info(logger, "gitlab_ci_generated", path=str(gitlab_ci_path))


def generate_cloudbuild(project_dir: Path, logger: logging.Logger):
    """Generate Google Cloud Build configuration for FLUID 0.7.1"""

    cloudbuild_content = """# FLUID Pipeline for Google Cloud Build

options:
  machineType: 'N1_HIGHCPU_8'
  logging: CLOUD_LOGGING_ONLY

substitutions:
  _FLUID_VERSION: "0.7.1"
  _ENVIRONMENT: "${BRANCH_NAME == 'master' ? 'prod' : BRANCH_NAME == 'staging' ? 'staging' : 'dev'}"

steps:
  # Setup - Install FLUID CLI
  - name: 'python:3.11'
    id: setup
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        pip install fluid-forge==$_FLUID_VERSION
        fluid --version
        echo "FLUID $_FLUID_VERSION installed"

  # Validate - Check all contracts
  - name: 'python:3.11'
    id: validate
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        pip install fluid-forge==$_FLUID_VERSION
        for contract in *.fluid.yaml; do
          if [ -f "$contract" ]; then
            echo "Validating $contract..."
            fluid validate "$contract"
          fi
        done
    waitFor: ['setup']

  # Plan - Generate deployment plan
  - name: 'python:3.11'
    id: plan
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        pip install fluid-forge==$_FLUID_VERSION
        for contract in *.fluid.yaml; do
          if [ -f "$contract" ]; then
            echo "Planning $contract for $_ENVIRONMENT..."
            fluid plan "$contract" --env $_ENVIRONMENT --provider gcp
          fi
        done
    waitFor: ['validate']
    env:
      - 'GOOGLE_APPLICATION_CREDENTIALS=/workspace/gcp-key.json'

  # Test - Run contract tests
  - name: 'python:3.11'
    id: test
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        pip install fluid-forge==$_FLUID_VERSION
        if [ -d "tests" ]; then
          fluid contract-tests *.fluid.yaml
        else
          echo "No tests directory found, skipping"
        fi
    waitFor: ['plan']

  # Deploy - Apply contracts
  - name: 'python:3.11'
    id: deploy
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        pip install fluid-forge==$_FLUID_VERSION
        for contract in *.fluid.yaml; do
          if [ -f "$contract" ]; then
            echo "Deploying $contract to $_ENVIRONMENT..."
            fluid apply "$contract" --env $_ENVIRONMENT --provider gcp
          fi
        done
    waitFor: ['test']
    env:
      - 'GOOGLE_APPLICATION_CREDENTIALS=/workspace/gcp-key.json'

  # Verify - Post-deployment checks
  - name: 'python:3.11'
    id: verify
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        pip install fluid-forge==$_FLUID_VERSION
        if [ "$_ENVIRONMENT" = "prod" ]; then
          for contract in *.fluid.yaml; do
            if [ -f "$contract" ]; then
              echo "Verifying $contract in production..."
              fluid verify "$contract" --env prod --provider gcp
            fi
          done
        else
          echo "Skipping verification for non-prod environment"
        fi
    waitFor: ['deploy']
    env:
      - 'GOOGLE_APPLICATION_CREDENTIALS=/workspace/gcp-key.json'

# Artifacts to store
artifacts:
  objects:
    location: 'gs://${PROJECT_ID}_cloudbuild/artifacts'
    paths:
      - '**/*.plan.json'
      - '**/*.fluid.yaml'

# Timeout for entire build
timeout: 1800s

# Service account for deployment
serviceAccount: 'projects/${PROJECT_ID}/serviceAccounts/fluid-deployer@${PROJECT_ID}.iam.gserviceaccount.com'
"""

    cloudbuild_path = project_dir / "cloudbuild.yaml"
    cloudbuild_path.write_text(cloudbuild_content.strip())

    if RICH_AVAILABLE:
        console.print("✅ Generated [bold]cloudbuild.yaml[/bold]")
        console.print("   - Multi-step pipeline (setup/validate/plan/test/deploy/verify)")
        console.print("   - GCP-native integration (BigQuery, GCS)")
        console.print("   - Service account authentication")
        console.print("   - Artifact storage to GCS")
    else:
        success("Generated Google Cloud Build configuration")

    info(logger, "cloudbuild_generated", path=str(cloudbuild_path))


# ============================================================================
# AGENT ZERO - PROJECT SCANNER
# ============================================================================


class ProjectDetector:
    """Base class for project detectors"""

    def can_detect(self, path: Path) -> bool:
        """Returns True if this detector can handle the project"""
        raise NotImplementedError

    def scan(self, logger: logging.Logger) -> Dict[str, Any]:
        """Scan the project and return results"""
        raise NotImplementedError


class DbtDetector(ProjectDetector):
    """Detect and parse dbt projects"""

    def can_detect(self, path: Path) -> bool:
        return (path / "dbt_project.yml").exists()

    def scan(self, logger: logging.Logger) -> Dict[str, Any]:
        """Scan dbt project"""

        import yaml

        results = {"project_type": "dbt", "models": [], "sensitive_columns": [], "metadata": {}}

        # Parse dbt_project.yml
        dbt_project_path = Path("dbt_project.yml")
        with open(dbt_project_path) as f:
            project = yaml.safe_load(f)

        results["metadata"]["project_name"] = project.get("name", "unknown")
        results["metadata"]["version"] = project.get("version", "1.0.0")

        if RICH_AVAILABLE:
            console.print(
                f"\n📦 Found dbt project: [bold]{results['metadata']['project_name']}[/bold]"
            )

        # Find models
        models_dir = Path("models")
        if models_dir.exists():
            sql_files = list(models_dir.rglob("*.sql"))

            if RICH_AVAILABLE:
                console.print(f"🔍 Scanning {len(sql_files)} SQL models...")

            for sql_file in sql_files:
                model = self._parse_model(sql_file, logger)
                if model:
                    results["models"].append(model)

        # Parse profiles.yml for target (if exists)
        profiles_path = Path.home() / ".dbt" / "profiles.yml"
        if profiles_path.exists():
            with open(profiles_path) as f:
                profiles = yaml.safe_load(f)
                if results["metadata"]["project_name"] in profiles:
                    profile = profiles[results["metadata"]["project_name"]]
                    target_name = profile.get("target", "dev")
                    outputs = profile.get("outputs", {})
                    if target_name in outputs:
                        target = outputs[target_name]
                        results["metadata"]["target_platform"] = target.get("type")
                        results["metadata"]["target_database"] = target.get("database")
                        results["metadata"]["target_schema"] = target.get("schema")

        # Detect PII
        results["sensitive_columns"] = self._detect_pii(results["models"])

        return results

    def _parse_model(self, sql_file: Path, logger: logging.Logger) -> Optional[Dict[str, Any]]:
        """Parse a dbt SQL model file"""

        try:
            content = sql_file.read_text()

            # Extract model name from file
            model_name = sql_file.stem

            # Try to extract column references from SQL
            # This is simplified - real implementation would use SQL parser
            columns = []

            # Look for SELECT statements
            select_pattern = r"SELECT\s+(.*?)\s+FROM"
            matches = re.findall(select_pattern, content, re.IGNORECASE | re.DOTALL)

            if matches:
                col_text = matches[0]
                # Split by comma and clean
                col_names = [
                    c.strip().split()[-1].split(".")[-1]
                    for c in col_text.split(",")
                    if c.strip() and c.strip() != "*"
                ]
                columns = [{"name": c, "type": "unknown"} for c in col_names if c]

            # Check for config in file
            config_pattern = r"{{[\s]*config\((.*?)\)[\s]*}}"
            config_match = re.search(config_pattern, content, re.DOTALL)

            materialization = "view"  # default
            if config_match:
                if "materialized='table'" in config_match.group(1):
                    materialization = "table"
                elif "materialized='incremental'" in config_match.group(1):
                    materialization = "incremental"

            return {
                "name": model_name,
                "path": str(sql_file),
                "materialization": materialization,
                "columns": columns,
                "raw_sql": content,
            }

        except Exception as e:
            if logger:
                info(logger, "model_parse_failed", file=str(sql_file), error=str(e))
            return None

    def _detect_pii(self, models: List[Dict]) -> List[Dict[str, Any]]:
        """Detect PII with confidence scores"""

        pii_keywords = {
            "ssn": {"patterns": ["ssn", "social_security", "social"], "confidence": 0.90},
            "email": {"patterns": ["email", "e_mail", "mail"], "confidence": 0.85},
            "phone": {"patterns": ["phone", "telephone", "mobile", "cell"], "confidence": 0.80},
            "credit_card": {
                "patterns": ["credit_card", "cc_number", "card_num"],
                "confidence": 0.95,
            },
            "address": {"patterns": ["address", "street", "zip", "postal"], "confidence": 0.70},
            "name": {
                "patterns": ["first_name", "last_name", "full_name", "customer_name"],
                "confidence": 0.60,
            },
            "dob": {"patterns": ["birth_date", "dob", "date_of_birth"], "confidence": 0.85},
        }

        findings = []

        for model in models:
            for col in model.get("columns", []):
                col_lower = col["name"].lower()

                for pii_type, pii_data in pii_keywords.items():
                    for pattern in pii_data["patterns"]:
                        if pattern in col_lower:
                            findings.append(
                                {
                                    "model": model["name"],
                                    "column": col["name"],
                                    "type": pii_type.upper(),
                                    "confidence": pii_data["confidence"],
                                    "method": "column_name_heuristic",
                                }
                            )
                            break  # Only report once per column

        return findings


class TerraformDetector(ProjectDetector):
    """Detect and parse Terraform configurations"""

    def can_detect(self, path: Path) -> bool:
        tf_files = list(path.glob("*.tf"))
        return len(tf_files) > 0

    def scan(self, logger: logging.Logger) -> Dict[str, Any]:
        """Scan Terraform files"""

        results = {
            "project_type": "terraform",
            "resources": [],
            "sensitive_columns": [],
            "metadata": {},
        }

        tf_files = list(Path.cwd().glob("*.tf"))

        if RICH_AVAILABLE:
            console.print(f"\n🔍 Found {len(tf_files)} Terraform files")

        # Parse Terraform files (simplified)
        for tf_file in tf_files:
            content = tf_file.read_text()

            # Look for data sources and resources
            # This is simplified - real implementation would use HCL parser
            if 'resource "google_bigquery_dataset"' in content:
                results["metadata"]["target_platform"] = "gcp"
            elif 'resource "snowflake_database"' in content:
                results["metadata"]["target_platform"] = "snowflake"

        results["metadata"]["files_count"] = len(tf_files)

        return results


class SqlFileDetector(ProjectDetector):
    """Detect standalone SQL files"""

    def can_detect(self, path: Path) -> bool:
        sql_files = list(path.glob("*.sql"))
        return len(sql_files) > 0 and not (path / "dbt_project.yml").exists()

    def scan(self, logger: logging.Logger) -> Dict[str, Any]:
        """Scan SQL files"""

        results = {"project_type": "sql", "files": [], "sensitive_columns": [], "metadata": {}}

        sql_files = list(Path.cwd().glob("*.sql"))

        if RICH_AVAILABLE:
            console.print(f"\n📄 Found {len(sql_files)} SQL files")

        for sql_file in sql_files:
            results["files"].append({"name": sql_file.name, "path": str(sql_file)})

        results["metadata"]["files_count"] = len(sql_files)

        return results


def detect_project_type(path: Path) -> Optional[ProjectDetector]:
    """Auto-detect project type"""

    detectors = [DbtDetector(), TerraformDetector(), SqlFileDetector()]

    for detector in detectors:
        if detector.can_detect(path):
            return detector

    return None


def show_scan_results(results: Dict[str, Any]):
    """Display scan results with rich formatting"""

    if not RICH_AVAILABLE:
        cprint(f"\nProject Type: {results['project_type']}")
        return

    console.print("\n" + "━" * 70)
    console.print("📊 [bold]Scan Results[/bold]")
    console.print("━" * 70 + "\n")

    # Project info
    project_type = results["project_type"]
    console.print(f"Project Type: [bold cyan]{project_type.upper()}[/bold cyan]")

    if project_type == "dbt":
        console.print(
            f"Project Name: [bold]{results['metadata'].get('project_name', 'N/A')}[/bold]"
        )
        console.print(f"Models Found: [bold]{len(results.get('models', []))}[/bold]")

        # Show target platform
        target_platform = results["metadata"].get("target_platform")
        if target_platform:
            console.print(f"Target Platform: [bold]{target_platform.upper()}[/bold]")

            # Infer jurisdiction
            target_db = results["metadata"].get("target_database", "")
            if "eu" in target_db.lower():
                console.print("  [yellow]→ Detected EU region (GDPR considerations)[/yellow]")

    elif project_type == "terraform":
        console.print(f"Files Found: [bold]{results['metadata'].get('files_count', 0)}[/bold]")
        target = results["metadata"].get("target_platform")
        if target:
            console.print(f"Target Platform: [bold]{target.upper()}[/bold]")

    elif project_type == "sql":
        console.print(f"SQL Files: [bold]{results['metadata'].get('files_count', 0)}[/bold]")

    # Show PII detection results
    sensitive = results.get("sensitive_columns", [])
    if sensitive:
        console.print(
            f"\n🔒 [yellow bold]Sensitive Data Detected:[/yellow bold] {len(sensitive)} columns\n"
        )

        if RICH_AVAILABLE:
            table = Table(show_header=True, header_style="bold")
            table.add_column("Model", style="cyan")
            table.add_column("Column", style="yellow")
            table.add_column("Type", style="red")
            table.add_column("Confidence", justify="right")

            for finding in sensitive[:10]:  # Show top 10
                confidence = finding["confidence"]
                color = "red" if confidence > 0.9 else "yellow" if confidence > 0.7 else "white"

                table.add_row(
                    finding["model"],
                    finding["column"],
                    finding["type"],
                    f"[{color}]{confidence:.0%}[/{color}]",
                )

            console.print(table)

            if len(sensitive) > 10:
                console.print(f"\n  ... and {len(sensitive) - 10} more")
    else:
        console.print("\n✅ [green]No obvious PII detected[/green]")

    console.print()


def generate_contracts_from_scan(
    results: Dict[str, Any], provider: str, logger: logging.Logger
) -> List[Dict]:
    """Generate FLUID contracts from scan results"""

    contracts = []
    project_type = results["project_type"]

    if RICH_AVAILABLE:
        console.print("\n⚙️  [bold]Generating FLUID contracts...[/bold]\n")

    if project_type == "dbt":
        # Generate contract for dbt project
        contract = {
            "version": "0.7.1",
            "kind": "fluid",
            "name": results["metadata"].get("project_name", "imported-project"),
            "description": f"Imported from dbt project on {Path.cwd().name}",
            "exposes": [],
            "produces": [],
        }

        # Add models as produces
        for model in results.get("models", [])[:5]:  # Limit to first 5 for demo
            produce = {
                "name": model["name"],
                "description": f"Imported dbt model: {model['name']}",
                "from": {
                    "type": "sql",
                    "sql": "-- Imported from dbt\n" + model.get("raw_sql", "")[:200] + "...",
                },
            }

            if model.get("columns"):
                produce["schema"] = [
                    {"name": col["name"], "type": col.get("type", "string")}
                    for col in model["columns"][:10]  # Limit columns
                ]

            contract["produces"].append(produce)

        # Add binding
        target_platform = results["metadata"].get("target_platform", "local")
        contract["binding"] = {
            "provider": (
                target_platform if target_platform in ["gcp", "snowflake", "aws"] else "local"
            )
        }

        if target_platform == "gcp":
            contract["binding"]["location"] = {
                "project": results["metadata"].get("target_database", "my-project"),
                "dataset": results["metadata"].get("target_schema", "analytics"),
            }

        contracts.append(contract)

        if RICH_AVAILABLE:
            console.print(f"✅ Generated contract with {len(contract['produces'])} models")

    elif project_type == "terraform":
        # Generate basic contract for Terraform
        contract = {
            "version": "0.7.1",
            "kind": "fluid",
            "name": "terraform-import",
            "description": "Imported from Terraform configuration",
            "exposes": [],
            "produces": [],
            "binding": {"provider": results["metadata"].get("target_platform", "local")},
        }
        contracts.append(contract)

    elif project_type == "sql":
        # Generate contract for SQL files
        contract = {
            "version": "0.7.1",
            "kind": "fluid",
            "name": "sql-import",
            "description": "Imported from SQL files",
            "exposes": [],
            "produces": [],
            "binding": {"provider": provider},
        }
        contracts.append(contract)

    return contracts


def apply_governance_policies(
    contracts: List[Dict], results: Dict[str, Any], logger: logging.Logger
) -> List[Dict]:
    """Apply governance policies based on PII detection"""

    if not RICH_AVAILABLE:
        return contracts

    sensitive = results.get("sensitive_columns", [])
    if not sensitive:
        return contracts

    console.print("\n" + "━" * 70)
    console.print("🛡️  [bold]Governance Configuration[/bold]")
    console.print("━" * 70 + "\n")

    console.print(f"Found {len(sensitive)} potentially sensitive columns.\n")

    # Ask if user wants to apply governance
    if not Confirm.ask("Apply data governance policies?", default=True):
        return contracts

    # Group by model
    by_model = {}
    for finding in sensitive:
        model = finding["model"]
        if model not in by_model:
            by_model[model] = []
        by_model[model].append(finding)

    # Apply masking to contracts
    for contract in contracts:
        _policies = []  # noqa: F841

        for produce in contract.get("produces", []):
            model_name = produce["name"]

            if model_name in by_model:
                console.print(f"\n📋 [bold]{model_name}[/bold]:")

                masking_rules = []
                for finding in by_model[model_name][:3]:  # Limit to 3
                    console.print(
                        f"  • {finding['column']} ([{finding['type']}], {finding['confidence']:.0%} confidence)"
                    )

                    masking_rules.append(
                        {
                            "column": finding["column"],
                            "method": "SHA256" if finding["confidence"] > 0.8 else "MASK",
                            "reason": f"Detected {finding['type']} with {finding['confidence']:.0%} confidence",
                        }
                    )

                if masking_rules:
                    if "policy" not in produce:
                        produce["policy"] = {}
                    produce["policy"]["masking"] = masking_rules

        # Add jurisdiction if detected
        target_db = results.get("metadata", {}).get("target_database", "")
        if "eu" in target_db.lower():
            if Confirm.ask("\nApply GDPR sovereignty controls?", default=True):
                contract["sovereignty"] = {
                    "jurisdiction": "EU",
                    "dataResidency": {"allowedRegions": ["europe-west1", "europe-west4"]},
                    "jurisdictionRequirements": ["GDPR"],
                }

    console.print("\n✅ Governance policies applied\n")

    return contracts


def show_migration_summary(contracts: List[Dict], results: Dict[str, Any], logger: logging.Logger):
    """Show migration summary"""

    if not RICH_AVAILABLE:
        cprint(f"\n✅ Generated {len(contracts)} FLUID contract(s)")
        return

    console.print("━" * 70)
    console.print("✅ [green bold]Migration Complete![/green bold]\n")

    console.print(f"Generated: [bold]{len(contracts)} FLUID contract(s)[/bold]")

    for contract in contracts:
        console.print(f"\n📄 [cyan]{contract['name']}.fluid.yaml[/cyan]")
        console.print(f"   Version: {contract['version']}")
        console.print(f"   Provider: {contract['binding']['provider']}")
        console.print(f"   Models: {len(contract.get('produces', []))}")

        if contract.get("sovereignty"):
            console.print("   Governance: [yellow]GDPR controls enabled[/yellow]")

    console.print("\n[bold]Next Steps:[/bold]\n")
    console.print("  1. Review generated contracts:")
    console.print("     [cyan]$ ls *.fluid.yaml[/cyan]")
    console.print("\n  2. Validate contracts:")
    console.print("     [cyan]$ fluid validate *.fluid.yaml[/cyan]")
    console.print("\n  3. Test locally:")
    console.print("     [cyan]$ fluid plan <contract>.fluid.yaml --provider local[/cyan]")
    console.print("\n  4. Deploy:")
    console.print("     [cyan]$ fluid apply <contract>.fluid.yaml[/cyan]")

    console.print("\n" + "━" * 70)
    console.print()
