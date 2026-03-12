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
CLI command for generating dynamic DataOps pipeline templates

This command allows teams to generate CI/CD pipeline configurations
tailored to their preferred provider and requirements.
"""

import argparse
import logging
from pathlib import Path

from fluid_build.cli.console import cprint, success, warning
from fluid_build.cli.console import error as console_error

from ..forge.core.pipeline_templates import (
    PipelineComplexity,
    PipelineConfig,
    PipelineProvider,
    PipelineTemplateGenerator,
)

COMMAND = "generate-pipeline"


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register pipeline generator command"""
    parser = subparsers.add_parser(
        COMMAND,
        help="🚀 Generate CI/CD pipeline templates for FLUID projects",
        description="Create dynamic DataOps pipeline configurations for your preferred CI/CD provider",
    )

    parser.add_argument(
        "--provider",
        choices=[
            "github_actions",
            "gitlab_ci",
            "azure_devops",
            "jenkins",
            "bitbucket",
            "circleci",
            "tekton",
        ],
        help="CI/CD provider to generate pipeline for",
    )
    parser.add_argument(
        "--complexity",
        choices=["basic", "standard", "advanced", "enterprise"],
        default="standard",
        help="Pipeline complexity level",
    )
    parser.add_argument(
        "--environments",
        nargs="*",
        default=["dev", "staging", "prod"],
        help="Deployment environments",
    )
    parser.add_argument(
        "--enable-approvals", action="store_true", help="Enable manual approval gates"
    )
    parser.add_argument(
        "--enable-security-scan",
        action="store_true",
        default=True,
        help="Enable security scanning in pipeline",
    )
    parser.add_argument(
        "--enable-marketplace", action="store_true", help="Enable marketplace publishing"
    )
    parser.add_argument("--output-dir", default=".", help="Directory to output pipeline files")
    parser.add_argument(
        "--preview", action="store_true", help="Preview pipeline content without writing files"
    )
    parser.add_argument(
        "--interactive", action="store_true", help="Interactive mode for configuration"
    )


def run(args: argparse.Namespace, logger: logging.Logger) -> int:
    """
    Generate dynamic DataOps pipeline templates for FLUID projects.

    This command creates comprehensive CI/CD pipeline configurations that include
    all FLUID workflow steps: validate, plan, apply, test, visualize, and marketplace publishing.
    """
    try:
        cprint("🚀 FLUID Dynamic DataOps Pipeline Generator")
        cprint("=" * 50)

        # Use interactive mode if provider not specified
        if args.interactive or not args.provider:
            (
                provider,
                complexity,
                environments,
                enable_approvals,
                enable_security_scan,
                enable_marketplace,
            ) = _interactive_config()
        else:
            provider = args.provider
            complexity = args.complexity
            environments = args.environments
            enable_approvals = args.enable_approvals
            enable_security_scan = args.enable_security_scan
            enable_marketplace = args.enable_marketplace

        # Create configuration
        config = PipelineConfig(
            provider=PipelineProvider(provider),
            complexity=PipelineComplexity(complexity),
            environments=environments,
            enable_approvals=enable_approvals,
            enable_security_scan=enable_security_scan,
            enable_marketplace_publishing=enable_marketplace,
        )

        # Generate pipeline
        generator = PipelineTemplateGenerator()
        files = generator.generate_pipeline(config)

        # Handle preview mode
        if args.preview:
            cprint(f"\n🔍 Preview mode - would generate {len(files)} files:")
            for filename, content in files.items():
                cprint(f"\n📄 {filename}")
                cprint("-" * 40)
                cprint(content[:500] + "..." if len(content) > 500 else content)
            return 0

        # Write files to output directory
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        for filename, content in files.items():
            file_path = output_dir / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with file_path.open("w") as f:
                f.write(content)

            success(f"Created: {file_path}")

        cprint(f"\n🎉 Successfully generated {len(files)} pipeline files!")
        _show_next_steps(provider, output_dir)

        return 0

    except Exception as e:
        logger.error(f"Pipeline generation failed: {e}")
        console_error(f"Error: {e}")
        return 1


def _interactive_config():
    """Interactive configuration mode"""
    cprint("\n🎯 Interactive Pipeline Configuration")

    # Provider selection
    providers = [p.value for p in PipelineProvider]
    cprint("\nAvailable CI/CD providers:")
    for i, provider in enumerate(providers, 1):
        cprint(f"  {i}. {provider.replace('_', ' ').title()}")

    while True:
        try:
            choice = int(input("\nSelect provider (1-7): "))
            if 1 <= choice <= len(providers):
                provider = providers[choice - 1]
                break
            else:
                console_error("Invalid choice. Please select 1-7.")
        except ValueError:
            console_error("Please enter a valid number.")

    # Complexity selection
    complexities = [c.value for c in PipelineComplexity]
    cprint("\nPipeline complexity levels:")
    cprint("  1. Basic - Simple validate -> apply workflow")
    cprint("  2. Standard - Full workflow with testing and multi-environment")
    cprint("  3. Advanced - Multi-environment with approvals and security")
    cprint("  4. Enterprise - Full governance and compliance")

    while True:
        try:
            choice = int(input("\nSelect complexity (1-4, default=2): ") or "2")
            if 1 <= choice <= len(complexities):
                complexity = complexities[choice - 1]
                break
            else:
                console_error("Invalid choice. Please select 1-4.")
        except ValueError:
            console_error("Please enter a valid number.")

    # Environment configuration
    environments = ["dev", "staging", "prod"]
    if complexity in ["standard", "advanced", "enterprise"]:
        env_input = input(
            f"\nUse default environments ({', '.join(environments)})? [Y/n]: "
        ).lower()
        if env_input in ["n", "no"]:
            environments = []
            cprint("\nEnter environments (press Enter when done):")
            while True:
                env = input("Environment name: ").strip()
                if not env:
                    break
                environments.append(env)

            if not environments:
                environments = ["dev"]
                warning("No environments specified, using 'dev' as default")
    else:
        environments = ["dev"]

    # Advanced options
    enable_approvals = False
    enable_security_scan = True
    enable_marketplace = False

    if complexity in ["advanced", "enterprise"]:
        enable_approvals = input(
            "\nEnable manual approval gates for production? [Y/n]: "
        ).lower() not in ["n", "no"]
        enable_security_scan = input("Enable security scanning? [Y/n]: ").lower() not in ["n", "no"]
        enable_marketplace = input("Enable marketplace publishing? [y/N]: ").lower() in ["y", "yes"]

    return (
        provider,
        complexity,
        environments,
        enable_approvals,
        enable_security_scan,
        enable_marketplace,
    )


def _show_next_steps(provider: str, output_dir: Path):
    """Show next steps for setup"""
    cprint(f"\n📋 Next Steps for {provider.replace('_', ' ').title()}:")

    if provider == "github_actions":
        cprint("1. Commit the .github/workflows/ files to your repository")
        cprint("2. Configure repository secrets for deployment credentials")
        cprint("3. Set up environment protection rules in repository settings")
        cprint("4. Configure any required GitHub Apps or integrations")
    elif provider == "gitlab_ci":
        cprint("1. Commit the .gitlab-ci.yml file to your repository")
        cprint("2. Configure CI/CD variables for deployment credentials")
        cprint("3. Set up environments and deployment rules in GitLab")
        cprint("4. Configure runners with appropriate tags if using self-hosted runners")
    elif provider == "azure_devops":
        cprint("1. Commit the azure-pipelines.yml file to your repository")
        cprint("2. Configure service connections for deployment targets")
        cprint("3. Set up environments with approval and check policies")
        cprint("4. Configure variable groups for environment-specific settings")
    elif provider == "jenkins":
        cprint("1. Create a new pipeline job in Jenkins")
        cprint("2. Configure the Jenkinsfile in your repository")
        cprint("3. Set up Jenkins credentials for deployment targets")
        cprint("4. Configure any required Jenkins plugins")
    else:
        cprint("1. Commit the pipeline configuration to your repository")
        cprint("2. Configure deployment credentials and secrets")
        cprint("3. Set up environment-specific configurations")
        cprint("4. Test the pipeline with a sample commit")

    cprint(f"\n📁 Pipeline files created in: {output_dir.absolute()}")
    cprint("\n🔧 FLUID Commands included in pipeline:")
    cprint("  • fluid validate --strict")
    cprint("  • fluid plan --output plan.json")
    cprint("  • fluid apply --plan plan.json")
    cprint("  • fluid test --coverage")
    cprint("  • fluid viz-plan --output pipeline-viz.html")
    cprint("  • fluid export-opds --output opds-catalog.json")
    cprint("  • fluid marketplace publish --catalog opds-catalog.json")
