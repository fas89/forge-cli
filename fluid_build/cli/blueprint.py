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
FLUID Build - Blueprint Command

Dedicated command for managing and deploying FLUID blueprints
"""

import argparse
import logging
from pathlib import Path

from ..blueprints import registry as blueprint_registry
from ..blueprints.base import BlueprintCategory, BlueprintComplexity

COMMAND = "blueprint"


def register(subparsers: argparse._SubParsersAction):
    """Register the blueprint command with the CLI parser"""
    p = subparsers.add_parser(
        COMMAND, help="🔧 Manage and deploy FLUID blueprints - complete data product templates"
    )

    blueprint_subparsers = p.add_subparsers(dest="blueprint_action", help="Blueprint actions")

    # List blueprints
    list_parser = blueprint_subparsers.add_parser("list", help="List available blueprints")
    list_parser.add_argument(
        "--category", choices=[c.value for c in BlueprintCategory], help="Filter by category"
    )
    list_parser.add_argument(
        "--complexity", choices=[c.value for c in BlueprintComplexity], help="Filter by complexity"
    )
    list_parser.add_argument("--provider", help="Filter by provider")
    list_parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show detailed information"
    )

    # Show blueprint details
    describe_parser = blueprint_subparsers.add_parser("describe", help="Show blueprint details")
    describe_parser.add_argument("name", help="Blueprint name to describe")

    # Create project from blueprint
    create_parser = blueprint_subparsers.add_parser("create", help="Create project from blueprint")
    create_parser.add_argument("name", help="Blueprint name to use")
    create_parser.add_argument("--target-dir", "-d", help="Target directory for project")
    create_parser.add_argument("--provider", "-p", help="Provider to use")
    create_parser.add_argument(
        "--quickstart", "-q", action="store_true", help="Skip confirmation prompts"
    )
    create_parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be created without doing it"
    )

    # Search blueprints
    search_parser = blueprint_subparsers.add_parser("search", help="Search blueprints")
    search_parser.add_argument("query", help="Search query")

    # Validate blueprints
    validate_parser = blueprint_subparsers.add_parser("validate", help="Validate blueprints")
    validate_parser.add_argument(
        "name", nargs="?", help="Blueprint name to validate (all if not specified)"
    )

    p.set_defaults(func=run)


def run(args, logger: logging.Logger) -> int:
    """Run the blueprint command"""
    try:
        if not args.blueprint_action:
            logger.error("Blueprint action required. Use --help for available actions.")
            return 1

        # Refresh blueprint registry
        blueprint_registry.refresh()

        if args.blueprint_action == "list":
            return list_blueprints(args, logger)
        elif args.blueprint_action == "describe":
            return describe_blueprint(args, logger)
        elif args.blueprint_action == "create":
            return create_project(args, logger)
        elif args.blueprint_action == "search":
            return search_blueprints(args, logger)
        elif args.blueprint_action == "validate":
            return validate_blueprints(args, logger)
        else:
            logger.error(f"Unknown blueprint action: {args.blueprint_action}")
            return 1

    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"Blueprint command failed: {e}", exc_info=True)
        return 1


def list_blueprints(args, logger: logging.Logger) -> int:
    """List available blueprints"""
    # Apply filters
    category = BlueprintCategory(args.category) if args.category else None
    complexity = BlueprintComplexity(args.complexity) if args.complexity else None

    blueprints = blueprint_registry.list_blueprints(
        category=category, complexity=complexity, provider=args.provider
    )

    if not blueprints:
        logger.info("No blueprints found matching criteria")
        return 0

    logger.info(f"📋 Available Blueprints ({len(blueprints)} found):")
    logger.info("")

    for blueprint in blueprints:
        metadata = blueprint.metadata

        # Basic info
        logger.info(f"🔹 {metadata.name}")
        logger.info(f"   Title: {metadata.title}")

        if args.verbose:
            logger.info(f"   Description: {metadata.description}")
            logger.info(f"   Category: {metadata.category.value}")
            logger.info(f"   Complexity: {metadata.complexity.value}")
            logger.info(f"   Providers: {', '.join(metadata.providers)}")
            logger.info(f"   Setup Time: {metadata.setup_time}")
            if metadata.tags:
                logger.info(f"   Tags: {', '.join(metadata.tags)}")
        else:
            logger.info(f"   {metadata.description[:80]}...")
            logger.info(
                f"   Category: {metadata.category.value} | Complexity: {metadata.complexity.value}"
            )

        logger.info("")

    if not args.verbose:
        logger.info(
            "💡 Use --verbose for more details or 'fluid blueprint describe <name>' for full info"
        )

    return 0


def describe_blueprint(args, logger: logging.Logger) -> int:
    """Show detailed blueprint information"""
    blueprint = blueprint_registry.get_blueprint(args.name)
    if not blueprint:
        logger.error(f"Blueprint '{args.name}' not found")
        return 1

    metadata = blueprint.metadata

    logger.info(f"📋 Blueprint: {metadata.name}")
    logger.info("=" * (len(metadata.name) + 12))
    logger.info(f"Title: {metadata.title}")
    logger.info(f"Description: {metadata.description}")
    logger.info("")

    # Classification
    logger.info("🏷️  Classification:")
    logger.info(f"   Category: {metadata.category.value}")
    logger.info(f"   Complexity: {metadata.complexity.value}")
    logger.info(f"   Setup Time: {metadata.setup_time}")
    logger.info("")

    # Technical details
    logger.info("🔧 Technical Details:")
    logger.info(f"   Providers: {', '.join(metadata.providers)}")
    logger.info(f"   Runtimes: {', '.join(metadata.runtimes)}")
    logger.info("")

    # Features
    logger.info("✨ Features:")
    if metadata.has_sample_data:
        logger.info("   ✅ Sample Data")
    if metadata.has_tests:
        logger.info("   ✅ Data Quality Tests")
    if metadata.has_docs:
        logger.info("   ✅ Documentation")
    if metadata.has_cicd:
        logger.info("   ✅ CI/CD Configuration")
    logger.info("")

    # Use cases
    if metadata.use_cases:
        logger.info("🎯 Use Cases:")
        for use_case in metadata.use_cases:
            logger.info(f"   • {use_case}")
        logger.info("")

    # Best practices
    if metadata.best_practices:
        logger.info("⭐ Best Practices:")
        for practice in metadata.best_practices:
            logger.info(f"   • {practice}")
        logger.info("")

    # Tags
    if metadata.tags:
        logger.info(f"🏷️  Tags: {', '.join(metadata.tags)}")
        logger.info("")

    # Dependencies
    if metadata.dependencies:
        logger.info("📦 Dependencies:")
        for dep in metadata.dependencies:
            req_str = " (required)" if dep.required else " (optional)"
            version_str = f" v{dep.version}" if dep.version else ""
            logger.info(f"   • {dep.name}{version_str}{req_str}")
        logger.info("")

    # Creation info
    logger.info(f"👤 Author: {metadata.author}")
    logger.info(f"📅 Created: {metadata.created_at}")
    if metadata.updated_at:
        logger.info(f"🔄 Updated: {metadata.updated_at}")

    return 0


def create_project(args, logger: logging.Logger) -> int:
    """Create a project from a blueprint"""
    blueprint = blueprint_registry.get_blueprint(args.name)
    if not blueprint:
        logger.error(f"Blueprint '{args.name}' not found")
        return 1

    # Determine target directory
    target_dir = Path(args.target_dir) if args.target_dir else Path.cwd() / args.name

    # Check if directory exists
    if target_dir.exists() and any(target_dir.iterdir()):
        if not args.quickstart:
            response = input(f"Directory {target_dir} exists and is not empty. Continue? (y/N): ")
            if response.lower() != "y":
                logger.info("Operation cancelled")
                return 1
        else:
            logger.error(f"Target directory {target_dir} exists and is not empty")
            return 1

    # Validate blueprint
    errors = blueprint.validate()
    if errors:
        logger.error("Blueprint validation failed:")
        for error in errors:
            logger.error(f"  ❌ {error}")
        return 1

    # Show blueprint info
    metadata = blueprint.metadata
    logger.info(f"🚀 Creating project from blueprint: {metadata.title}")
    logger.info(f"   Category: {metadata.category.value}")
    logger.info(f"   Complexity: {metadata.complexity.value}")
    logger.info(f"   Estimated setup: {metadata.setup_time}")

    if not args.quickstart:
        logger.info("\nThis will create a complete data product with:")
        if metadata.has_sample_data:
            logger.info("   ✅ Sample data for testing")
        if metadata.has_tests:
            logger.info("   ✅ Data quality tests")
        if metadata.has_docs:
            logger.info("   ✅ Complete documentation")

        response = input(f"\nCreate project in {target_dir}? (Y/n): ")
        if response.lower() == "n":
            logger.info("Operation cancelled")
            return 1

    # Dry run mode
    if args.dry_run:
        logger.info(f"DRY RUN: Would create project in {target_dir}")
        logger.info("Files that would be created:")
        for file_path in blueprint.path.rglob("*"):
            if file_path.is_file() and file_path.name != "blueprint.yaml":
                rel_path = file_path.relative_to(blueprint.path)
                logger.info(f"  📄 {rel_path}")
        return 0

    # Generate project
    logger.info(f"📁 Creating project in {target_dir}...")
    blueprint.generate_project(target_dir)

    logger.info("✅ Project created successfully!")
    logger.info(f"📂 Location: {target_dir}")

    # Next steps
    logger.info("\n📖 Next Steps:")
    logger.info(f"1. cd {target_dir}")
    logger.info("2. Review documentation: cat docs/README.md")
    logger.info("3. Set up your data connections")
    logger.info("4. Run: fluid validate")

    if "dbt" in metadata.runtimes:
        logger.info("5. Run: dbt run")
        logger.info("6. Run: dbt test")

    return 0


def search_blueprints(args, logger: logging.Logger) -> int:
    """Search blueprints by query"""
    blueprints = blueprint_registry.search_blueprints(args.query)

    if not blueprints:
        logger.info(f"No blueprints found matching '{args.query}'")
        return 0

    logger.info(f"🔍 Search results for '{args.query}' ({len(blueprints)} found):")
    logger.info("")

    for blueprint in blueprints:
        metadata = blueprint.metadata
        logger.info(f"🔹 {metadata.name}")
        logger.info(f"   {metadata.title}")
        logger.info(f"   {metadata.description[:100]}...")
        logger.info(
            f"   Tags: {', '.join(metadata.tags[:3])}{'...' if len(metadata.tags) > 3 else ''}"
        )
        logger.info("")

    return 0


def validate_blueprints(args, logger: logging.Logger) -> int:
    """Validate blueprint structure and content"""
    if args.name:
        # Validate specific blueprint
        blueprint = blueprint_registry.get_blueprint(args.name)
        if not blueprint:
            logger.error(f"Blueprint '{args.name}' not found")
            return 1

        logger.info(f"🔍 Validating blueprint: {args.name}")
        errors = blueprint.validate()

        if not errors:
            logger.info("✅ Blueprint validation passed")
            return 0
        else:
            logger.error("❌ Blueprint validation failed:")
            for error in errors:
                logger.error(f"  • {error}")
            return 1
    else:
        # Validate all blueprints
        logger.info("🔍 Validating all blueprints...")
        validation_results = blueprint_registry.validate_all()

        if not validation_results:
            logger.info("✅ All blueprints are valid")
            return 0
        else:
            logger.error("❌ Validation errors found:")
            for blueprint_name, errors in validation_results.items():
                logger.error(f"\n{blueprint_name}:")
                for error in errors:
                    logger.error(f"  • {error}")
            return 1
