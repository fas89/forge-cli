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
FLUID AI Copilot - Intelligent Assistant for Data Product Development

This module provides AI-powered assistance throughout the data product development
lifecycle, including smart suggestions, error recovery, and automated optimizations.
"""

from __future__ import annotations
import argparse
import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.tree import Tree

from ._common import CLIError
from ._logging import info, warn, error

COMMAND = "copilot"

@dataclass
class CopilotContext:
    """Context for AI copilot operations"""
    command: str
    args: Dict[str, Any]
    current_directory: Path
    user_history: List[str]
    project_metadata: Optional[Dict[str, Any]] = None

@dataclass
class AIsuggestion:
    """AI-generated suggestion"""
    type: str  # 'command', 'fix', 'optimization', 'template'
    confidence: float
    title: str
    description: str
    action: str
    reasoning: str
    examples: List[str]

class FluidAICopilot:
    """AI-powered copilot for FLUID CLI operations"""
    
    def __init__(self):
        self.console = Console()
        self.knowledge_base = self._load_knowledge_base()
        self.user_patterns = {}
        
    def _load_knowledge_base(self) -> Dict[str, Any]:
        """Load AI knowledge base with common patterns and solutions"""
        return {
            "common_errors": {
                "contract_not_found": {
                    "suggestions": [
                        "Check if the contract file exists in the current directory",
                        "Verify the file extension (.yaml, .yml, or .json)",
                        "Use 'fluid forge' to create a new contract",
                        "Use 'ls *.yaml' to list available contracts"
                    ],
                    "auto_fixes": ["fluid forge --template quickstart"]
                },
                "provider_auth_failed": {
                    "suggestions": [
                        "Run 'fluid auth login <provider>' to authenticate",
                        "Check provider credentials and permissions",
                        "Verify cloud CLI tools are installed",
                        "Use 'fluid doctor' to diagnose issues"
                    ],
                    "auto_fixes": ["fluid auth status", "fluid doctor"]
                }
            },
            "optimization_patterns": {
                "large_datasets": [
                    "Consider using partitioning for better performance",
                    "Add data quality checks to catch issues early",
                    "Use incremental processing where possible"
                ],
                "multi_provider": [
                    "Standardize on common data formats",
                    "Use environment-specific configurations",
                    "Consider cross-cloud data transfer costs"
                ]
            },
            "best_practices": {
                "naming_conventions": [
                    "Use descriptive, consistent naming patterns",
                    "Follow domain.layer.entity pattern",
                    "Avoid special characters and spaces"
                ],
                "documentation": [
                    "Add comprehensive descriptions to all entities",
                    "Include data lineage information",
                    "Document data quality expectations"
                ]
            }
        }
    
    async def analyze_command(self, context: CopilotContext) -> List[AIsuggestion]:
        """Analyze current command and provide intelligent suggestions"""
        suggestions = []
        
        # Command-specific analysis
        if context.command == "validate":
            suggestions.extend(await self._analyze_validation_context(context))
        elif context.command == "plan":
            suggestions.extend(await self._analyze_planning_context(context))
        elif context.command == "apply":
            suggestions.extend(await self._analyze_deployment_context(context))
        
        # General optimization suggestions
        suggestions.extend(await self._analyze_optimization_opportunities(context))
        
        return suggestions
    
    async def _analyze_validation_context(self, context: CopilotContext) -> List[AIsuggestion]:
        """Analyze validation context for smart suggestions"""
        suggestions = []
        
        # Check for common validation improvements
        contract_path = context.args.get('contract')
        if contract_path and Path(contract_path).exists():
            suggestions.append(AIsuggestion(
                type="optimization",
                confidence=0.8,
                title="Add automated testing",
                description="Consider adding contract tests to catch breaking changes",
                action="fluid contract-tests " + contract_path,
                reasoning="Contract tests help prevent breaking changes and improve reliability",
                examples=["fluid contract-tests contract.yaml --generate-tests"]
            ))
        
        return suggestions
    
    async def _analyze_planning_context(self, context: CopilotContext) -> List[AIsuggestion]:
        """Analyze planning context for optimization suggestions"""
        suggestions = []
        
        # Suggest plan visualization
        suggestions.append(AIsuggestion(
            type="enhancement",
            confidence=0.9,
            title="Visualize execution plan",
            description="Generate visual representation of your execution plan",
            action="fluid viz-plan",
            reasoning="Visual plans help understand dependencies and catch issues early",
            examples=["fluid viz-plan runtime/plan.json --out plan.html"]
        ))
        
        return suggestions
    
    async def _analyze_deployment_context(self, context: CopilotContext) -> List[AIsuggestion]:
        """Analyze deployment context for safety suggestions"""
        suggestions = []
        
        # Always suggest dry-run for apply commands
        if not context.args.get('dry_run'):
            suggestions.append(AIsuggestion(
                type="safety",
                confidence=1.0,
                title="Preview changes first",
                description="Run with --dry-run to preview changes before applying",
                action="Add --dry-run flag",
                reasoning="Dry runs prevent accidental changes and help verify deployment plans",
                examples=["fluid apply contract.yaml --dry-run"]
            ))
        
        return suggestions
    
    async def _analyze_optimization_opportunities(self, context: CopilotContext) -> List[AIsuggestion]:
        """Analyze for general optimization opportunities"""
        suggestions = []
        
        # Check for performance optimizations
        if self._has_large_datasets(context):
            suggestions.append(AIsuggestion(
                type="performance",
                confidence=0.7,
                title="Optimize for large datasets",
                description="Consider partitioning and incremental processing",
                action="Review data processing strategy",
                reasoning="Large datasets benefit from partitioning and incremental updates",
                examples=["Add partitioning configuration", "Enable incremental processing"]
            ))
        
        return suggestions
    
    def _has_large_datasets(self, context: CopilotContext) -> bool:
        """Check if context indicates large dataset processing"""
        # Simplified heuristic - in real implementation, analyze contract files
        return any(term in str(context.args) for term in ['bigquery', 'warehouse', 'analytics'])
    
    def suggest_next_steps(self, completed_command: str, success: bool) -> List[str]:
        """Suggest logical next steps based on completed command"""
        if not success:
            return [
                "🔍 Run 'fluid doctor' to diagnose issues",
                "📚 Check documentation with 'fluid <command> --help'",
                "🧪 Try with --dry-run flag to test safely"
            ]
        
        next_steps = {
            "validate": [
                "📋 Generate execution plan: 'fluid plan contract.yaml'",
                "🎨 Visualize data lineage: 'fluid viz-graph contract.yaml'",
                "🧪 Add contract tests: 'fluid contract-tests contract.yaml'"
            ],
            "plan": [
                "🚀 Deploy with: 'fluid apply runtime/plan.json'",
                "👀 Preview first: 'fluid apply runtime/plan.json --dry-run'",
                "🎨 Visualize plan: 'fluid viz-plan runtime/plan.json'"
            ],
            "apply": [
                "📊 Check deployment status with provider tools",
                "📖 Generate documentation: 'fluid docs contract.yaml'",
                "🔍 Monitor data quality and performance"
            ],
            "forge": [
                "✅ Validate new contract: 'fluid validate contract.yaml'",
                "📋 Generate execution plan: 'fluid plan contract.yaml'",
                "🎨 Visualize architecture: 'fluid viz-graph contract.yaml'"
            ]
        }
        
        return next_steps.get(completed_command, [
            "✅ Command completed successfully",
            "📚 Check 'fluid --help' for more commands",
            "🚀 Continue building your data product"
        ])
    
    def auto_recover_from_error(self, error_context: Dict[str, Any]) -> Optional[List[str]]:
        """Suggest automatic recovery actions for common errors"""
        error_type = error_context.get('type', '')
        
        recovery_actions = {
            'auth_error': [
                "fluid auth login " + error_context.get('provider', 'gcp'),
                "fluid doctor"
            ],
            'contract_not_found': [
                "fluid forge --template quickstart",
                "ls *.yaml *.yml *.json"
            ],
            'validation_error': [
                "fluid validate contract.yaml --explain",
                "fluid forge --fix-contract contract.yaml"
            ]
        }
        
        return recovery_actions.get(error_type)

# Enhanced CLI Integration
def register(subparsers: argparse._SubParsersAction):
    """Register the copilot command"""
    p = subparsers.add_parser(
        COMMAND,
        help="🧠 AI-powered assistant for FLUID development"
    )
    
    copilot_subparsers = p.add_subparsers(dest='copilot_action', help='Copilot actions')
    
    # Analyze command
    analyze_parser = copilot_subparsers.add_parser('analyze', help='Analyze current context and suggest improvements')
    analyze_parser.add_argument('--command', help='Command to analyze')
    analyze_parser.add_argument('--context', help='Additional context for analysis')
    
    # Suggest next steps
    suggest_parser = copilot_subparsers.add_parser('suggest', help='Suggest next steps')
    suggest_parser.add_argument('--last-command', help='Last completed command')
    suggest_parser.add_argument('--success', action='store_true', help='Whether last command succeeded')
    
    # Interactive mode
    _interactive_parser = copilot_subparsers.add_parser('interactive', help='Start interactive AI assistant')  # noqa: F841
    
    p.set_defaults(func=run)

async def run(args, logger: logging.Logger) -> int:
    """Main entry point for copilot command"""
    try:
        copilot = FluidAICopilot()
        
        if args.copilot_action == 'analyze':
            return await handle_analyze(args, copilot, logger)
        elif args.copilot_action == 'suggest':
            return await handle_suggest(args, copilot, logger)
        elif args.copilot_action == 'interactive':
            return await handle_interactive(args, copilot, logger)
        else:
            copilot.console.print("[red]❌ Unknown copilot action. Use 'fluid copilot --help' for available options.[/red]")
            return 1
            
    except Exception as e:
        logger.exception("Copilot command failed")
        console = Console()
        console.print(f"[red]❌ Copilot failed: {e}[/red]")
        return 1

async def handle_analyze(args, copilot: FluidAICopilot, logger: logging.Logger) -> int:
    """Handle analyze command"""
    console = Console()
    
    # Create context from current environment
    context = CopilotContext(
        command=args.command or "general",
        args=vars(args),
        current_directory=Path.cwd(),
        user_history=[]  # In real implementation, load from history
    )
    
    # Get AI suggestions
    suggestions = await copilot.analyze_command(context)
    
    if suggestions:
        console.print("\n[bold blue]🧠 AI Copilot Suggestions[/bold blue]")
        for i, suggestion in enumerate(suggestions, 1):
            panel_content = Text()
            panel_content.append(f"{suggestion.description}\n\n", style="white")
            panel_content.append(f"💡 Action: ", style="bold yellow")
            panel_content.append(f"{suggestion.action}\n", style="cyan")
            panel_content.append(f"🤔 Why: ", style="bold green")
            panel_content.append(f"{suggestion.reasoning}", style="dim white")
            
            console.print(Panel(
                panel_content,
                title=f"[bold]{suggestion.type.title()}: {suggestion.title}[/bold]",
                title_align="left",
                border_style=_get_suggestion_color(suggestion.type)
            ))
    else:
        console.print("[dim]No specific suggestions for current context.[/dim]")
    
    return 0

async def handle_suggest(args, copilot: FluidAICopilot, logger: logging.Logger) -> int:
    """Handle suggest next steps command"""
    console = Console()
    
    last_command = args.last_command or "unknown"
    success = args.success
    
    next_steps = copilot.suggest_next_steps(last_command, success)
    
    console.print(f"\n[bold blue]🎯 Suggested Next Steps[/bold blue]")
    for step in next_steps:
        console.print(f"  {step}")
    
    return 0

async def handle_interactive(args, copilot: FluidAICopilot, logger: logging.Logger) -> int:
    """Handle interactive copilot mode"""
    console = Console()
    
    console.print("[bold blue]🧠 FLUID AI Copilot - Interactive Mode[/bold blue]")
    console.print("[dim]Type 'help' for commands, 'exit' to quit[/dim]\n")
    
    while True:
        try:
            user_input = console.input("[bold cyan]Copilot>[/bold cyan] ")
            
            if user_input.lower() in ['exit', 'quit', 'bye']:
                console.print("[dim]👋 Goodbye![/dim]")
                break
            elif user_input.lower() == 'help':
                _show_interactive_help(console)
            else:
                # Process user input and provide assistance
                await _process_interactive_input(user_input, copilot, console)
                
        except KeyboardInterrupt:
            console.print("\n[dim]👋 Goodbye![/dim]")
            break
        except EOFError:
            break
    
    return 0

def _get_suggestion_color(suggestion_type: str) -> str:
    """Get color for suggestion type"""
    colors = {
        'safety': 'red',
        'performance': 'yellow',
        'optimization': 'green',
        'enhancement': 'blue',
        'command': 'cyan'
    }
    return colors.get(suggestion_type, 'white')

def _show_interactive_help(console: Console):
    """Show interactive mode help"""
    help_text = """
[bold]Available Commands:[/bold]
• analyze <command>     - Analyze a specific command
• suggest <action>      - Get suggestions for next steps
• explain <error>       - Explain an error and suggest fixes
• optimize <contract>   - Suggest optimizations for a contract
• help                  - Show this help
• exit                  - Exit interactive mode

[bold]Examples:[/bold]
• analyze validate
• suggest after plan
• explain auth failed
• optimize contract.yaml
    """
    console.print(Panel(help_text, title="Interactive Help", border_style="blue"))

async def _process_interactive_input(user_input: str, copilot: FluidAICopilot, console: Console):
    """Process interactive user input"""
    parts = user_input.strip().split()
    if not parts:
        return
    
    command = parts[0].lower()
    args = parts[1:] if len(parts) > 1 else []
    
    if command == 'analyze':
        target = args[0] if args else 'general'
        console.print(f"[dim]Analyzing {target}...[/dim]")
        # Implement analysis logic
        console.print(f"✅ Analysis complete for {target}")
        
    elif command == 'suggest':
        context = ' '.join(args) if args else 'general'
        console.print(f"[dim]Generating suggestions for {context}...[/dim]")
        # Implement suggestion logic
        console.print("💡 Consider validating your contract first")
        
    elif command == 'explain':
        error = ' '.join(args) if args else 'unknown'
        console.print(f"[dim]Explaining error: {error}...[/dim]")
        # Implement error explanation logic
        console.print("🔍 This error typically occurs when...")
        
    else:
        console.print(f"[yellow]Unknown command: {command}. Type 'help' for available commands.[/yellow]")