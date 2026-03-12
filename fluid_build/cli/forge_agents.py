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
Domain-specific AI Agents for FLUID Forge

Specialized agents with domain expertise for creating data products
in specific industries and use cases.
"""
from pathlib import Path
from typing import Dict, Any, List, Optional
from fluid_build.cli.console import cprint, error as console_error


class AIAgentBase:
    """Base class for AI agents - minimal version for domain agents"""
    
    def __init__(self, name: str, description: str, domain: str):
        self.name = name
        self.description = description
        self.domain = domain
        try:
            from rich.console import Console
            self.console = Console()
        except ImportError:
            self.console = None
    
    def get_questions(self) -> List[Dict[str, Any]]:
        """Get questions for this agent"""
        raise NotImplementedError
    
    def analyze_requirements(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze requirements and provide suggestions"""
        raise NotImplementedError
    
    def create_project(self, target_dir: Path, context: Dict[str, Any]) -> bool:
        """Create project using AI agent - delegates to ForgeEngine"""
        try:
            # Analyze requirements and generate suggestions
            suggestions = self.analyze_requirements(context)
            
            # Show AI analysis to user
            self._show_ai_analysis(context, suggestions)
            
            # Create project configuration for ForgeEngine
            project_config = self._create_forge_config(target_dir, context, suggestions)
            
            # Use ForgeEngine to create and validate the project properly
            success = self._create_with_forge_engine(project_config)
            
            if success:
                # Show next steps
                self._show_next_steps(target_dir, context, suggestions)
                return True
            else:
                if self.console:
                    self.console.print("[red]❌ Project creation failed validation[/red]")
                return False
            
        except Exception as e:
            if self.console:
                self.console.print(f"[red]❌ Failed to create project: {e}[/red]")
            else:
                console_error(f"Failed to create project: {e}")
            return False
    
    def _create_forge_config(self, target_dir: Path, context: Dict[str, Any], suggestions: Dict[str, Any]) -> Dict[str, Any]:
        """Create configuration for ForgeEngine"""
        goal = context.get("project_goal", "Data Product")
        
        # Create project name that will pass validation
        project_name = self._sanitize_project_name(goal)
        
        return {
            'name': project_name,
            'description': f"AI-generated {goal} ({self.domain} domain)",
            'template': suggestions['recommended_template'],
            'provider': suggestions['recommended_provider'],
            'target_dir': str(target_dir),
            'ai_context': context,
            'ai_suggestions': suggestions,
            'domain': self.domain
        }
    
    def _sanitize_project_name(self, goal: str) -> str:
        """Create a valid project name from goal"""
        import re
        name = goal.lower()
        name = re.sub(r'[^a-z0-9\s\-_]', '', name)
        name = re.sub(r'\s+', '-', name)
        name = re.sub(r'-+', '-', name)
        name = name.strip('-')
        
        if name and not name[0].isalpha():
            name = f"project-{name}"
        
        if not name:
            name = f"{self.domain}-data-product"
            
        return name
    
    def _create_with_forge_engine(self, project_config: Dict[str, Any]) -> bool:
        """Use ForgeEngine to create and validate project"""
        try:
            from fluid_build.forge import ForgeEngine
            
            if self.console:
                with self.console.status(f"[bold blue]🔧 Generating {self.domain} project...", spinner="dots"):
                    engine = ForgeEngine()
                    success = engine.run_with_config(project_config, dry_run=False)
                return success
            else:
                cprint(f"🔧 Generating {self.domain} project...")
                engine = ForgeEngine()
                return engine.run_with_config(project_config, dry_run=False)
            
        except Exception as e:
            if self.console:
                self.console.print(f"[red]❌ ForgeEngine integration failed: {e}[/red]")
            return False
    
    def _show_ai_analysis(self, context: Dict[str, Any], suggestions: Dict[str, Any]):
        """Show AI analysis to user"""
        if not self.console:
            return
        
        from rich.panel import Panel
        
        analysis = f"""
[bold cyan]📊 {self.domain.title()} Domain Analysis[/bold cyan]

[yellow]Template:[/yellow] {suggestions.get('recommended_template')}
[yellow]Provider:[/yellow] {suggestions.get('recommended_provider')}
[yellow]Patterns:[/yellow] {', '.join(suggestions.get('recommended_patterns', []))}

[green]✓[/green] Analysis complete - optimized for {self.domain} use cases
        """
        
        self.console.print(Panel(analysis.strip(), border_style="blue"))
    
    def _show_next_steps(self, target_dir: Path, context: Dict[str, Any], suggestions: Dict[str, Any]):
        """Show intelligent next steps"""
        if not self.console:
            return
        
        from rich.panel import Panel
        
        next_steps = f"""
🎯 **Immediate Next Steps:**
1. Review and customize contract.fluid.yaml
2. Run `fluid validate contract.fluid.yaml` to check your setup
3. Configure your {suggestions['recommended_provider']} provider credentials

🚀 **Recommended Workflow:**
1. `make validate` - Validate your contract
2. `make plan` - Generate execution plan  
3. `make apply` - Deploy your data product

💡 **{self.domain.title()}-Specific Tips:**
"""
        
        # Add domain-specific tips
        if self.domain == "finance" and suggestions.get('security_requirements'):
            next_steps += "• Review security and compliance requirements\n"
            next_steps += "• Set up audit logging and access controls\n"
        elif self.domain == "healthcare":
            next_steps += "• Ensure HIPAA compliance measures are in place\n"
            next_steps += "• Review PHI handling procedures\n"
        elif self.domain == "retail":
            next_steps += "• Configure personalization engine settings\n"
            next_steps += "• Set up A/B testing framework\n"
        
        next_steps += "\n[dim]Generated by FLUID AI Agent - Domain: " + self.domain + "[/dim]"
        
        self.console.print(Panel(
            next_steps.strip(),
            title="🚀 What's Next?",
            border_style="green"
        ))


class FinanceAgent(AIAgentBase):
    """Finance and banking domain expert"""
    
    def __init__(self):
        super().__init__(
            name="finance",
            description="Expert in financial data products, regulatory compliance, and risk analytics",
            domain="finance"
        )
    
    def get_questions(self) -> List[Dict[str, Any]]:
        """Get finance-specific questions"""
        return [
            {
                "key": "product_type",
                "question": "What type of financial product are you building?",
                "type": "choice",
                "choices": ["risk_analytics", "trading_platform", "fraud_detection", "customer_analytics", "compliance_reporting"],
                "required": True
            },
            {
                "key": "data_sources",
                "question": "What data sources will you use?",
                "type": "text",
                "required": True,
                "default": "Transaction data, customer profiles, market data"
            },
            {
                "key": "compliance_requirements",
                "question": "Any specific compliance requirements?",
                "type": "choice",
                "choices": ["sox", "gdpr", "pci_dss", "basel_iii", "mifid_ii", "none"],
                "required": False
            },
            {
                "key": "real_time",
                "question": "Do you need real-time processing?",
                "type": "choice",
                "choices": ["yes", "no"],
                "default": "no"
            }
        ]
    
    def analyze_requirements(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze finance-specific requirements"""
        product_type = context.get("product_type", "customer_analytics")
        compliance = context.get("compliance_requirements", "none")
        real_time = context.get("real_time", "no") == "yes"
        
        suggestions = {
            "recommended_template": "finance-analytics",
            "recommended_provider": "gcp",
            "recommended_patterns": [],
            "architecture_suggestions": [],
            "best_practices": [],
            "technology_stack": [],
            "security_requirements": []
        }
        
        # Template selection
        if product_type == "risk_analytics":
            suggestions["recommended_template"] = "risk-analytics-template"
            suggestions["recommended_patterns"].extend(["monte_carlo", "stress_testing"])
            suggestions["technology_stack"].extend(["Python", "pandas", "numpy", "scikit-learn"])
        elif product_type == "trading_platform":
            suggestions["recommended_template"] = "trading-platform-template"
            suggestions["recommended_patterns"].extend(["event_sourcing", "cqrs"])
            suggestions["technology_stack"].extend(["Python", "Redis", "Kafka"])
        elif product_type == "fraud_detection":
            suggestions["recommended_template"] = "ml-pipeline-template"
            suggestions["recommended_patterns"].extend(["anomaly_detection", "real_time_scoring"])
            suggestions["technology_stack"].extend(["Python", "TensorFlow", "Kafka"])
        else:
            suggestions["recommended_template"] = "analytics-template"
        
        # Real-time processing
        if real_time:
            suggestions["recommended_patterns"].append("streaming_pipeline")
            suggestions["technology_stack"].extend(["Apache Kafka", "Apache Flink"])
            suggestions["architecture_suggestions"].append("Implement stream processing with Kafka")
        
        # Compliance requirements
        if compliance != "none":
            suggestions["security_requirements"].extend([
                "Implement data encryption at rest and in transit",
                "Set up audit logging for all data access",
                "Implement role-based access control (RBAC)",
                "Enable data retention policies",
                "Set up compliance reporting dashboards"
            ])
            
            if compliance == "sox":
                suggestions["security_requirements"].append("Implement SOX-compliant audit trails")
            elif compliance == "gdpr":
                suggestions["security_requirements"].extend([
                    "Implement right-to-be-forgotten workflows",
                    "Enable data portability",
                    "Set up consent management"
                ])
            elif compliance == "pci_dss":
                suggestions["security_requirements"].extend([
                    "Tokenize credit card data",
                    "Implement network segmentation",
                    "Regular security scans"
                ])
        
        # Best practices
        suggestions["best_practices"].extend([
            "Implement data lineage tracking",
            "Use version control for all models and code",
            "Set up automated testing for data quality",
            "Implement disaster recovery procedures",
            "Document data governance policies"
        ])
        
        # Architecture
        suggestions["architecture_suggestions"].extend([
            "Use medallion architecture (bronze/silver/gold)",
            "Implement data validation at ingestion",
            "Set up monitoring and alerting",
            "Consider data mesh for multiple teams"
        ])
        
        return suggestions


class HealthcareAgent(AIAgentBase):
    """Healthcare and life sciences domain expert"""
    
    def __init__(self):
        super().__init__(
            name="healthcare",
            description="Expert in healthcare data products, HIPAA compliance, and clinical analytics",
            domain="healthcare"
        )
    
    def get_questions(self) -> List[Dict[str, Any]]:
        """Get healthcare-specific questions"""
        return [
            {
                "key": "product_type",
                "question": "What type of healthcare product are you building?",
                "type": "choice",
                "choices": ["patient_analytics", "clinical_research", "population_health", "ehr_integration", "drug_discovery"],
                "required": True
            },
            {
                "key": "data_sources",
                "question": "What data sources will you use?",
                "type": "text",
                "required": True,
                "default": "EHR data, claims data, lab results"
            },
            {
                "key": "hipaa_required",
                "question": "Is HIPAA compliance required?",
                "type": "choice",
                "choices": ["yes", "no"],
                "default": "yes"
            },
            {
                "key": "phi_handling",
                "question": "Will you handle PHI (Protected Health Information)?",
                "type": "choice",
                "choices": ["yes", "no", "deidentified_only"],
                "default": "yes"
            }
        ]
    
    def analyze_requirements(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze healthcare-specific requirements"""
        product_type = context.get("product_type", "patient_analytics")
        hipaa = context.get("hipaa_required", "yes") == "yes"
        phi = context.get("phi_handling", "yes")
        
        suggestions = {
            "recommended_template": "healthcare-analytics",
            "recommended_provider": "gcp",
            "recommended_patterns": [],
            "architecture_suggestions": [],
            "best_practices": [],
            "technology_stack": [],
            "security_requirements": []
        }
        
        # Template selection
        if product_type == "clinical_research":
            suggestions["recommended_template"] = "clinical-research-template"
            suggestions["technology_stack"].extend(["Python", "R", "Jupyter"])
        elif product_type == "population_health":
            suggestions["recommended_template"] = "population-health-template"
            suggestions["recommended_patterns"].append("cohort_analysis")
        else:
            suggestions["recommended_template"] = "analytics-template"
        
        # HIPAA compliance
        if hipaa:
            suggestions["security_requirements"].extend([
                "Implement HIPAA-compliant data encryption",
                "Set up BAA (Business Associate Agreement) tracking",
                "Enable comprehensive audit logging",
                "Implement access controls with MFA",
                "Set up breach notification procedures",
                "Regular HIPAA compliance audits"
            ])
        
        # PHI handling
        if phi == "yes":
            suggestions["security_requirements"].extend([
                "Implement PHI data masking",
                "Use secure enclaves for PHI processing",
                "Enable data lineage for PHI",
                "Set up automatic PHI detection"
            ])
            suggestions["architecture_suggestions"].append("Separate PHI data into secure zones")
        elif phi == "deidentified_only":
            suggestions["architecture_suggestions"].append("Implement de-identification pipeline at ingestion")
            suggestions["best_practices"].append("Validate de-identification meets Safe Harbor or Expert Determination")
        
        # Best practices
        suggestions["best_practices"].extend([
            "Use standardized medical codes (ICD-10, SNOMED)",
            "Implement data quality checks for clinical data",
            "Document all data transformations",
            "Set up monitoring for data anomalies",
            "Implement version control for algorithms"
        ])
        
        # Architecture
        suggestions["architecture_suggestions"].extend([
            "Use FHIR standards for interoperability",
            "Implement HL7 message processing if needed",
            "Consider federated learning for privacy",
            "Set up secure data sharing mechanisms"
        ])
        
        return suggestions


class RetailAgent(AIAgentBase):
    """Retail and e-commerce domain expert"""
    
    def __init__(self):
        super().__init__(
            name="retail",
            description="Expert in retail analytics, customer personalization, and inventory optimization",
            domain="retail"
        )
    
    def get_questions(self) -> List[Dict[str, Any]]:
        """Get retail-specific questions"""
        return [
            {
                "key": "product_type",
                "question": "What type of retail product are you building?",
                "type": "choice",
                "choices": ["customer_360", "recommendation_engine", "inventory_optimization", "price_optimization", "demand_forecasting"],
                "required": True
            },
            {
                "key": "data_sources",
                "question": "What data sources will you use?",
                "type": "text",
                "required": True,
                "default": "Transaction data, customer profiles, inventory data"
            },
            {
                "key": "real_time_personalization",
                "question": "Do you need real-time personalization?",
                "type": "choice",
                "choices": ["yes", "no"],
                "default": "yes"
            },
            {
                "key": "scale",
                "question": "Expected data scale?",
                "type": "choice",
                "choices": ["small (<1M records)", "medium (1M-100M)", "large (>100M)"],
                "required": True
            }
        ]
    
    def analyze_requirements(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze retail-specific requirements"""
        product_type = context.get("product_type", "customer_360")
        real_time = context.get("real_time_personalization", "yes") == "yes"
        scale = context.get("scale", "medium")
        
        suggestions = {
            "recommended_template": "retail-analytics",
            "recommended_provider": "gcp",
            "recommended_patterns": [],
            "architecture_suggestions": [],
            "best_practices": [],
            "technology_stack": [],
            "performance_optimization": []
        }
        
        # Template selection
        if product_type == "recommendation_engine":
            suggestions["recommended_template"] = "ml-pipeline-template"
            suggestions["recommended_patterns"].extend(["collaborative_filtering", "content_based"])
            suggestions["technology_stack"].extend(["Python", "TensorFlow", "Redis"])
        elif product_type == "inventory_optimization":
            suggestions["recommended_template"] = "optimization-template"
            suggestions["recommended_patterns"].append("demand_forecasting")
            suggestions["technology_stack"].extend(["Python", "OR-Tools", "Prophet"])
        elif product_type == "customer_360":
            suggestions["recommended_template"] = "customer360-template"
            suggestions["recommended_patterns"].append("customer_segmentation")
        else:
            suggestions["recommended_template"] = "analytics-template"
        
        # Real-time requirements
        if real_time:
            suggestions["recommended_patterns"].append("streaming_pipeline")
            suggestions["technology_stack"].extend(["Redis", "Kafka"])
            suggestions["architecture_suggestions"].extend([
                "Implement feature store for real-time serving",
                "Use caching layer for frequently accessed data",
                "Set up A/B testing framework"
            ])
        
        # Scale considerations
        if "large" in scale:
            suggestions["performance_optimization"].extend([
                "Use partitioning for large tables",
                "Implement incremental processing",
                "Consider distributed processing (Spark)",
                "Set up data lake architecture",
                "Use columnar storage (Parquet)"
            ])
            suggestions["recommended_provider"] = "gcp"  # BigQuery for scale
        elif "small" in scale:
            suggestions["recommended_provider"] = "local"  # Can start locally
            suggestions["performance_optimization"].append("PostgreSQL sufficient for this scale")
        
        # Best practices
        suggestions["best_practices"].extend([
            "Implement customer privacy controls",
            "Set up experimentation framework",
            "Track customer journey metrics",
            "Implement data quality monitoring",
            "Set up business metrics dashboards"
        ])
        
        # Architecture
        suggestions["architecture_suggestions"].extend([
            "Use event-driven architecture for transactions",
            "Implement CDC (Change Data Capture)",
            "Set up data marts for business teams",
            "Consider customer data platform (CDP) patterns"
        ])
        
        return suggestions


# Registry of domain agents
DOMAIN_AGENTS = {
    "finance": FinanceAgent,
    "healthcare": HealthcareAgent,
    "retail": RetailAgent,
}


def get_agent(agent_name: str) -> AIAgentBase:
    """Get a domain agent by name
    
    Args:
        agent_name: Name of the agent
    
    Returns:
        AIAgentBase instance
    
    Raises:
        ValueError: If agent not found
    """
    if agent_name not in DOMAIN_AGENTS:
        raise ValueError(f"Agent '{agent_name}' not found. Available: {', '.join(DOMAIN_AGENTS.keys())}")
    
    agent_class = DOMAIN_AGENTS[agent_name]
    return agent_class()


def list_agents() -> List[Dict[str, str]]:
    """List all available domain agents
    
    Returns:
        List of agent info dictionaries
    """
    agents = []
    for name, agent_class in DOMAIN_AGENTS.items():
        agent = agent_class()
        agents.append({
            "name": agent.name,
            "domain": agent.domain,
            "description": agent.description
        })
    return agents
