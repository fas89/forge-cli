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
Dynamic DataOps Pipeline Templates for FLUID

This module provides comprehensive pipeline configuration templates for different
CI/CD providers with full FLUID workflow integration. Teams can choose their
preferred provider and get a complete DataOps pipeline that includes:

1. Validation (fluid validate)
2. Planning (fluid plan) 
3. Application (fluid apply)
4. Testing (fluid test)
5. Visualization (fluid viz)
6. Publishing (fluid publish --format opds)
7. Marketplace publishing (fluid marketplace publish)

The templates support:
- Multi-environment deployments (dev, staging, prod)
- Approval gates and manual triggers
- Artifact management and versioning
- Notification integrations
- Security scanning and compliance
- Performance monitoring
- Rollback capabilities
"""

from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import json
import json
from fluid_build.cli.console import cprint

try:
    import yaml
except ImportError:
    # Fallback YAML implementation
    class _YamlFallback:
        def dump(self, data, **kwargs):
            return json.dumps(data, indent=kwargs.get('indent', 2))
        
        def dump_all(self, documents, **kwargs):
            results = []
            for doc in documents:
                results.append(self.dump(doc, **kwargs))
            return '\n---\n'.join(results)
    
    yaml = _YamlFallback()


class PipelineProvider(Enum):
    """Supported CI/CD providers"""
    GITHUB_ACTIONS = "github_actions"
    GITLAB_CI = "gitlab_ci"
    AZURE_DEVOPS = "azure_devops"
    JENKINS = "jenkins"
    BITBUCKET = "bitbucket"
    CIRCLE_CI = "circle_ci"
    TEKTON = "tekton"


class PipelineComplexity(Enum):
    """Pipeline complexity levels"""
    BASIC = "basic"           # Simple validate -> apply workflow
    STANDARD = "standard"     # Full workflow with testing
    ADVANCED = "advanced"     # Multi-environment with approvals
    ENTERPRISE = "enterprise" # Full governance and compliance


@dataclass
class PipelineConfig:
    """Configuration for pipeline generation"""
    provider: PipelineProvider
    complexity: PipelineComplexity
    environments: List[str] = None
    enable_approvals: bool = False
    enable_security_scan: bool = True
    enable_performance_monitoring: bool = True
    enable_marketplace_publishing: bool = False
    notification_channels: List[str] = None
    custom_steps: List[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.environments is None:
            if self.complexity == PipelineComplexity.BASIC:
                self.environments = ["dev"]
            elif self.complexity == PipelineComplexity.STANDARD:
                self.environments = ["dev", "staging"]
            else:
                self.environments = ["dev", "staging", "prod"]
        
        if self.notification_channels is None:
            self.notification_channels = []
        
        if self.custom_steps is None:
            self.custom_steps = []


class PipelineTemplateGenerator:
    """Generates CI/CD pipeline templates for different providers"""
    
    def __init__(self):
        self.templates = {}
        self._initialize_templates()
    
    def _initialize_templates(self):
        """Initialize all pipeline templates"""
        self.templates = {
            PipelineProvider.GITHUB_ACTIONS: GitHubActionsTemplate(),
            PipelineProvider.GITLAB_CI: GitLabCITemplate(),
            PipelineProvider.AZURE_DEVOPS: AzureDevOpsTemplate(),
            PipelineProvider.JENKINS: JenkinsTemplate(),
            PipelineProvider.BITBUCKET: BitbucketTemplate(),
            PipelineProvider.CIRCLE_CI: CircleCITemplate(),
            PipelineProvider.TEKTON: TektonTemplate(),
        }
    
    def generate_pipeline(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate pipeline configuration files"""
        template = self.templates.get(config.provider)
        if not template:
            raise ValueError(f"Unsupported provider: {config.provider}")
        
        return template.generate(config)
    
    def list_available_providers(self) -> List[str]:
        """List available pipeline providers"""
        return [provider.value for provider in PipelineProvider]
    
    def get_provider_features(self, provider: PipelineProvider) -> Dict[str, Any]:
        """Get features supported by a provider"""
        template = self.templates.get(provider)
        if not template:
            return {}
        
        return template.get_features()


class BasePipelineTemplate:
    """Base class for pipeline templates"""
    
    def __init__(self):
        self.provider_name = "unknown"
        self.file_extensions = [".yml"]
    
    def generate(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate pipeline configuration"""
        raise NotImplementedError
    
    def get_features(self) -> Dict[str, Any]:
        """Get supported features"""
        return {
            "multi_environment": True,
            "approvals": True,
            "security_scanning": True,
            "artifact_management": True,
            "notifications": True,
            "parallel_execution": True,
            "matrix_builds": True
        }
    
    def _get_fluid_commands(self) -> Dict[str, str]:
        """Get standard FLUID commands for different stages"""
        return {
            "validate": "fluid validate --strict",
            "plan": "fluid plan --output plan.json",
            "apply": "fluid apply --plan plan.json",
            "test": "fluid test --coverage",
            "contract_test": "fluid contract-test --all",
            "visualize": "fluid viz-plan --output pipeline-viz.html && fluid viz-graph --output dependency-graph.png",
            "publish_opds": "fluid export-opds --output opds-catalog.json",
            "marketplace_publish": "fluid marketplace publish --catalog opds-catalog.json",
            "doctor": "fluid doctor --comprehensive"
        }
    
    def _get_common_environment_vars(self) -> Dict[str, str]:
        """Get common environment variables"""
        return {
            "FLUID_LOG_LEVEL": "INFO",
            "FLUID_CONFIG_PATH": "./fluid_config",
            "PYTHONPATH": ".",
            "PIP_CACHE_DIR": ".pip-cache"
        }


class GitHubActionsTemplate(BasePipelineTemplate):
    """GitHub Actions pipeline template"""
    
    def __init__(self):
        super().__init__()
        self.provider_name = "GitHub Actions"
        self.file_extensions = [".yml", ".yaml"]
    
    def generate(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate GitHub Actions workflow"""
        
        if config.complexity == PipelineComplexity.BASIC:
            return self._generate_basic_workflow(config)
        elif config.complexity == PipelineComplexity.STANDARD:
            return self._generate_standard_workflow(config)
        elif config.complexity == PipelineComplexity.ADVANCED:
            return self._generate_advanced_workflow(config)
        else:  # ENTERPRISE
            return self._generate_enterprise_workflow(config)
    
    def _generate_basic_workflow(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate basic GitHub Actions workflow"""
        
        commands = self._get_fluid_commands()
        env_vars = self._get_common_environment_vars()
        
        workflow = {
            "name": "FLUID DataOps Pipeline",
            "on": {
                "push": {"branches": ["main", "develop"]},
                "pull_request": {"branches": ["main"]}
            },
            "env": env_vars,
            "jobs": {
                "fluid-pipeline": {
                    "runs-on": "ubuntu-latest",
                    "steps": [
                        {
                            "name": "Checkout code",
                            "uses": "actions/checkout@v4"
                        },
                        {
                            "name": "Set up Python",
                            "uses": "actions/setup-python@v4",
                            "with": {"python-version": "3.9"}
                        },
                        {
                            "name": "Install FLUID",
                            "run": "pip install -r requirements.txt"
                        },
                        {
                            "name": "FLUID Doctor Check",
                            "run": commands["doctor"]
                        },
                        {
                            "name": "Validate Configuration",
                            "run": commands["validate"]
                        },
                        {
                            "name": "Generate Plan",
                            "run": commands["plan"]
                        },
                        {
                            "name": "Apply Changes",
                            "run": commands["apply"],
                            "if": "github.ref == 'refs/heads/main'"
                        },
                        {
                            "name": "Run Tests",
                            "run": commands["test"]
                        },
                        {
                            "name": "Generate Artifacts",
                            "run": f"{commands['visualize']} && {commands['publish_opds']}"
                        },
                        {
                            "name": "Upload Artifacts",
                            "uses": "actions/upload-artifact@v3",
                            "with": {
                                "name": "fluid-artifacts",
                                "path": "plan.json\npipeline-viz.html\ndependency-graph.png\nopds-catalog.json\ntest-results/"
                            }
                        }
                    ]
                }
            }
        }
        
        return {".github/workflows/fluid-pipeline.yml": yaml.dump(workflow, indent=2)}
    
    def _generate_standard_workflow(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate standard GitHub Actions workflow with multiple environments"""
        
        commands = self._get_fluid_commands()
        env_vars = self._get_common_environment_vars()
        
        workflow = {
            "name": "FLUID DataOps Pipeline - Standard",
            "on": {
                "push": {"branches": ["main", "develop", "feature/*"]},
                "pull_request": {"branches": ["main", "develop"]}
            },
            "env": env_vars,
            "jobs": {
                "validate": {
                    "runs-on": "ubuntu-latest",
                    "outputs": {
                        "changes-detected": "${{ steps.changes.outputs.changes }}"
                    },
                    "steps": [
                        {"name": "Checkout", "uses": "actions/checkout@v4"},
                        {"name": "Setup Python", "uses": "actions/setup-python@v4", "with": {"python-version": "3.9"}},
                        {"name": "Install Dependencies", "run": "pip install -r requirements.txt"},
                        {"name": "FLUID Doctor", "run": commands["doctor"]},
                        {"name": "Validate", "run": commands["validate"]},
                        {
                            "name": "Detect Changes",
                            "id": "changes", 
                            "run": "if git diff --name-only HEAD~1 | grep -E '\\.(sql|py|yaml|json)$'; then\n  echo \"changes=true\" >> $GITHUB_OUTPUT\nelse\n  echo \"changes=false\" >> $GITHUB_OUTPUT\nfi"
                        }
                    ]
                },
                "plan": {
                    "needs": "validate",
                    "runs-on": "ubuntu-latest",
                    "if": "needs.validate.outputs.changes-detected == 'true'",
                    "steps": [
                        {"name": "Checkout", "uses": "actions/checkout@v4"},
                        {"name": "Setup Python", "uses": "actions/setup-python@v4", "with": {"python-version": "3.9"}},
                        {"name": "Install Dependencies", "run": "pip install -r requirements.txt"},
                        {"name": "Generate Plan", "run": commands["plan"]},
                        {"name": "Upload Plan", "uses": "actions/upload-artifact@v3", "with": {"name": "plan", "path": "plan.json"}}
                    ]
                },
                "test": {
                    "needs": "validate",
                    "runs-on": "ubuntu-latest",
                    "strategy": {
                        "matrix": {
                            "test-type": ["unit", "integration", "contract"]
                        }
                    },
                    "steps": [
                        {"name": "Checkout", "uses": "actions/checkout@v4"},
                        {"name": "Setup Python", "uses": "actions/setup-python@v4", "with": {"python-version": "3.9"}},
                        {"name": "Install Dependencies", "run": "pip install -r requirements.txt"},
                        {
                            "name": "Run Tests",
                            "run": "fluid test --type ${{ matrix.test-type }} --output test-results-${{ matrix.test-type }}.xml"
                        },
                        {
                            "name": "Upload Test Results",
                            "uses": "actions/upload-artifact@v3",
                            "with": {
                                "name": "test-results-${{ matrix.test-type }}",
                                "path": "test-results-${{ matrix.test-type }}.xml"
                            }
                        }
                    ]
                }
            }
        }
        
        # Add deployment jobs for each environment
        for env in config.environments:
            job_name = f"deploy-{env}"
            
            depends_on = ["plan", "test"]
            if env == "prod":
                depends_on.append("deploy-staging")
            
            workflow["jobs"][job_name] = {
                "needs": depends_on,
                "runs-on": "ubuntu-latest",
                "environment": env,
                "if": f"github.ref == 'refs/heads/main' || (github.ref == 'refs/heads/develop' && '{env}' != 'prod')",
                "steps": [
                    {"name": "Checkout", "uses": "actions/checkout@v4"},
                    {"name": "Setup Python", "uses": "actions/setup-python@v4", "with": {"python-version": "3.9"}},
                    {"name": "Install Dependencies", "run": "pip install -r requirements.txt"},
                    {"name": "Download Plan", "uses": "actions/download-artifact@v3", "with": {"name": "plan"}},
                    {"name": f"Deploy to {env.upper()}", "run": f"FLUID_ENV={env} {commands['apply']}"},
                    {"name": "Run Contract Tests", "run": f"FLUID_ENV={env} {commands['contract_test']}"},
                    {
                        "name": "Generate Visualization",
                        "run": commands["visualize"],
                        "if": f"'{env}' == 'prod'"
                    },
                    {
                        "name": "Publish to Marketplace",
                        "run": f"{commands['publish_opds']} && {commands['marketplace_publish']}",
                        "if": f"'{env}' == 'prod' && {str(config.enable_marketplace_publishing).lower()}"
                    }
                ]
            }
        
        return {".github/workflows/fluid-standard.yml": yaml.dump(workflow, indent=2)}
    
    def _generate_advanced_workflow(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate advanced workflow with approvals and security"""
        
        # This would include the standard workflow plus:
        # - Security scanning
        # - Approval gates
        # - Advanced monitoring
        # - Rollback capabilities
        
        files = self._generate_standard_workflow(config)
        
        # Add security workflow
        security_workflow = {
            "name": "Security Scan",
            "on": {
                "push": {"branches": ["main", "develop"]},
                "schedule": [{"cron": "0 2 * * *"}]  # Daily at 2 AM
            },
            "jobs": {
                "security-scan": {
                    "runs-on": "ubuntu-latest",
                    "steps": [
                        {"name": "Checkout", "uses": "actions/checkout@v4"},
                        {"name": "Run Trivy vulnerability scanner", "uses": "aquasecurity/trivy-action@master"},
                        {"name": "FLUID Security Check", "run": "fluid validate --security-only"},
                        {"name": "Upload SARIF", "uses": "github/codeql-action/upload-sarif@v2"}
                    ]
                }
            }
        }
        
        files[".github/workflows/security.yml"] = yaml.dump(security_workflow, indent=2)
        
        return files
    
    def _generate_enterprise_workflow(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate enterprise workflow with full governance"""
        
        files = self._generate_advanced_workflow(config)
        
        # Add compliance and audit workflow
        compliance_workflow = {
            "name": "Compliance and Audit",
            "on": {
                "schedule": [{"cron": "0 0 * * 0"}],  # Weekly
                "workflow_dispatch": None
            },
            "jobs": {
                "compliance-audit": {
                    "runs-on": "ubuntu-latest",
                    "steps": [
                        {"name": "Checkout", "uses": "actions/checkout@v4"},
                        {"name": "Generate Compliance Report", "run": "fluid audit --compliance --output compliance-report.json"},
                        {"name": "Check Data Lineage", "run": "fluid lineage --validate"},
                        {"name": "Performance Benchmarks", "run": "fluid benchmark --baseline"},
                        {"name": "Upload Compliance Artifacts", "uses": "actions/upload-artifact@v3"}
                    ]
                }
            }
        }
        
        files[".github/workflows/compliance.yml"] = yaml.dump(compliance_workflow, indent=2)
        
        return files


class GitLabCITemplate(BasePipelineTemplate):
    """GitLab CI pipeline template"""
    
    def __init__(self):
        super().__init__()
        self.provider_name = "GitLab CI"
        self.file_extensions = [".yml"]
    
    def generate(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate GitLab CI pipeline"""
        
        commands = self._get_fluid_commands()
        env_vars = self._get_common_environment_vars()
        
        if config.complexity == PipelineComplexity.BASIC:
            pipeline = self._generate_basic_gitlab_pipeline(config, commands, env_vars)
        elif config.complexity == PipelineComplexity.STANDARD:
            pipeline = self._generate_standard_gitlab_pipeline(config, commands, env_vars)
        else:
            pipeline = self._generate_advanced_gitlab_pipeline(config, commands, env_vars)
        
        return {".gitlab-ci.yml": yaml.dump(pipeline, indent=2)}
    
    def _generate_basic_gitlab_pipeline(self, config, commands, env_vars):
        """Generate basic GitLab CI pipeline"""
        
        return {
            "stages": ["validate", "plan", "apply", "test", "publish"],
            "variables": env_vars,
            "image": "python:3.9",
            "before_script": [
                "pip install -r requirements.txt"
            ],
            "validate": {
                "stage": "validate",
                "script": [
                    commands["doctor"],
                    commands["validate"]
                ],
                "rules": [{"if": "$CI_PIPELINE_SOURCE == 'push'"}]
            },
            "plan": {
                "stage": "plan", 
                "script": [commands["plan"]],
                "artifacts": {
                    "paths": ["plan.json"],
                    "expire_in": "1 day"
                }
            },
            "apply": {
                "stage": "apply",
                "script": [commands["apply"]],
                "dependencies": ["plan"],
                "only": ["main"],
                "when": "manual"
            },
            "test": {
                "stage": "test",
                "script": [
                    commands["test"],
                    commands["contract_test"]
                ],
                "artifacts": {
                    "reports": {"junit": "test-results/*.xml"},
                    "paths": ["test-results/"]
                }
            },
            "publish": {
                "stage": "publish",
                "script": [
                    commands["visualize"],
                    commands["publish_opds"]
                ],
                "artifacts": {
                    "paths": [
                        "pipeline-viz.html",
                        "dependency-graph.png", 
                        "opds-catalog.json"
                    ],
                    "expire_in": "30 days"
                },
                "only": ["main"]
            }
        }
    
    def _generate_standard_gitlab_pipeline(self, config, commands, env_vars):
        """Generate standard GitLab CI pipeline with environments"""
        
        pipeline = {
            "stages": ["validate", "test", "plan", "deploy", "publish"],
            "variables": env_vars,
            "image": "python:3.9",
            "before_script": ["pip install -r requirements.txt"]
        }
        
        # Add validation and testing jobs
        pipeline.update({
            "validate": {
                "stage": "validate",
                "script": [commands["doctor"], commands["validate"]]
            },
            "unit-tests": {
                "stage": "test",
                "script": ["fluid test --type unit"],
                "artifacts": {"reports": {"junit": "test-results/unit.xml"}}
            },
            "integration-tests": {
                "stage": "test", 
                "script": ["fluid test --type integration"],
                "artifacts": {"reports": {"junit": "test-results/integration.xml"}}
            },
            "plan": {
                "stage": "plan",
                "script": [commands["plan"]],
                "artifacts": {"paths": ["plan.json"]}
            }
        })
        
        # Add deployment jobs for each environment
        for env in config.environments:
            deploy_job = {
                "stage": "deploy",
                "script": [f"FLUID_ENV={env} {commands['apply']}"],
                "environment": {"name": env},
                "dependencies": ["plan"]
            }
            
            if env == "prod":
                deploy_job["when"] = "manual"
                deploy_job["only"] = ["main"]
            elif env == "staging":
                deploy_job["only"] = ["main", "develop"]
            
            pipeline[f"deploy-{env}"] = deploy_job
        
        # Add publishing job
        pipeline["publish"] = {
            "stage": "publish",
            "script": [
                commands["visualize"],
                commands["publish_opds"]
            ],
            "artifacts": {
                "paths": ["pipeline-viz.html", "dependency-graph.png", "opds-catalog.json"]
            },
            "only": ["main"],
            "dependencies": [f"deploy-{config.environments[-1]}"]  # Depends on final environment
        }
        
        if config.enable_marketplace_publishing:
            pipeline["marketplace-publish"] = {
                "stage": "publish",
                "script": [commands["marketplace_publish"]],
                "only": ["main"],
                "when": "manual",
                "dependencies": ["publish"]
            }
        
        return pipeline
    
    def _generate_advanced_gitlab_pipeline(self, config, commands, env_vars):
        """Generate advanced GitLab CI pipeline with security and compliance"""
        
        pipeline = self._generate_standard_gitlab_pipeline(config, commands, env_vars)
        
        # Add security stage
        pipeline["stages"].insert(-1, "security")
        
        # Add security jobs
        pipeline.update({
            "security-scan": {
                "stage": "security",
                "script": [
                    "fluid validate --security-only",
                    "trivy fs ."
                ],
                "artifacts": {
                    "reports": {"sast": "security-report.json"}
                },
                "allow_failure": True
            },
            "compliance-check": {
                "stage": "security", 
                "script": ["fluid audit --compliance"],
                "artifacts": {"paths": ["compliance-report.json"]},
                "only": ["main"]
            }
        })
        
        return pipeline


class AzureDevOpsTemplate(BasePipelineTemplate):
    """Azure DevOps pipeline template"""
    
    def __init__(self):
        super().__init__()
        self.provider_name = "Azure DevOps"
        self.file_extensions = [".yml"]
    
    def generate(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate Azure DevOps pipeline"""
        
        commands = self._get_fluid_commands()
        
        pipeline = {
            "trigger": {
                "branches": {"include": ["main", "develop"]}
            },
            "pr": {
                "branches": {"include": ["main", "develop"]}
            },
            "pool": {"vmImage": "ubuntu-latest"},
            "variables": self._get_common_environment_vars(),
            "stages": []
        }
        
        # Validation stage
        validate_stage = {
            "stage": "Validate",
            "displayName": "Validate and Test",
            "jobs": [
                {
                    "job": "ValidateJob",
                    "displayName": "FLUID Validation",
                    "steps": [
                        {
                            "task": "UsePythonVersion@0",
                            "inputs": {"versionSpec": "3.9"}
                        },
                        {
                            "script": "pip install -r requirements.txt",
                            "displayName": "Install dependencies"
                        },
                        {
                            "script": commands["doctor"],
                            "displayName": "FLUID Doctor Check"
                        },
                        {
                            "script": commands["validate"],
                            "displayName": "Validate configuration"
                        },
                        {
                            "script": commands["plan"],
                            "displayName": "Generate plan"
                        },
                        {
                            "task": "PublishBuildArtifacts@1",
                            "inputs": {
                                "pathToPublish": "plan.json",
                                "artifactName": "plan"
                            }
                        }
                    ]
                },
                {
                    "job": "TestJob",
                    "displayName": "Run Tests",
                    "steps": [
                        {
                            "task": "UsePythonVersion@0",
                            "inputs": {"versionSpec": "3.9"}
                        },
                        {
                            "script": "pip install -r requirements.txt",
                            "displayName": "Install dependencies"
                        },
                        {
                            "script": commands["test"],
                            "displayName": "Run tests"
                        },
                        {
                            "task": "PublishTestResults@2",
                            "inputs": {
                                "testResultsFiles": "test-results/*.xml",
                                "testRunTitle": "FLUID Tests"
                            }
                        }
                    ]
                }
            ]
        }
        
        pipeline["stages"].append(validate_stage)
        
        # Deployment stages for each environment
        for env in config.environments:
            deploy_stage = {
                "stage": f"Deploy{env.title()}",
                "displayName": f"Deploy to {env.upper()}",
                "dependsOn": "Validate",
                "jobs": [
                    {
                        "deployment": f"Deploy{env.title()}Job",
                        "displayName": f"Deploy to {env}",
                        "environment": env,
                        "strategy": {
                            "runOnce": {
                                "deploy": {
                                    "steps": [
                                        {
                                            "task": "UsePythonVersion@0",
                                            "inputs": {"versionSpec": "3.9"}
                                        },
                                        {
                                            "script": "pip install -r requirements.txt",
                                            "displayName": "Install dependencies"
                                        },
                                        {
                                            "task": "DownloadBuildArtifacts@0",
                                            "inputs": {"artifactName": "plan"}
                                        },
                                        {
                                            "script": f"FLUID_ENV={env} {commands['apply']}",
                                            "displayName": f"Apply to {env}"
                                        },
                                        {
                                            "script": f"FLUID_ENV={env} {commands['contract_test']}",
                                            "displayName": "Run contract tests"
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
            
            if env == "prod":
                deploy_stage["condition"] = "and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))"
            
            pipeline["stages"].append(deploy_stage)
        
        # Publishing stage
        if config.enable_marketplace_publishing:
            publish_stage = {
                "stage": "Publish",
                "displayName": "Publish Artifacts",
                "dependsOn": f"Deploy{config.environments[-1].title()}",
                "condition": "and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))",
                "jobs": [
                    {
                        "job": "PublishJob",
                        "displayName": "Publish to Marketplace",
                        "steps": [
                            {
                                "task": "UsePythonVersion@0",
                                "inputs": {"versionSpec": "3.9"}
                            },
                            {
                                "script": "pip install -r requirements.txt",
                                "displayName": "Install dependencies"
                            },
                            {
                                "script": commands["visualize"],
                                "displayName": "Generate visualizations"
                            },
                            {
                                "script": commands["publish_opds"],
                                "displayName": "Export OPDS catalog"
                            },
                            {
                                "script": commands["marketplace_publish"],
                                "displayName": "Publish to marketplace"
                            },
                            {
                                "task": "PublishBuildArtifacts@1",
                                "inputs": {
                                    "pathToPublish": "opds-catalog.json",
                                    "artifactName": "marketplace-artifacts"
                                }
                            }
                        ]
                    }
                ]
            }
            
            pipeline["stages"].append(publish_stage)
        
        return {"azure-pipelines.yml": yaml.dump(pipeline, indent=2)}


class JenkinsTemplate(BasePipelineTemplate):
    """Jenkins pipeline template"""
    
    def __init__(self):
        super().__init__()
        self.provider_name = "Jenkins"
        self.file_extensions = [".groovy"]
    
    def generate(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate Jenkins pipeline"""
        
        commands = self._get_fluid_commands()
        
        jenkins_pipeline = f'''
pipeline {{
    agent any
    
    environment {{
        FLUID_LOG_LEVEL = 'INFO'
        FLUID_CONFIG_PATH = './fluid_config'
        PYTHONPATH = '.'
    }}
    
    triggers {{
        pollSCM('H/5 * * * *')  // Poll every 5 minutes
    }}
    
    stages {{
        stage('Setup') {{
            steps {{
                sh 'pip install -r requirements.txt'
            }}
        }}
        
        stage('Validate') {{
            parallel {{
                stage('FLUID Doctor') {{
                    steps {{
                        sh '{commands["doctor"]}'
                    }}
                }}
                stage('Configuration Validation') {{
                    steps {{
                        sh '{commands["validate"]}'
                    }}
                }}
            }}
        }}
        
        stage('Plan') {{
            steps {{
                sh '{commands["plan"]}'
                archiveArtifacts artifacts: 'plan.json', fingerprint: true
            }}
        }}
        
        stage('Test') {{
            parallel {{
                stage('Unit Tests') {{
                    steps {{
                        sh 'fluid test --type unit --output test-results-unit.xml'
                    }}
                    post {{
                        always {{
                            junit 'test-results-unit.xml'
                        }}
                    }}
                }}
                stage('Integration Tests') {{
                    steps {{
                        sh 'fluid test --type integration --output test-results-integration.xml'
                    }}
                    post {{
                        always {{
                            junit 'test-results-integration.xml'
                        }}
                    }}
                }}
            }}
        }}
'''
        
        # Add deployment stages
        for env in config.environments:
            approval = ""
            when_condition = ""
            
            if env == "prod":
                approval = '''
                input {
                    message "Deploy to production?"
                    ok "Deploy"
                    parameters {
                        choice(name: 'DEPLOY_ACTION', choices: ['Deploy', 'Skip'], description: 'Choose deployment action')
                    }
                }'''
                when_condition = "when { branch 'main' }"
            elif env == "staging":
                when_condition = "when { anyOf { branch 'main'; branch 'develop' } }"
            
            jenkins_pipeline += f'''
        stage('Deploy to {env.upper()}') {{
            {when_condition}
            steps {{{approval}
                sh 'FLUID_ENV={env} {commands["apply"]}'
                sh 'FLUID_ENV={env} {commands["contract_test"]}'
            }}
            post {{
                success {{
                    echo 'Deployment to {env} successful'
                }}
                failure {{
                    echo 'Deployment to {env} failed'
                }}
            }}
        }}
'''
        
        # Add publishing stage
        jenkins_pipeline += f'''
        stage('Publish') {{
            when {{ branch 'main' }}
            steps {{
                sh '{commands["visualize"]}'
                sh '{commands["publish_opds"]}'
                archiveArtifacts artifacts: 'pipeline-viz.html,dependency-graph.png,opds-catalog.json', fingerprint: true
'''
        
        if config.enable_marketplace_publishing:
            jenkins_pipeline += f'''
                sh '{commands["marketplace_publish"]}'
'''
        
        jenkins_pipeline += '''
            }
        }
    }
    
    post {
        always {
            cleanWs()
        }
        success {
            echo 'Pipeline completed successfully!'
        }
        failure {
            echo 'Pipeline failed!'
        }
    }
}
'''
        
        return {"Jenkinsfile": jenkins_pipeline}


class BitbucketTemplate(BasePipelineTemplate):
    """Bitbucket Pipelines template"""
    
    def __init__(self):
        super().__init__()
        self.provider_name = "Bitbucket Pipelines"
    
    def generate(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate Bitbucket pipeline"""
        
        commands = self._get_fluid_commands()
        
        pipeline = {
            "image": "python:3.9",
            "definitions": {
                "steps": [
                    {
                        "step": {
                            "name": "Validate",
                            "script": [
                                "pip install -r requirements.txt",
                                commands["doctor"],
                                commands["validate"]
                            ]
                        }
                    },
                    {
                        "step": {
                            "name": "Plan",
                            "script": [commands["plan"]],
                            "artifacts": ["plan.json"]
                        }
                    },
                    {
                        "step": {
                            "name": "Test",
                            "script": [
                                commands["test"],
                                commands["contract_test"]
                            ]
                        }
                    }
                ]
            },
            "pipelines": {
                "branches": {
                    "main": [
                        {"step": "Validate"},
                        {"step": "Plan"},
                        {"step": "Test"}
                    ]
                }
            }
        }
        
        # Add deployment steps for each environment
        for env in config.environments:
            deploy_step = {
                "step": {
                    "name": f"Deploy to {env.upper()}",
                    "deployment": env,
                    "script": [
                        f"FLUID_ENV={env} {commands['apply']}",
                        f"FLUID_ENV={env} {commands['contract_test']}"
                    ]
                }
            }
            
            if env == "prod":
                deploy_step["step"]["trigger"] = "manual"
            
            pipeline["pipelines"]["branches"]["main"].append(deploy_step)
        
        # Add publishing step
        publish_step = {
            "step": {
                "name": "Publish",
                "script": [
                    commands["visualize"],
                    commands["publish_opds"]
                ],
                "artifacts": [
                    "pipeline-viz.html",
                    "dependency-graph.png", 
                    "opds-catalog.json"
                ]
            }
        }
        
        if config.enable_marketplace_publishing:
            publish_step["step"]["script"].append(commands["marketplace_publish"])
            publish_step["step"]["trigger"] = "manual"
        
        pipeline["pipelines"]["branches"]["main"].append(publish_step)
        
        return {"bitbucket-pipelines.yml": yaml.dump(pipeline, indent=2)}


class CircleCITemplate(BasePipelineTemplate):
    """CircleCI pipeline template"""
    
    def __init__(self):
        super().__init__()
        self.provider_name = "CircleCI"
    
    def generate(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate CircleCI pipeline"""
        
        commands = self._get_fluid_commands()
        
        pipeline = {
            "version": 2.1,
            "executors": {
                "python-executor": {
                    "docker": [{"image": "python:3.9"}],
                    "working_directory": "~/project"
                }
            },
            "jobs": {
                "validate": {
                    "executor": "python-executor",
                    "steps": [
                        "checkout",
                        {"run": "pip install -r requirements.txt"},
                        {"run": {"name": "FLUID Doctor", "command": commands["doctor"]}},
                        {"run": {"name": "Validate", "command": commands["validate"]}}
                    ]
                },
                "plan": {
                    "executor": "python-executor", 
                    "steps": [
                        "checkout",
                        {"run": "pip install -r requirements.txt"},
                        {"run": {"name": "Generate Plan", "command": commands["plan"]}},
                        {"persist_to_workspace": {"root": ".", "paths": ["plan.json"]}}
                    ]
                },
                "test": {
                    "executor": "python-executor",
                    "steps": [
                        "checkout",
                        {"run": "pip install -r requirements.txt"},
                        {"run": {"name": "Run Tests", "command": commands["test"]}},
                        {"store_test_results": {"path": "test-results"}}
                    ]
                }
            },
            "workflows": {
                "fluid-pipeline": {
                    "jobs": [
                        "validate",
                        "plan",
                        {"test": {"requires": ["validate"]}}
                    ]
                }
            }
        }
        
        # Add deployment jobs
        for env in config.environments:
            job_name = f"deploy-{env}"
            
            deploy_job = {
                "executor": "python-executor",
                "steps": [
                    "checkout",
                    {"attach_workspace": {"at": "."}},
                    {"run": "pip install -r requirements.txt"},
                    {"run": {"name": f"Deploy to {env}", "command": f"FLUID_ENV={env} {commands['apply']}"}},
                    {"run": {"name": "Contract Tests", "command": f"FLUID_ENV={env} {commands['contract_test']}"}}
                ]
            }
            
            pipeline["jobs"][job_name] = deploy_job
            
            # Add to workflow with dependencies
            workflow_job = {job_name: {"requires": ["plan", "test"]}}
            
            if env == "prod":
                workflow_job[job_name]["filters"] = {"branches": {"only": "main"}}
                workflow_job[job_name]["type"] = "approval"
            
            pipeline["workflows"]["fluid-pipeline"]["jobs"].append(workflow_job)
        
        return {".circleci/config.yml": yaml.dump(pipeline, indent=2)}


class TektonTemplate(BasePipelineTemplate):
    """Tekton pipeline template"""
    
    def __init__(self):
        super().__init__()
        self.provider_name = "Tekton"
    
    def generate(self, config: PipelineConfig) -> Dict[str, str]:
        """Generate Tekton pipeline"""
        
        commands = self._get_fluid_commands()
        
        # Tekton pipeline definition
        pipeline = {
            "apiVersion": "tekton.dev/v1beta1",
            "kind": "Pipeline",
            "metadata": {"name": "fluid-dataops-pipeline"},
            "spec": {
                "workspaces": [{"name": "shared-data"}],
                "tasks": [
                    {
                        "name": "validate",
                        "taskRef": {"name": "fluid-validate"},
                        "workspaces": [{"name": "source", "workspace": "shared-data"}]
                    },
                    {
                        "name": "plan",
                        "taskRef": {"name": "fluid-plan"},
                        "workspaces": [{"name": "source", "workspace": "shared-data"}],
                        "runAfter": ["validate"]
                    },
                    {
                        "name": "test",
                        "taskRef": {"name": "fluid-test"},
                        "workspaces": [{"name": "source", "workspace": "shared-data"}],
                        "runAfter": ["validate"]
                    }
                ]
            }
        }
        
        # Add deployment tasks
        for env in config.environments:
            deploy_task = {
                "name": f"deploy-{env}",
                "taskRef": {"name": "fluid-deploy"},
                "params": [{"name": "environment", "value": env}],
                "workspaces": [{"name": "source", "workspace": "shared-data"}],
                "runAfter": ["plan", "test"]
            }
            
            pipeline["spec"]["tasks"].append(deploy_task)
        
        # Task definitions
        task_definitions = []
        
        # Validate task
        validate_task = {
            "apiVersion": "tekton.dev/v1beta1",
            "kind": "Task",
            "metadata": {"name": "fluid-validate"},
            "spec": {
                "workspaces": [{"name": "source"}],
                "steps": [
                    {
                        "name": "validate",
                        "image": "python:3.9",
                        "workingDir": "$(workspaces.source.path)",
                        "script": f'''#!/bin/bash
pip install -r requirements.txt
{commands["doctor"]}
{commands["validate"]}
'''
                    }
                ]
            }
        }
        
        task_definitions.append(validate_task)
        
        files = {
            "tekton/pipeline.yaml": yaml.dump(pipeline, indent=2),
            "tekton/tasks.yaml": yaml.dump_all(task_definitions, indent=2)
        }
        
        return files


# Main pipeline generator function
def generate_pipeline_template(provider: str, complexity: str = "standard", 
                             environments: List[str] = None,
                             enable_marketplace: bool = False) -> Dict[str, str]:
    """
    Generate pipeline template for specified provider
    
    Args:
        provider: CI/CD provider (github_actions, gitlab_ci, etc.)
        complexity: Pipeline complexity (basic, standard, advanced, enterprise)
        environments: List of deployment environments
        enable_marketplace: Enable marketplace publishing
        
    Returns:
        Dictionary of filename -> content for pipeline files
    """
    
    try:
        provider_enum = PipelineProvider(provider)
        complexity_enum = PipelineComplexity(complexity)
    except ValueError as e:
        raise ValueError(f"Invalid parameter: {e}")
    
    config = PipelineConfig(
        provider=provider_enum,
        complexity=complexity_enum,
        environments=environments,
        enable_marketplace_publishing=enable_marketplace
    )
    
    generator = PipelineTemplateGenerator()
    return generator.generate_pipeline(config)


if __name__ == "__main__":
    # Demo the pipeline generator
    cprint("FLUID Dynamic DataOps Pipeline Templates")
    cprint("=" * 50)
    
    generator = PipelineTemplateGenerator()
    
    cprint(f"Available providers: {generator.list_available_providers()}")
    
    # Generate a sample GitHub Actions pipeline
    config = PipelineConfig(
        provider=PipelineProvider.GITHUB_ACTIONS,
        complexity=PipelineComplexity.STANDARD,
        environments=["dev", "staging", "prod"],
        enable_marketplace_publishing=True
    )
    
    files = generator.generate_pipeline(config)
    
    cprint(f"\nGenerated {len(files)} pipeline files:")
    for filename, content in files.items():
        cprint(f"  - {filename} ({len(content)} characters)")
    
    cprint("\nSample file content preview:")
    first_file = list(files.items())[0]
    cprint(f"\n{first_file[0]}:")
    cprint("-" * 40)
    cprint(first_file[1][:500] + "..." if len(first_file[1]) > 500 else first_file[1])
