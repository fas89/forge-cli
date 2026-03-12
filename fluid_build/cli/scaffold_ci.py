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
import argparse, logging, os
from ._logging import info
from ._io import atomic_write
from ._common import CLIError

COMMAND = "scaffold-ci"

def register(subparsers: argparse._SubParsersAction):
    p = subparsers.add_parser(COMMAND, help="Generate CI pipeline (GitLab/GitHub)")
    p.add_argument("contract", help="contract.fluid.yaml")
    p.add_argument("--system", choices=["gitlab","github"], default="gitlab", help="CI system")
    p.add_argument("--out", default=".gitlab-ci.yml", help="Output path")
    p.set_defaults(cmd=COMMAND, func=run)

GITLAB = """stages:
  - validate
  - plan
  - test
  - apply
validate:
  stage: validate
  script:
    - python -m fluid_build.cli validate $CONTRACT
plan:
  stage: plan
  script:
    - python -m fluid_build.cli --provider $PROVIDER plan $CONTRACT --out runtime/plan.json
  artifacts:
    paths: [runtime/plan.json]
tests:
  stage: test
  script:
    - python -m fluid_build.cli contract-tests $CONTRACT
apply:
  stage: apply
  when: manual
  script:
    - python -m fluid_build.cli --provider $PROVIDER apply runtime/plan.json --yes
"""

GITHUB = """name: FLUID
on: [push]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: python -m fluid_build.cli validate ${{ env.CONTRACT }}
  plan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: python -m fluid_build.cli --provider ${{ env.PROVIDER }} plan ${{ env.CONTRACT }} --out runtime/plan.json
  apply:
    needs: [plan]
    runs-on: ubuntu-latest
    if: github.event_name == 'workflow_dispatch'
    steps:
      - uses: actions/checkout@v4
      - run: python -m fluid_build.cli --provider ${{ env.PROVIDER }} apply runtime/plan.json --yes
"""

def run(args, logger: logging.Logger) -> int:
    try:
        content = GITLAB if args.system == "gitlab" else GITHUB
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        atomic_write(args.out, content)
        info(logger, "scaffold_ci_written", out=args.out, system=args.system)
        return 0
    except Exception as e:
        raise CLIError(1, "scaffold_ci_failed", {"error": str(e)})
