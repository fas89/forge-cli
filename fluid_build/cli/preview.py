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

import argparse
import logging

from ._logging import info
from .plan import run as run_plan
from .viz_plan import render_plan_html

COMMAND = "preview"


def register(subparsers: argparse._SubParsersAction):
    p = subparsers.add_parser(COMMAND, help="Validate → Plan → Visualize (no apply)")
    p.add_argument("contract", help="contract.fluid.yaml")
    p.add_argument("--env", help="overlay env")
    p.add_argument("--out", default="runtime/plan.json", help="plan path")
    p.add_argument("--html", default="runtime/plan.html", help="HTML report")
    p.set_defaults(cmd=COMMAND, func=run)


def run(args, logger: logging.Logger) -> int:
    run_plan(args, logger)  # writes plan
    try:
        render_plan_html(args.out, args.html, logger)
        info(logger, "preview_ok", html=args.html)
    except Exception:
        info(logger, "preview_partial", note="plan visualizer not available")
    return 0
