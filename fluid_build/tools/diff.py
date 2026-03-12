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

import json, difflib

def plan_diff(plan_a_path: str, plan_b_path: str) -> str:
    a = json.dumps(json.load(open(plan_a_path)), indent=2).splitlines()
    b = json.dumps(json.load(open(plan_b_path)), indent=2).splitlines()
    return "\n".join(difflib.unified_diff(a, b, fromfile=plan_a_path, tofile=plan_b_path, lineterm=""))
