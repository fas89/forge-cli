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

"""Transports for the consumer MCP output-port server.

Phase-1 ships ``stdio``. Phase-3 lands ``streamable_http`` (MCP spec
2025-03-26) using the same dispatcher attached to a different
transport. The split here keeps the dispatcher transport-agnostic so
the eventual HTTP transport doesn't have to fork the protocol code.
"""
