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

from setuptools import setup, find_packages

setup(
    name='customer-360-dataflow',
    version='1.0.0',
    description='Real-time customer events processing pipeline for Customer 360 analytics',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]==2.50.0',
        'google-cloud-bigquery==3.12.0',
        'google-cloud-pubsub==2.18.4',
        'google-cloud-storage==2.10.0',
    ],
    python_requires='>=3.8',
)