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

import json
import logging
import argparse
from datetime import datetime
from typing import Dict, Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.transforms import window


class ParseCustomerEvent(beam.DoFn):
    """Parse and validate customer events from Pub/Sub."""
    
    def process(self, element):
        try:
            # Parse JSON message
            if isinstance(element, bytes):
                element = element.decode('utf-8')
            
            event_data = json.loads(element)
            
            # Validate required fields
            required_fields = ['event_id', 'customer_id', 'event_type', 'event_timestamp']
            for field in required_fields:
                if field not in event_data:
                    logging.error(f"Missing required field: {field}")
                    return
            
            # Transform and enrich the event
            processed_event = self._transform_event(event_data)
            
            yield processed_event
            
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON: {e}")
        except Exception as e:
            logging.error(f"Error processing event: {e}")
    
    def _transform_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform and enrich the raw event data."""
        
        # Parse timestamp
        timestamp = event_data.get('event_timestamp')
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                timestamp = datetime.utcnow()
        elif isinstance(timestamp, (int, float)):
            timestamp = datetime.utcfromtimestamp(timestamp / 1000)  # Assume milliseconds
        else:
            timestamp = datetime.utcnow()
        
        # Extract and hash IP address for privacy
        ip_address = event_data.get('ip_address')
        ip_address_hash = None
        if ip_address:
            import hashlib
            ip_address_hash = hashlib.sha256(ip_address.encode()).hexdigest()
        
        # Parse UTM parameters
        utm_params = event_data.get('utm_parameters', {})
        if isinstance(utm_params, str):
            try:
                utm_params = json.loads(utm_params)
            except json.JSONDecodeError:
                utm_params = {}
        
        # Parse event properties
        event_properties = event_data.get('event_properties', {})
        if isinstance(event_properties, str):
            try:
                event_properties = json.loads(event_properties)
            except json.JSONDecodeError:
                event_properties = {}
        
        # Determine device type from user agent
        user_agent = event_data.get('user_agent', '').lower()
        device_type = 'desktop'
        if 'mobile' in user_agent or 'android' in user_agent or 'iphone' in user_agent:
            device_type = 'mobile'
        elif 'tablet' in user_agent or 'ipad' in user_agent:
            device_type = 'tablet'
        
        # Create the processed event
        processed_event = {
            'event_id': event_data['event_id'],
            'customer_id': event_data['customer_id'],
            'session_id': event_data.get('session_id'),
            'event_timestamp': timestamp.isoformat(),
            'event_date': timestamp.date().isoformat(),
            'event_type': event_data['event_type'].upper(),
            'event_properties': json.dumps(event_properties) if event_properties else None,
            'user_agent': event_data.get('user_agent'),
            'ip_address_hash': ip_address_hash,
            'referrer': event_data.get('referrer'),
            'utm_parameters': json.dumps(utm_params) if utm_params else None,
            'device_type': device_type,
            'channel': event_data.get('channel', 'unknown'),
            'revenue_amount': float(event_data.get('revenue_amount', 0)) if event_data.get('revenue_amount') else None,
            
            # Derived fields
            'event_hour': timestamp.hour,
            'event_day_of_week': timestamp.weekday() + 1,  # 1-7 for Monday-Sunday
            'is_weekend': timestamp.weekday() >= 5,
            
            # Processing metadata
            'processed_timestamp': datetime.utcnow().isoformat(),
            'pipeline_version': '1.0.0'
        }
        
        return processed_event


class EnrichCustomerEvent(beam.DoFn):
    """Enrich events with additional customer context."""
    
    def process(self, element):
        # In a real implementation, this would lookup customer data
        # from BigQuery or other sources for enrichment
        
        # Add enrichment fields
        enriched_event = dict(element)
        
        # Example enrichments (in reality, these would come from lookups)
        enriched_event.update({
            'customer_segment': 'unknown',  # Would be looked up
            'customer_tenure_days': None,   # Would be calculated
            'is_high_value_customer': False, # Would be determined from historical data
        })
        
        yield enriched_event


class CustomerEventPipeline:
    """Main pipeline for processing customer events."""
    
    def __init__(self, options: PipelineOptions):
        self.options = options
        
    def run(self):
        """Execute the pipeline."""
        
        # Parse custom arguments
        known_args, pipeline_args = self.parse_arguments()
        
        # BigQuery table schema
        table_schema = {
            'fields': [
                {'name': 'event_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'customer_id', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'session_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'event_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
                {'name': 'event_date', 'type': 'DATE', 'mode': 'REQUIRED'},
                {'name': 'event_type', 'type': 'STRING', 'mode': 'REQUIRED'},
                {'name': 'event_properties', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'user_agent', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'ip_address_hash', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'referrer', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'utm_parameters', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'device_type', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'channel', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'revenue_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'event_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'event_day_of_week', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'is_weekend', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                {'name': 'customer_segment', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'customer_tenure_days', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'is_high_value_customer', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                {'name': 'processed_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
                {'name': 'pipeline_version', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        }
        
        # Create pipeline
        with beam.Pipeline(options=self.options) as pipeline:
            
            # Read from Pub/Sub
            events = (
                pipeline
                | 'Read from Pub/Sub' >> ReadFromPubSub(
                    subscription=known_args.input_subscription
                )
            )
            
            # Process events
            processed_events = (
                events
                | 'Parse Events' >> beam.ParDo(ParseCustomerEvent())
                | 'Enrich Events' >> beam.ParDo(EnrichCustomerEvent())
            )
            
            # Write to BigQuery with windowing for streaming inserts
            (
                processed_events
                | 'Window Events' >> beam.WindowInto(
                    window.FixedWindows(60)  # 1-minute windows
                )
                | 'Write to BigQuery' >> WriteToBigQuery(
                    table=known_args.output_table,
                    schema=table_schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
            )
            
            # Optional: Write to Cloud Storage for backup
            if known_args.backup_bucket:
                (
                    processed_events
                    | 'Convert to JSON' >> beam.Map(json.dumps)
                    | 'Write to GCS' >> beam.io.WriteToText(
                        f"gs://{known_args.backup_bucket}/customer-events",
                        file_name_suffix='.jsonl',
                        num_shards=10
                    )
                )
    
    def parse_arguments(self):
        """Parse pipeline arguments."""
        parser = argparse.ArgumentParser()
        
        parser.add_argument(
            '--input_subscription',
            required=True,
            help='Pub/Sub subscription to read from'
        )
        
        parser.add_argument(
            '--output_table',
            required=True,
            help='BigQuery table to write to (format: project:dataset.table)'
        )
        
        parser.add_argument(
            '--backup_bucket',
            help='GCS bucket for backup storage (optional)'
        )
        
        known_args, pipeline_args = parser.parse_known_args()
        return known_args, pipeline_args


def run_pipeline():
    """Main entry point for the pipeline."""
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Pipeline options
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True
    
    # Create and run pipeline
    pipeline = CustomerEventPipeline(pipeline_options)
    pipeline.run()


if __name__ == '__main__':
    run_pipeline()