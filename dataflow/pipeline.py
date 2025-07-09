import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime, timezone
import json
import argparse
import logging

class ParseMessageFn(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode('utf-8'))
            return [{
                'user_id': record.get('user_id', 'unknown'),
                'action': record.get('action', 'unknown'),
                'timestamp': record.get('timestamp', datetime.now(timezone.utc).isoformat()),
                'ingest_time': datetime.now(timezone.utc).isoformat()
            }]
        except Exception as e:
            logging.error(f"Failed to parse message: {element}, Error: {str(e)}")
            return []

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='GCP Project ID')
    parser.add_argument('--region', required=True, help='GCP Region')
    parser.add_argument('--input_topic', required=True, help='PubSub topic to read from')
    parser.add_argument('--temp_location', required=True, help='GCS temp location')
    parser.add_argument('--staging_location', required=True, help='GCS staging location')
    parser.add_argument('--output_table', required=True, help='BigQuery output table')
    parser.add_argument('--output_path', required=True, help='GCS output path')
    parser.add_argument('--runner', default='DataflowRunner', help='Pipeline runner')
    
    known_args, pipeline_args = parser.parse_known_args()
    
    options = PipelineOptions(
        pipeline_args,
        project=known_args.project,
        region=known_args.region,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
        runner=known_args.runner,
        streaming=True
    )

    with beam.Pipeline(options=options) as p:
        # Simple streaming pipeline with no grouping
        (p
         | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
         | "Parse Messages" >> beam.ParDo(ParseMessageFn())
         | "Write to BigQuery" >> beam.io.WriteToBigQuery(
             known_args.output_table,
             schema={
                 'fields': [
                     {'name': 'user_id', 'type': 'STRING'},
                     {'name': 'action', 'type': 'STRING'},
                     {'name': 'timestamp', 'type': 'TIMESTAMP'},
                     {'name': 'ingest_time', 'type': 'TIMESTAMP'}
                 ]
             },
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
         )
         | "Write to GCS" >> beam.io.WriteToText(
             known_args.output_path,
             file_name_suffix='.json'
         ))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()