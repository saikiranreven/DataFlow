# dataflow/pipeline.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from datetime import datetime, timezone
import argparse
import json

class ParseMessageFn(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            record['ingest_time'] = datetime.now(timezone.utc).isoformat()
            yield record
        except Exception as e:
            print(f"Error parsing message: {e}")

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    args, beam_args = parser.parse_known_args()

    print(f"✔️ Using Pub/Sub topic: {args.input_topic}")

    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(PipelineOptions).project = args.project
    options.view_as(PipelineOptions).region = args.region
    options.view_as(PipelineOptions).temp_location = args.temp_location
    options.view_as(PipelineOptions).staging_location = args.staging_location

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=args.input_topic)
            | "Parse JSON" >> beam.ParDo(ParseMessageFn())
        )

        # BigQuery sink
        messages | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            args.output_table,
            schema={
                'fields': [
                    {'name': 'user_id', 'type': 'STRING'},
                    {'name': 'action', 'type': 'STRING'},
                    {'name': 'timestamp', 'type': 'STRING'},
                    {'name': 'ingest_time', 'type': 'STRING'}
                ]
            },
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location=args.temp_location
        )

        # Cloud Storage archive
        messages | "Write to GCS" >> beam.io.WriteToText(
            file_path_prefix=args.output_path,
            file_name_suffix=".json",
            num_shards=1
        )

if __name__ == '__main__':
    run()