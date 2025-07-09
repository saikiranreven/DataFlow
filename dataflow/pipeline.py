import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime, timezone
import json
import argparse

class ParseMessageFn(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode('utf-8'))
            record['ingest_time'] = datetime.now(timezone.utc).isoformat()
            yield record
        except Exception as e:
            print(f"Error parsing message: {e}")
            return

def run():
    # Define pipeline options
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default='bct-project-465419')
    parser.add_argument('--region', default='us-central1')
    parser.add_argument('--input_topic', 
                      default='projects/bct-project-465419/topics/stream-topic')
    parser.add_argument('--temp_location',
                      default='gs://bct-project-465419-raw-backup/temp')
    parser.add_argument('--staging_location',
                      default='gs://bct-project-465419-raw-backup/staging')
    parser.add_argument('--output_table',
                      default='bct-project-465419:streaming_dataset.user_events')
    parser.add_argument('--output_path',
                      default='gs://bct-project-465419-raw-backup/raw/events')
    parser.add_argument('--runner', default='DataflowRunner')
    
    known_args, pipeline_args = parser.parse_known_args()
    
    options = PipelineOptions(
        pipeline_args,
        project=known_args.project,
        region=known_args.region,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
        runner=known_args.runner
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | "Window into intervals" >> beam.WindowInto(beam.window.FixedWindows(60))
            | "Parse JSON" >> beam.ParDo(ParseMessageFn())
        )

        messages | "Write to BQ" >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema={
                'fields': [
                    {'name': 'user_id', 'type': 'STRING'},
                    {'name': 'action', 'type': 'STRING'},
                    {'name': 'timestamp', 'type': 'STRING'},
                    {'name': 'ingest_time', 'type': 'STRING'}
                ]
            }
        )

        messages | "Write to GCS" >> beam.io.WriteToText(
            known_args.output_path,
            file_name_suffix='.json'
        )

if __name__ == '__main__':
    run()