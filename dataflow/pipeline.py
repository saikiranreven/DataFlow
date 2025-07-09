import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
import argparse
from datetime import datetime
from apache_beam import window

class ParseMessageFn(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            record['ingest_time'] = datetime.utcnow().isoformat()
            yield record
        except Exception as e:
            print(f"Parsing error: {e}")
            return

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--runner', default='DataflowRunner')

    args, beam_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(
        beam_args,
        project=args.project,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        runner=args.runner,
        streaming=True,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(
                topic="projects/bct-project-465419/topics/stream-topic")
            | "Window into fixed intervals" >> beam.WindowInto(
                window.FixedWindows(60))  # 60-second windows
            | "Parse JSON" >> beam.ParDo(ParseMessageFn())
        )

        # Write to BigQuery
        messages | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            args.output_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=args.temp_location,
            schema={
                "fields": [
                    {"name": "user_id", "type": "STRING"},
                    {"name": "action", "type": "STRING"},
                    {"name": "timestamp", "type": "STRING"},
                    {"name": "ingest_time", "type": "STRING"}
                ]
            }
        )

        # Archive raw messages
        messages | "Write to GCS" >> beam.io.WriteToText(
            file_path_prefix=args.output_path,
            file_name_suffix=".json",
            num_shards=1
        )

if __name__ == '__main__':
    run()