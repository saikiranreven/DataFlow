import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime
import json

class ParseAndAnnotate(beam.DoFn):
    def process(self, element):
        try:
            data = json.loads(element.decode('utf-8'))
            data['timestamp'] = datetime.utcnow().isoformat()
            data['ingest_time'] = datetime.utcnow().isoformat()
            yield data
        except Exception as e:
            print(f"Parse error: {e}")

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    # Pull values from pipeline arguments
    args = options.view_as(PipelineOptions)
    input_topic = args.get_all_options().get("input_topic")
    output_table = args.get_all_options().get("output_table")
    gcs_path_prefix = args.get_all_options().get("output_path")
    gcs_temp_location = args.get_all_options().get("temp_location")

    with beam.Pipeline(options=options) as p:
        events = (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=input_topic)
            | "Parse and Annotate" >> beam.ParDo(ParseAndAnnotate())
            | "Apply Fixed Window" >> beam.WindowInto(beam.window.FixedWindows(60))
        )

        events | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            output_table,
            schema={
                "fields": [
                    {"name": "user_id", "type": "STRING"},
                    {"name": "action", "type": "STRING"},
                    {"name": "timestamp", "type": "TIMESTAMP"},
                    {"name": "ingest_time", "type": "TIMESTAMP"},
                ]
            },
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=gcs_temp_location
        )

        events | "Write to GCS" >> beam.io.WriteToText(
            file_path_prefix=gcs_path_prefix,
            file_name_suffix=".json"
        )

if __name__ == "__main__":
    run()