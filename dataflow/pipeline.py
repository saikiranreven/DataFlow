import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
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
            print(f"Error parsing: {e}")

def run():
    options = PipelineOptions(
        project="bct-project-465419",
        region="us-central1",
        runner="DataflowRunner",
        streaming=True,
        temp_location="gs://bct-project-465419-raw-backup/temp",
        staging_location="gs://bct-project-465419-raw-backup/staging",
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        events = (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(
                topic="projects/bct-project-465419/topics/stream-topic")
            | "Parse JSON" >> beam.ParDo(ParseAndAnnotate())
            | "Apply Fixed Window" >> beam.WindowInto(
                beam.window.FixedWindows(60))
        )

        events | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table="bct-project-465419:streaming_dataset.user_events",
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
            custom_gcs_temp_location="gs://bct-project-465419-raw-backup/temp"
        )

        events | "Write to GCS" >> beam.io.WriteToText(
            file_path_prefix="gs://bct-project-465419-raw-backup/raw/events",
            file_name_suffix=".json"
        )

if __name__ == "__main__":
    run()