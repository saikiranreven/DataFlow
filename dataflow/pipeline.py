import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timezone
import json

class ParseMessage(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode('utf-8'))
            record['timestamp'] = datetime.utcnow().isoformat()
            record['ingest_time'] = datetime.utcnow().isoformat()
            yield record
        except Exception as e:
            print("Parsing failed:", e)

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
        messages = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
                topic="projects/bct-project-465419/topics/stream-topic")
            | "ParseMessage" >> beam.ParDo(ParseMessage())
            | "WindowIntoFixed1Min" >> beam.WindowInto(
                beam.window.FixedWindows(60))
        )

        messages | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            table="bct-project-465419:streaming_dataset.user_events",
            schema={
                "fields": [
                    {"name": "user_id", "type": "STRING"},
                    {"name": "action", "type": "STRING"},
                    {"name": "timestamp", "type": "TIMESTAMP"},
                    {"name": "ingest_time", "type": "TIMESTAMP"},
                ]
            },
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location="gs://bct-project-465419-raw-backup/temp"
        )

        messages | "WriteToGCS" >> beam.io.WriteToText(
            file_path_prefix="gs://bct-project-465419-raw-backup/raw/events",
            file_name_suffix=".json"
        )

if __name__ == "__main__":
    run()