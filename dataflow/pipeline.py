import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timezone
import json

class ParseMessage(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode('utf-8'))
        yield {
            'user_id': record.get('user_id'),
            'action': record.get('action'),
            'timestamp': datetime.utcnow().isoformat(),
            'ingest_time': datetime.utcnow().isoformat()
        }

def run():
    options = PipelineOptions(
        project="bct-project-465419",
        region="us-central1",
        runner="DataflowRunner",
        temp_location="gs://bct-project-465419-raw-backup/temp",
        staging_location="gs://bct-project-465419-raw-backup/staging",
        streaming=True,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        parsed_messages = (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(
                topic="projects/bct-project-465419/topics/stream-topic")
            | "Parse JSON" >> beam.ParDo(ParseMessage())
            | "Apply Fixed 1-min Window" >> beam.WindowInto(
                beam.window.FixedWindows(60))
        )

        parsed_messages | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            "bct-project-465419:streaming_dataset.user_events",
            schema={
                'fields': [
                    {'name': 'user_id', 'type': 'STRING'},
                    {'name': 'action', 'type': 'STRING'},
                    {'name': 'timestamp', 'type': 'TIMESTAMP'},
                    {'name': 'ingest_time', 'type': 'TIMESTAMP'}
                ]
            },
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location="gs://bct-project-465419-raw-backup/temp"
        )

        parsed_messages | "Write to GCS" >> beam.io.WriteToText(
            "gs://bct-project-465419-raw-backup/raw/events",
            file_name_suffix=".json"
        )

if __name__ == '__main__':
    run()