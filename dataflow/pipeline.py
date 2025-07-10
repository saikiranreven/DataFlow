import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timezone
import json

def run():
    options = PipelineOptions(
        project="bct-project-465419",
        region="us-central1",
        temp_location="gs://bct-project-465419-raw-backup/temp",
        staging_location="gs://bct-project-465419-raw-backup/staging",
        runner="DataflowRunner",
        streaming=True
    )

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(
                topic="projects/bct-project-465419/topics/stream-topic")
            | "Parse JSON" >> beam.Map(lambda x: {
                'user_id': json.loads(x.decode('utf-8')).get('user_id'),
                'action': json.loads(x.decode('utf-8')).get('action'),
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'ingest_time': datetime.now(timezone.utc).isoformat()
            })
        )

        # Fixed: Don't chain the writes, assign them to unused variables
        _ = messages | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            "bct-project-465419:streaming_dataset.user_events",
            schema={
                'fields': [
                    {'name': 'user_id', 'type': 'STRING'},
                    {'name': 'action', 'type': 'STRING'},
                    {'name': 'timestamp', 'type': 'TIMESTAMP'},
                    {'name': 'ingest_time', 'type': 'TIMESTAMP'}
                ]
            }
        )

        _ = messages | "Write to GCS" >> beam.io.WriteToText(
            "gs://bct-project-465419-raw-backup/raw/events",
            file_name_suffix='.json'
        )

if __name__ == '__main__':
    run()