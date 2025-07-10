import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime, timezone
import json

class ParseMessageFn(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode('utf-8'))
            yield {
                'user_id': record.get('user_id', 'unknown'),
                'action': record.get('action', 'unknown'),
                'timestamp': record.get('timestamp', datetime.now(timezone.utc).isoformat()),
                'ingest_time': datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            print(f"Failed to parse message: {str(e)}")
            yield beam.pvalue.TaggedOutput('parse_errors', element)

def run():
    options = PipelineOptions(
        project="bct-project-465419",
        region="us-central1",
        temp_location="gs://bct-project-465419-raw-backup/temp",
        staging_location="gs://bct-project-465419-raw-backup/staging",
        runner="DataflowRunner"
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Main pipeline with proper windowing
        parsed_messages = (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(
                topic="projects/bct-project-465419/topics/stream-topic")
            | "Window into 1-min intervals" >> beam.WindowInto(
                beam.window.FixedWindows(60))
            | "Parse JSON" >> beam.ParDo(ParseMessageFn()).with_outputs()
        )

        # Write to BigQuery (no grouping)
        _ = (
            parsed_messages[None]  # Main output
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
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
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        # Write to GCS (no grouping)
        _ = (
            parsed_messages[None]
            | "Write to GCS" >> beam.io.WriteToText(
                "gs://bct-project-465419-raw-backup/raw/events",
                file_name_suffix='.json'
            )
        )

        # Optionally handle errors
        _ = (
            parsed_messages['parse_errors']
            | "Write Errors" >> beam.io.WriteToText(
                "gs://bct-project-465419-raw-backup/errors/errors",
                file_name_suffix='.json'
            )
        )

if __name__ == '__main__':
    run()