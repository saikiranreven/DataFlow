# minimal_pipeline.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='your-project-id',
        region='us-central1',
        temp_location='gs://your-project-id-raw-backup/temp',
        staging_location='gs://your-project-id-raw-backup/staging'
    )

    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
             topic='projects/your-project-id/topics/stream-topic')
         | 'WriteToGCS' >> beam.io.WriteToText(
             'gs://your-project-id-raw-backup/raw/events',
             file_name_suffix='.json')
        )

if __name__ == '__main__':
    run()