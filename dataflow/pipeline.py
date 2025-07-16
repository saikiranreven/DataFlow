import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import json

def run():
    options = PipelineOptions(
        streaming=True,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        # Simple streaming pipeline without GroupByKey
        (p
         | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
             topic='projects/YOUR_PROJECT/topics/stream-topic')
         | 'WindowInto' >> beam.WindowInto(FixedWindows(60))  # Critical fix
         | 'ParseJSON' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
         | 'WriteToGCS' >> beam.io.WriteToText(
             'gs://YOUR_BUCKET/raw/events',
             file_name_suffix='.json')
        )

if __name__ == '__main__':
    run()