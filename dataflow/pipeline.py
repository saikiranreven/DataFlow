import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

def run():
    options = PipelineOptions(
        streaming=True,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        (p
         | beam.io.ReadFromPubSub(topic='projects/bct-project-465419/topics/stream-topic')
         | beam.Map(lambda x: json.loads(x.decode('utf-8')))
         | beam.io.WriteToText('gs://YOUR_BUCKET/raw/events')
        )

if __name__ == '__main__':
    run()