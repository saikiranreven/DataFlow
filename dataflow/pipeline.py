import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode
from datetime import datetime, timezone
import argparse
import json
import logging

class ParseMessageFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        try:
            record = json.loads(element.decode("utf-8"))
            record["ingest_time"] = datetime.now(timezone.utc).isoformat()
            record["window_start"] = window.start.to_utc_datetime().isoformat()
            yield record
        except Exception as e:
            logging.error(f"Error parsing message: {e}")

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_table', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args)
    google_options = options.view_as(GoogleCloudOptions)
    google_options.project = args.project
    google_options.region = args.region
    google_options.temp_location = args.temp_location
    google_options.staging_location = args.staging_location
    
    # Set streaming options
    streaming_options = options.view_as(StandardOptions)
    streaming_options.streaming = True

    with beam.Pipeline(options=options) as p:
        # Read and window first
        windowed_messages = (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=args.input_topic)
            | "Window into fixed intervals" >> beam.WindowInto(
                FixedWindows(60),  # 60-second windows
                trigger=AfterWatermark(
                    early=AfterProcessingTime(10),  # Early firing every 10 sec
                    late=AfterProcessingTime(20)),  # Late firing every 20 sec
                accumulation_mode=AccumulationMode.DISCARDING)
        )

        # Parse after windowing
        parsed_messages = (
            windowed_messages
            | "Parse JSON" >> beam.ParDo(ParseMessageFn())
        )

        # Grouping operation (only if needed)
        grouped_data = (
            parsed_messages
            | "Add key" >> beam.Map(lambda x: (x["user_id"], x))
            | "Group by user" >> beam.GroupByKey()
            | "Process groups" >> beam.Map(lambda x: {
                "user_id": x[0],
                "count": len(x[1]),
                "actions": [r["action"] for r in x[1]],
                "window_start": x[1][0]["window_start"]
            })
        )

        # Write to BigQuery
        grouped_data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            args.output_table,
            schema={
                'fields': [
                    {'name': 'user_id', 'type': 'STRING'},
                    {'name': 'count', 'type': 'INTEGER'},
                    {'name': 'actions', 'type': 'STRING', 'mode': 'REPEATED'},
                    {'name': 'window_start', 'type': 'STRING'}
                ]
            },
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        # Write raw messages to GCS (without grouping)
        parsed_messages | "Write to GCS" >> beam.io.WriteToText(
            file_path_prefix=args.output_path,
            file_name_suffix=".json"
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()