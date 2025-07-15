import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime
from apache_beam.transforms import window
from datetime import datetime, timezone
import argparse
import json
import logging
import os

class ParseMessageFn(beam.DoFn):
    """Parse JSON messages and add processing metadata"""
    def process(self, element, window=beam.DoFn.WindowParam):
        try:
            record = json.loads(element.decode('utf-8'))
            record['processing_time'] = datetime.now(timezone.utc).isoformat()
            record['window_start'] = window.start.to_utc_datetime().isoformat()
            yield record
        except Exception as e:
            logging.error(f"Failed to parse message: {str(e)}")
            raise

def format_grouped_data(group):
    """Format grouped data for BigQuery output"""
    user_id, actions = group
    return {
        'user_id': user_id,
        'action_count': len(actions),
        'actions': list(set(a['action'] for a in actions)),  # Distinct actions
        'window_start': actions[0]['window_start'],
        'processing_time': datetime.now(timezone.utc).isoformat()
    }

def run(argv=None):
    """Main pipeline definition"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True,
                      help='Input Pub/Sub topic in projects/<project>/topics/<topic> format')
    parser.add_argument('--output_table', required=True,
                      help='Output BigQuery table in project:dataset.table format')
    parser.add_argument('--output_path', required=True,
                      help='Output GCS path for raw messages (gs://bucket/path)')
    parser.add_argument('--window_duration_sec', type=int, default=60,
                      help='Window duration in seconds')
    parser.add_argument('--runner', default='DataflowRunner',
                      help='Pipeline runner (DataflowRunner or DirectRunner)')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set pipeline options
    options = PipelineOptions(
        pipeline_args,
        runner=known_args.runner,
        streaming=True,
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        # 1. Read from Pub/Sub and immediately apply windowing
        raw_messages = (
            p 
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | 'ApplyWindowing' >> beam.WindowInto(
                FixedWindows(known_args.window_duration_sec),
                trigger=AfterWatermark(
                    early=AfterProcessingTime(10),  # Early results every 10s
                    late=AfterProcessingTime(20)),  # Late data allowance
                accumulation_mode=window.AccumulationMode.DISCARDING)
        )

        # 2. Parse JSON messages
        parsed_messages = (
            raw_messages
            | 'ParseMessages' >> beam.ParDo(ParseMessageFn())
        )

        # 3. Group by user_id (now safe because of windowing)
        grouped_data = (
            parsed_messages
            | 'ExtractUserKey' >> beam.Map(lambda x: (x['user_id'], x))
            | 'GroupByUser' >> beam.GroupByKey()
            | 'FormatGroupedData' >> beam.Map(format_grouped_data)
        )

        # 4. Write to BigQuery
        _ = (
            grouped_data
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema='''
                    user_id:STRING,
                    action_count:INTEGER,
                    actions:ARRAY<STRING>,
                    window_start:STRING,
                    processing_time:STRING
                ''',
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

        # 5. Write raw messages to GCS
        _ = (
            parsed_messages
            | 'FormatForGCS' >> beam.Map(json.dumps)
            | 'WriteToGCS' >> beam.io.WriteToText(
                known_args.output_path,
                file_name_suffix='.json')
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()