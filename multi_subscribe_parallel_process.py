'''
Purpose: This Dataflow job subscribes to same Pub/Sub topic based on Metadata and processes the messages in parallel
'''

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

# Define Parser Arguments for the pipeline
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input', required=False, help='Input file path on Google Cloud Storage (GCS)')
        parser.add_argument('--input_type', required=False, choices=['text', 'avro', 'parquet', 'json'], help='Input file type (text or avro)')
        parser.add_argument('--output', required=False, help='Output file path on GCS (optional)')
        
        
# Define  run function that takes in customeOptions and pipeline options
def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args).view_as(CustomOptions)
    
    # Define the pipeline
    with beam.Pipeline(options=options) as pipeline:
        
        pipeline1 = (pipeline 
                     | 'Systematic-ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription='projects/pandarahul-joonix/subscriptions/sftp-sub1') 
                     | 'Systematic-ProcessAndAck' >> beam.Map(print)
                )
        
        pipeline2 = (pipeline 
                     | 'Async-ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription='projects/pandarahul-joonix/subscriptions/sftp-sub2') 
                     | 'Async-ProcessAndAck' >> beam.Map(print)
                )
        

if __name__ == '__main__':
    run()
    
    
'''
python multi_subscribe_parallel_process.py \
--runner DataflowRunner \
--project=pandarahul-joonix \
--region=asia-south1 \
--temp_location=gs://pandarahul-joonix/temp \
--streaming
'''