'''
Purpose: This python code uses Apache-Beam framework to read files from Google Cloud Storage
'''

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

# Define Parser Arguments for the pipeline
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input', required=True, help='Input file path on Google Cloud Storage (GCS)')
        parser.add_argument('--input_type', required=True, choices=['text', 'avro', 'parquet', 'json'], help='Input file type (text or avro)')
        parser.add_argument('--output', required=False, help='Output file path on GCS (optional)')
        
# Define  run function that takes in customeOptions and pipeline options
def run(argv=None):
    import json
    
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args).view_as(CustomOptions)
    
    # Define the pipeline
    with beam.Pipeline(options=options) as pipeline:
        options = pipeline.options.view_as(CustomOptions)
        
        # Read the file from Google Cloud Storage
        if options.input_type == 'text':
            lines = (pipeline 
                     | 'ReadText' >> beam.io.ReadFromText(options.input) 
                     | 'BatchElements' >> beam.BatchElements(min_batch_size=5, max_batch_size=10)) # Window into batches of 1000)
        elif options.input_type == 'avro':
            lines = (pipeline 
                     | 'ReadAvro' >> beam.io.ReadFromAvro(options.input)  # Use beam.io.avroio.ReadFromAvro for older Beam versions < 2.43
                     | 'BatchElements' >> beam.BatchElements(min_batch_size=5, max_batch_size=10) # Window into batches of 1000
                    )
        elif options.input_type == 'parquet':
            lines = pipeline | 'ReadParquet' >> beam.io.ReadFromParquet(options.input)
        elif options.input_type == 'json':
            lines = (pipeline
             | 'ReadJSON' >> beam.io.ReadFromText(options.input)
             | 'ParseJSON' >> beam.Map(lambda x: json.loads(x))
        )
        else:
            raise ValueError(f"Unsupported input_type: {options.input_type}")
        
        # Print the lines
        lines | 'Print' >> beam.Map(print)
        
if __name__ == '__main__':
    run()
    
'''
python read_files_from_gcs.py \
--runner=DirectRunner \
--input_type=text \
--input=gs://pandarahul-joonix/sample-data/rainfall/rainfall-data1.csv

python read_files_from_gcs.py \
--project=pandarahul-joonix \
--region=asia-south1 \
--temp_location=gs://pandarahul-joonix/temp \
--runner=DataflowRunner \
--input_type=text \
--input=gs://pandarahul-joonix/sample-data/rainfall/rainfall-data1.csv

python read_files_from_gcs.py \
--runner=DirectRunner \
--input_type=avro \
--input=gs://pandarahul-joonix/sample-data/avro/BlueSkyProfiles.avro

python read_files_from_gcs.py \
--project=pandarahul-joonix \
--region=asia-south1 \
--temp_location=gs://pandarahul-joonix/temp \
--runner=DataflowRunner \
--input_type=avro \
--input=gs://pandarahul-joonix/sample-data/avro/BlueSkyProfiles.avro

python read_files_from_gcs.py \
--runner=DirectRunner \
--input_type=parquet \
--input=gs://pandarahul-joonix/sample-data/parquet/example_sample_submission.parquet

python read_files_from_gcs.py \
--project=pandarahul-joonix \
--region=asia-south1 \
--temp_location=gs://pandarahul-joonix/temp \
--runner=DataflowRunner \
--input_type=parquet \
--input=gs://pandarahul-joonix/sample-data/parquet/example_sample_submission.parquet

python read_files_from_gcs.py \
--runner=DirectRunner \
--input_type=json \
--input=gs://pandarahul-joonix/sample-data/json/iris.json
'''