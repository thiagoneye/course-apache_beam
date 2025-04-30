# Imports

import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

# Setup

service_account = "./data/curso-apache-beam-458421-d07f0df3ad2a.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account

# Main

# Constants

pipeline_options = {
    "project": "curso-apache-beam-458421",
    "runner": "DataflowRunner",
    "region": "us-east1",
    "staging_location": "gs://curso-udemy-apache-beam/temp",
    "temp_location": "gs://curso-udemy-apache-beam/temp",
    "template_location": "gs://curso-udemy-apache-beam/template/batch_job_df_flights"
}
input_path = "gs://curso-udemy-apache-beam/input/voos_sample.csv"
output_path = "gs://curso-udemy-apache-beam/output/flight_delays.csv"

# Pipeline Define

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
pl1 = beam.Pipeline(options=pipeline_options)

# Auxiliar Class
class filter_values(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]

# Pipeline Creation

pcolection_sum_delays = (
    pl1
    | "Data Reading (1)" >> beam.io.ReadFromText(input_path, skip_header_lines = 1)
    | "Split by Common (1)" >> beam.Map(lambda record: record.split(','))
    | "Filter by Delay (1)" >> beam.ParDo(filter_values())
    | "Create Pair (1)" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Sum by Key" >> beam.CombinePerKey(sum)
)

pcolection_count_delays = (
    pl1
    | "Data Reading (2)" >> beam.io.ReadFromText(input_path, skip_header_lines = 1)
    | "Split by Common (2)" >> beam.Map(lambda record: record.split(','))
    | "Filter by Delay (2)" >> beam.ParDo(filter_values())
    | "Create Pair (2)" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Count by Key" >> beam.combiners.Count.PerKey()
)

delay_table = (
    {"Count": pcolection_count_delays, "Sum": pcolection_sum_delays}
    | "Group By" >> beam.CoGroupByKey()
    | "Output to GCP" >> beam.io.WriteToText(output_path)
)

# Executation
pl1.run()
