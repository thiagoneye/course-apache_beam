# Imports

import apache_beam as beam
import os

# Setup

service_account = "./data/curso-apache-beam-458421-d07f0df3ad2a.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account

# Main

# Auxiliar Class
class filter_values(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]

# Pipeline Define
pl1 = beam.Pipeline()

pcolection_sum_delays = (
    pl1
    | "Data Reading (1)" >> beam.io.ReadFromText("./data/voos_sample.csv", skip_header_lines = 1)
    | "Split by Common (1)" >> beam.Map(lambda record: record.split(','))
    | "Filter by Delay (1)" >> beam.ParDo(filter_values())
    | "Create Pair (1)" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Sum by Key" >> beam.CombinePerKey(sum)
)

pcolection_count_delays = (
    pl1
    | "Data Reading (2)" >> beam.io.ReadFromText("./data/voos_sample.csv", skip_header_lines = 1)
    | "Split by Common (2)" >> beam.Map(lambda record: record.split(','))
    | "Filter by Delay (2)" >> beam.ParDo(filter_values())
    | "Create Pair (2)" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Count by Key" >> beam.combiners.Count.PerKey()
)

delay_table = (
    {"Count": pcolection_count_delays, "Sum": pcolection_sum_delays}
    | "Group By" >> beam.CoGroupByKey()
    | "Output to GCP" >> beam.io.WriteToText(r"gs://curso-udemy-apache-beam/flight_delays.csv")
)

# Executation
pl1.run()
