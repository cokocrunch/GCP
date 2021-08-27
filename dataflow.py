from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1
from google.cloud import bigquery
import apache_beam as beam
import logging
import argparse
import sys
import re


PROJECT="gcp-dmo-mvp-vendor-portal"
schema = 'deviceid:STRING, dt:DATETIME, temp:FLOAT, hum:FLOAT, pres:FLOAT, alt:FLOAT, gas:INTEGER'
TOPIC = "projects/gcp-dmo-mvp-vendor-portal/topics/enviro-event"
BUCKET = "gs://enviro-iot-bq"



class Split(beam.DoFn):

    def process(self, element):
        from datetime import datetime
        element = element.split(",")
        #d = datetime.strptime(element[0], "%d/%b/%Y:%H:%M:%S")
        date_string = datetime.fromtimestamp(float(element[0])).strftime(
                "%Y-%m-%d %H:%M:%S.%f")
        
        return [{ 
            'deviceid': element[6],
            'dt': date_string,
            'temp': element[1],
            'hum': element[2],
            'pres': element[3],
            'alt': element[4],
            'gas': element[5]
    
        }]


def main(argv=None):

   parser = argparse.ArgumentParser()
   parser.add_argument("--input_topic")
   parser.add_argument("--output")
   known_args = parser.parse_known_args(argv)


   p = beam.Pipeline(options=PipelineOptions())

   (p
      | 'ReadData' >> beam.io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)
      | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
      | 'ParseCSV' >> beam.ParDo(Split())
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:enviro_iot.sensordata'.format(PROJECT), schema=schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   )
   result = p.run()
   result.wait_until_finish()

if __name__ == '__main__':
  logger = logging.getLogger().setLevel(logging.INFO)
  main()