#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A streaming word-counting workflow.

Important: streaming pipeline support in Python Dataflow is in development
and is not yet available for use.
"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
from datetime import datetime

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window

TABLE_SCHEMA = (
    'username:STRING, message:STRING, date:DATE ')


def find_words(element):
  import re
  return re.findall(r'[A-Za-z\']+', element)


class FormatDoFn(beam.DoFn):
  def process(self, element, window=beam.DoFn.WindowParam):
    ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
    #window_start = window.start.to_utc_datetime().strftime(ts_format)
    timestamp = window.end.to_utc_datetime().strftime(ts_format)
    return [{
        'username': element[1],
        'message': element[0],
        #'window_start': window_start,
        'date': timestamp
    }]


def run(argv=None):
  """Build and run the pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_topic',
      required=True,
      help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
  parser.add_argument(
      '--output_table',
      required=True,
      help=(
          'Output BigQuery table for results specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:

    # Read the text from PubSub messages.
    lines = p | beam.io.ReadFromPubSub(known_args.input_topic, with_attributes=True)

    # Get the number of appearances of a word.
    def add_user(message_user):
      (message, user) = message_user
      return (message, 'João')

    def parse(payload):
        now = datetime.now()
        timestamp_format = now.strftime('%Y-%m-%d %H:%M:%S.%f UTC')
        message = payload.data.decode('utf-8')
        timestamp = timestamp_format
        username = 'João'

        return {
            'username': username,
            'message': message,
            'date': timestamp
        }

    transformed = (
        lines
        | 'parse' >> beam.Map(parse))


    # Write to BigQuery.
    # pylint: disable=expression-not-assigned
    transformed | 'Write' >> beam.io.WriteToBigQuery(
        known_args.output_table,
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()