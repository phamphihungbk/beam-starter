import argparse
import csv
import itertools

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from utils.service_factory import ServiceFactory


class CleanData(beam.DoFn):
    def __init__(self, bool_cols=[], int_cols=[], float_cols=[]):
        self.bool_cols = bool_cols
        self.int_cols = int_cols
        self.float_cols = float_cols

    def _parse_numeric(self, record, ttype):
        if ttype == 'float':
            func = float
            _cols = self.float_cols
        elif ttype == 'int':
            func = int
            _cols = self.int_cols
        else:
            raise AttributeError('Invalid type {}'.format(ttype))
        for col in _cols:
            if record[col]:
                try:
                    record[col] = func(record[col])
                except ValueError:
                    record[col] = None
            else:
                record[col] = None
        return record

    def process(self, record: dict):
        for k in record:
            if record[k] == '\\N':
                record[k] = None
        for col in self.bool_cols:
            if record[col] == '0':
                record[col] = False
            else:
                record[col] = True
        record = self._parse_numeric(record, 'int')
        record = self._parse_numeric(record, 'float')

        # Return
        yield record


class FilterRatingData(beam.DoFn):
    def process(self, record: dict):
        if record['averageRating'] and record['averageRating'] >= 5.0:
            yield record


class FilterBasicData(beam.DoFn):
    def process(self, record: dict):
        #print(record)
        if record['titleType'] == 'movie' and not record['isAdult'] and record['startYear'] and record['startYear'] >= 1970:
            yield record


class GetAttribute(beam.DoFn):
    def __init__(self, col):
        self.col = col

    def process(self, record: dict):
        yield record[self.col]


def join_ratings(v):
    r = itertools.product(v[1]['movie_keys'], v[1]['rating_keys'])
    return r

def run():
    parser = argparse.ArgumentParser(description='Pipeline group movie by certain criteria')
    parser.add_argument('--input-basics',
                        dest='input_basics',
                        required=True,
                        help='Input movie base file to process.')
    parser.add_argument('--input-ratings',
                        dest='input_ratings',
                        required=True,
                        help='Input rating file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output to write results to.')

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    columns_title_basic = ['tconst', 'titleType', 'primaryTitle',
                           'originalTitle',
                           'isAdult', 'startYear', 'endYear', 'runtimeMinutes',
                           'genres']
    columns_ratings = ['tconst', 'averageRating', 'numVotes']

    with beam.Pipeline(options=pipeline_options) as p:
        basic_data = ( p | 'Read data' >> beam.io.ReadFromText(known_args.input_basics, skip_header_lines=1)
                    | 'Parse CSV' >> beam.ParDo(ParseCsv(columns_title_basic))
                    | 'Clean data' >> beam.ParDo(CleanData(bool_cols=['isAdult'], int_cols=['startYear', 'endYear', 'runtimeMinutes']))
                    | 'Filter data' >> beam.ParDo(FilterBasicData())
                    )

        result = p.run()
        result.wait_until_finish()

if __name__ == '__main__':
    logger = ServiceFactory.get_logger('movie-pipeline-group')
    logger.info('Finish')
    run()
    logger.info('Starting')

