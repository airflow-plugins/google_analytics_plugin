from google_analytics_plugin.hooks.google_analytics_hook import GoogleAnalyticsHook

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator

import hashlib
import json
import os
from datetime import datetime

class GoogleAnalyticsReportingToS3Operator(BaseOperator):
    template_fields = ('s3_key', 'since', 'until')

    def __init__(self,
                 google_analytics_conn_id,
                 view_id,
                 since,
                 until,
                 sampling_level,
                 dimensions,
                 metrics,
                 page_size,
                 include_empty_rows,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.google_analytics_conn_id = google_analytics_conn_id
        self.view_id = view_id
        self.since = since
        self.until = until
        self.sampling_level = sampling_level
        self.dimensions = dimensions
        self.metrics = metrics
        self.page_size = page_size
        self.include_empty_rows = include_empty_rows
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        self.metricMap = {
            'METRIC_TYPE_UNSPECIFIED': 'varchar(255)',
            'CURRENCY': 'decimal(20,5)',
            'INTEGER': 'int(11)',
            'FLOAT': 'decimal(20,5)',
            'PERCENT': 'decimal(20,5)',
            'TIME': 'time'
        }

    def execute(self, context):
        ga_conn = GoogleAnalyticsHook(self.google_analytics_conn_id)
        s3_conn = S3Hook(self.s3_conn_id)

        # This has to be here because template_fields are not yet parsed in the __init__ function
        since_formatted = datetime.strptime(self.since, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        until_formatted = datetime.strptime(self.until, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

        report = ga_conn.get_analytics_report(self.view_id, since_formatted, until_formatted, self.sampling_level, self.dimensions, self.metrics, self.page_size, self.include_empty_rows)

        columnHeader = report.get('columnHeader', {})
        # Right now all dimensions are hardcoded to varchar(255), will need a map if any non-varchar dimensions are used in the future
        # Unfortunately the API does not send back types for Dimensions like it does for Metrics (yet..)
        dimensionHeaders = [
            { 'name': header.replace('ga:', ''), 'type': 'varchar(255)' }
            for header
            in columnHeader.get('dimensions', [])
        ]
        metricHeaders = [
            { 'name': entry.get('name').replace('ga:', ''), 'type': self.metricMap.get(entry.get('type'), 'varchar(255)') }
            for entry
            in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        ]

        file_name = '/tmp/{key}.jsonl'.format(key=self.s3_key)
        with open(file_name, 'w') as ga_file:
            rows = report.get('data', {}).get('rows', [])

            for row_counter, row in enumerate(rows):
                root_data_obj = {}
                dimensions = row.get('dimensions', [])
                metrics = row.get('metrics', [])

                for index, dimension in enumerate(dimensions):
                    header = dimensionHeaders[index].get('name').lower()
                    root_data_obj[header] = dimension

                for metric in metrics:
                    data = {}
                    data.update(root_data_obj)

                    for index, value in enumerate(metric.get('values', [])):
                        header = metricHeaders[index].get('name').lower()
                        data[header] = value

                    data['viewid'] = self.view_id
                    data['timestamp'] = self.since

                    ga_file.write(json.dumps(data) + ('' if row_counter == len(rows) else '\n'))

        s3_conn.load_file(file_name, self.s3_key, self.s3_bucket, True)
        os.remove(file_name)
