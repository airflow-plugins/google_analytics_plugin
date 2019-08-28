from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
import pandas as pd

from google_analytics_plugin.hooks.google_analytics_hook import GoogleAnalyticsHook


class GoogleAnalyticsReportingToPostgresOperator(BaseOperator):
    """
    Google Analytics Reporting To S3 Operator

    :param google_analytics_conn_id:    The Google Analytics connection id.
    :type google_analytics_conn_id:     string
    :param view_id:                     The view id for associated report.
    :type view_id:                      string/array
    :param since:                       The date up from which to pull GA data.
                                        This can either be a string in the format
                                        of '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d'
                                        but in either case it will be
                                        passed to GA as '%Y-%m-%d'.
    :type since:                        string
    :param until:                       The date up to which to pull GA data.
                                        This can either be a string in the format
                                        of '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d'
                                        but in either case it will be
                                        passed to GA as '%Y-%m-%d'.
    :type until:                        string
    
    
    :type postgres_conn_id:             string
    :param google_analytics_conn_id     The Postgres connection id
    
    :type destination_table:            string
    :param destination_table:           Table to be created in the database
    
    :type destination_table_dtypes:     dict
    :param destination_table_dtypes     Dictionary containing column/sqlalchemy type mapping
    
    :type if_exists:                    string
    :param if_exists                    What to do if the table exists. Options: fail,replace,append.
                                        See pandas documetation for to_sql for more

    :type destination_schema            string
    :param destination_schema           Database schema where to create the table
    """

    template_fields = ('since',
                       'until',
                       'destination_table')

    def __init__(self,
                 google_analytics_conn_id,
                 view_id,
                 since,
                 until,
                 dimensions,
                 metrics,
                 postgres_conn_id,
                 destination_table,
                 destination_schema=None,
                 if_exists='fail',
                 destination_table_dtypes=None,
                 page_size=1000,
                 include_empty_rows=True,
                 sampling_level=None,
                 dimension_filter_clauses=None,
                 key_file=None,
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
        self.postgres_conn_id = postgres_conn_id
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.destination_table_dtypes = destination_table_dtypes
        self.if_exists = if_exists
        self.dimension_filter_clauses = dimension_filter_clauses
        self.key_file=key_file

        self.metricMap = {
            'METRIC_TYPE_UNSPECIFIED': 'varchar(255)',
            'CURRENCY': 'decimal(20,5)',
            'INTEGER': 'int(11)',
            'FLOAT': 'decimal(20,5)',
            'PERCENT': 'decimal(20,5)',
            'TIME': 'time'
        }

        if self.page_size > 10000:
            raise Exception('Please specify a page size equal to or lower than 10000.')

        if not isinstance(self.include_empty_rows, bool):
            raise Exception('Please specificy "include_empty_rows" as a boolean.')

    def execute(self, context):
        ga_conn = GoogleAnalyticsHook(self.google_analytics_conn_id, key_file=self.key_file)
        try:
            since_formatted = datetime.strptime(self.since, '%Y-%m-%d %H:%M:%S').strftime(
                '%Y-%m-%d')
        except:
            since_formatted = str(self.since)
        try:
            until_formatted = datetime.strptime(self.until, '%Y-%m-%d %H:%M:%S').strftime(
                '%Y-%m-%d')
        except:
            until_formatted = str(self.until)
        report = ga_conn.get_analytics_report(self.view_id,
                                              since_formatted,
                                              until_formatted,
                                              self.sampling_level,
                                              self.dimensions,
                                              self.metrics,
                                              self.page_size,
                                              self.include_empty_rows,
                                              dimension_filter_clauses=self.dimension_filter_clauses
                                              )

        columnHeader = report.get('columnHeader', {})
        # Right now all dimensions are hardcoded to varchar(255), will need a map if any non-varchar dimensions are used in the future
        # Unfortunately the API does not send back types for Dimensions like it does for Metrics (yet..)
        dimensionHeaders = [
            {'name': header.replace('ga:', ''), 'type': 'varchar(255)'}
            for header
            in columnHeader.get('dimensions', [])
        ]
        metricHeaders = [
            {'name': entry.get('name').replace('ga:', ''),
             'type': self.metricMap.get(entry.get('type'), 'varchar(255)')}
            for entry
            in columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        ]

        print('samples read', report.get('data', {}).get('samplesReadCounts', 0))
        print('total read', report.get('data', {}).get('totals', 0))
        print('row count', report.get('data', {}).get('rowCount', 0))

        rows = report.get('data', {}).get('rows', [])
        all_data = []
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

                all_data.append(data)

        df_google_data = pd.DataFrame(all_data)
        postgres_hook = PostgresHook(self.postgres_conn_id)
        df_google_data.to_sql(name=self.destination_table,
                              con=postgres_hook.get_sqlalchemy_engine(),
                              dtype=self.destination_table_dtypes,
                              if_exists=self.if_exists,
                              schema=self.destination_schema
                              )
