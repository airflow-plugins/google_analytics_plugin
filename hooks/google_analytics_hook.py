from airflow.hooks.base_hook import BaseHook

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import time

class GoogleAnalyticsHook(BaseHook):
    def __init__(self, google_analytics_conn_id='google_analytics_default'):
        self.google_analytics_conn_id = google_analytics_conn_id
        self.connection = self.get_connection(google_analytics_conn_id)

        self.client_secrets = self.connection.extra_dejson['client_secrets']

    def get_service_object(self, api_name, api_version, scopes):
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.client_secrets, scopes)
        return build(api_name, api_version, credentials=credentials)

    def get_analytics_report(self, view_id, since, until, sampling_level, dimensions, metrics, page_size, include_empty_rows):
        analytics = self.get_service_object('analyticsreporting', 'v4', ['https://www.googleapis.com/auth/analytics.readonly'])

        reportRequest = {
            'viewId': view_id,
            'dateRanges': [{ 'startDate': since, 'endDate': until }],
            'samplingLevel': sampling_level or 'LARGE',
            'dimensions': dimensions,
            'metrics': metrics,
            'pageSize': page_size or 100,
            'includeEmptyRows': include_empty_rows or False
        }

        response = analytics.reports().batchGet(body={ 'reportRequests': [reportRequest] }).execute()

        if response.get('reports'):
            report = response['reports'][0]
            rows = report.get('data', {}).get('rows', [])

            while report.get('nextPageToken'):
                time.sleep(1)
                reportRequest.update({ 'pageToken': report['nextPageToken'] })
                response = analytics.reports().batchGet(body={ 'reportRequests': [reportRequest] }).execute()
                report = response['reports'][0]
                rows.extend(report.get('data', {}).get('rows', []))

            if report['data']:
                report['data']['rows'] = rows

            return report
        else:
            return {}
