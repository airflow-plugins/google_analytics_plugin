import json
import os

from airflow.models import BaseOperator

from airflow.hooks.S3_hook import S3Hook

from google_analytics_plugin.hooks.google_analytics_hook import GoogleAnalyticsHook


class GoogleAnalyticsAccountSummariesToS3Operator(BaseOperator):
    template_fields = ('s3_key',)

    def __init__(self,
                 google_analytics_conn_id,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 brand,
                 space,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.google_analytics_conn_id = google_analytics_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.brand = brand
        self.space = space

    def execute(self, context):
        ga_conn = GoogleAnalyticsHook(self.google_analytics_conn_id)
        s3_conn = S3Hook(self.s3_conn_id)

        account_summaries = ga_conn.get_account_summaries()

        file_name = '/tmp/{key}.jsonl'.format(key=self.s3_key)
        with open(file_name, 'w') as ga_file:
            data = []
            for item in account_summaries.get('items', []):
                root_data_obj = {
                    'account_id': item['id'],
                    'brand': self.brand,
                    'space': self.space
                }

                for web_property in item.get('webProperties', []):
                    data_obj = {}
                    data_obj.update(root_data_obj)

                    data_obj['property_id'] = web_property['id']

                    for profile in web_property.get('profiles', []):
                        data_obj['profile_id'] = profile['id']
                        data_obj['profile_name'] = profile['name']
                        data.append(data_obj)

            json_data = '\n'.join([json.dumps(d) for d in data])
            ga_file.write(json_data)

        s3_conn.load_file(file_name, self.s3_key, self.s3_bucket, True)
        os.remove(file_name)
