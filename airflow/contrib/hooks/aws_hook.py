import boto3
from airflow.hooks.base_hook import BaseHook


class AwsHook(BaseHook):
    """
    Interact with AWS.

    This class is a thin wrapper around the boto3 python library.
    """
    def __init__(self, aws_conn_id='aws_default'):
        self.aws_conn_id = aws_conn_id

    def get_client_type(self, client_type):
        connection_object = self.get_connection(self.aws_conn_id)
        return boto3.client(
            client_type,
            region_name=connection_object.extra_dejson.get('region_name'),
            aws_access_key_id=connection_object.login,
            aws_secret_access_key=connection_object.password,
        )
