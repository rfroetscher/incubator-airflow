import logging

from airflow.hooks import EmrHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException


class EmrCreateJobFlowOperator(BaseOperator):
    """
    Creates an EMR JobFlow, reading the config from the EMR connection.
    A dictionary of JobFlow overrides can be passed that override the config from the connection.

    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param emr_conn_id: emr connection to use
    :type emr_conn_id: str
    :param job_flow_overrides: boto3 style arguments to override emr_connection extra
    :type steps: dict
    """
    template_fields = []
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            aws_conn_id='s3_default',
            emr_conn_id='emr_default',
            job_flow_overrides={},
            *args, **kwargs):
        super(EmrCreateJobFlowOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        self.job_flow_overrides = job_flow_overrides

    def execute(self, context):
        emr = EmrHook(aws_conn_id=self.aws_conn_id, emr_conn_id=self.emr_conn_id)

        logging.info('Creating JobFlow')
        response = emr.create_job_flow(self.job_flow_overrides)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('JobFlow creation failed: %s' % response)
        else:
            logging.info('JobFlow with id %s created', response['JobFlowId'])
            return response['JobFlowId']
