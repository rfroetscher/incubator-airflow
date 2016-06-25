from airflow.exceptions import AirflowException
from airflow.contrib.hooks import AwsHook


class EmrHook(AwsHook):
    """
    Interact with AWS EMR. emr_conn_id is only neccessary for using the create_job_flow method.
    """

    def __init__(self, emr_conn_id=None, *args, **kwargs):
        self.emr_conn_id = emr_conn_id
        super(EmrHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        self.conn = self.get_client_type('emr')
        return self.conn

    def create_job_flow(self, job_flow_overrides):
        """
        Creates a job flow using the config from the EMR connection.
        Keys of the json extra hash may have the arguments of the boto3 run_job_flow method.
        Overrides for this config may be passed as the job_flow_overrides.
        """

        if not self.emr_conn_id:
            raise AirflowException('emr_conn_id must be present to use create_job_flow')

        emr_conn = self.get_connection(self.emr_conn_id)

        config = emr_conn.extra_dejson.copy()
        config.update(job_flow_overrides)

        response = self.get_conn().run_job_flow(
            Name=config['Name'],
            LogUri=config['LogUri'],
            ReleaseLabel=config['ReleaseLabel'],
            Instances=config['Instances'],
            Steps=config.get('Steps', []),
            Applications=config['Applications'],
            VisibleToAllUsers=config['VisibleToAllUsers'],
            JobFlowRole=config['JobFlowRole'],
            ServiceRole=config['ServiceRole'],
            Tags=config['Tags']
        )

        return response
