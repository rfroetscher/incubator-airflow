# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import datetime
from dateutil.tz import tzlocal
from mock import MagicMock, patch

from airflow import configuration
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor

DESCRIBE_JOB_FLOWS_RUNNING_RETURN = {
    'JobFlows': [
        {
            'BootstrapActions': [],
            'ExecutionStatusDetail': {
                'CreationDateTime': datetime.datetime(2016, 6, 17, 21, 3, 17, 913000, tzinfo=tzlocal()),
                'EndDateTime': None,
                'LastStateChangeReason': 'Steps completed',
                'ReadyDateTime': datetime.datetime(2016, 6, 17, 21, 11, 52, 66000, tzinfo=tzlocal()),
                'StartDateTime': datetime.datetime(2016, 6, 17, 21, 11, 52, 66000, tzinfo=tzlocal()),
                'State': 'RUNNING'
            }
        }
    ],
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
        'RequestId': '8613a4d2-34e8-11e6-9d46-951401f04e0e'
    }
}

DESCRIBE_JOB_FLOWS_COMPLETED_RETURN = {
    'JobFlows': [
        {
            'BootstrapActions': [],
            'ExecutionStatusDetail': {
                'CreationDateTime': datetime.datetime(2016, 6, 17, 21, 3, 17, 913000, tzinfo=tzlocal()),
                'EndDateTime': datetime.datetime(2016, 6, 17, 21, 14, 37, 557000, tzinfo=tzlocal()),
                'LastStateChangeReason': 'Steps completed',
                'ReadyDateTime': datetime.datetime(2016, 6, 17, 21, 11, 52, 66000, tzinfo=tzlocal()),
                'StartDateTime': datetime.datetime(2016, 6, 17, 21, 11, 52, 66000, tzinfo=tzlocal()),
                'State': 'COMPLETED'
            }
        }
    ],
    'ResponseMetadata': {
        'HTTPStatusCode': 200,
        'RequestId': '8613a4d2-34e8-11e6-9d46-951401f04e0e'
    }
}


class TestEmrJobFlowSensor(unittest.TestCase):
    def setUp(self):
        configuration.test_mode()

        # Mock out the emr_client (moto has incorrect response)
        self.mock_emr_client = MagicMock()
        self.mock_emr_client.describe_job_flows.side_effect = [
            DESCRIBE_JOB_FLOWS_RUNNING_RETURN,
            DESCRIBE_JOB_FLOWS_COMPLETED_RETURN
        ]

        # Mock out the emr_client creator
        self.boto3_client_mock = MagicMock(return_value=self.mock_emr_client)


    def test_execute_calls_with_the_job_flow_id_until_it_reaches_a_terminal_state(self):
        with patch('boto3.client', self.boto3_client_mock):

            operator = EmrJobFlowSensor(
                task_id='test_task',
                poke_interval=1,
                job_flow_id='j-8989898989',
                aws_conn_id='aws_default'
            )

            operator.execute(None)

            # make sure we called twice
            self.assertEqual(self.mock_emr_client.describe_job_flows.call_count, 2)

            # make sure it was called with the job_flow_id
            self.mock_emr_client.describe_job_flows.assert_called_with(JobFlowIds=['j-8989898989'])


if __name__ == '__main__':
    unittest.main()
