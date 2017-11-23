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

import airflow
from builtins import range
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime
from datetime import timedelta



args = {
    'owner': 'songshutong',
    'start_date': datetime(2017,6,12,11),
    'retries':3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='credit_fail', default_args=args,
    schedule_interval='@hourly',
    dagrun_timeout=timedelta(minutes=60))


task = BashOperator(
        task_id='run_credit_fail_hour',
        bash_command=('python /root/etl/hour/day_credit_fail_hour.py'),
        dag=dag)