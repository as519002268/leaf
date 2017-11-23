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

def find_the_task():
    import sys
    sys.path.append('/root/etl/module')
    import db
    Y=db.DB_F(1).main()
    Y.connect()
    sql='''
    select output_table,taskname,depended_table from basisdata.job_config_day
    where day_key='2017-06-13'
    '''
    df=Y.sqldataframe(sql)
    x=dict(df.iloc[:,:2].values)
    y=dict(df.iloc[:,1:3].values)
    Y.unconnect()    
    return x,y




args = {
    'owner': 'airflow',
    'start_date': datetime(2017,6,9)
}

dag = DAG(
    dag_id='day_job', default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60))


a,b=find_the_task()
for i in b.keys():
    if not dag.has_task(i):
       task = BashOperator(
           task_id=i,
           bash_command=('python /root/etl/day/%s'%i+'.py'),
           dag=dag)
    else:
        task=dag.get_task(i)
    if b[i]<>'':
          depend=b[i].split(',')
          for l in depend:
              try:
                    rely_task=a[l]
                    if  not dag.has_task(rely_task):
                        rely_on=BashOperator(
                                   task_id=rely_task,
                                   bash_command=('python /root/etl/day/%s'%rely_task+'.py'),
                                   dag=dag)
                        task.set_upstream(rely_on) 
                    else:
                        task.set_upstream(dag.get_task(rely_task))
              except Exception,e:
                continue


