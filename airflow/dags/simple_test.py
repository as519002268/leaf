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
from datetime import datetime,timedelta


def  load_data():
     import sys
     sys.path.append('/root/etl/module')
     import db
     Y=db.DB_F(20).main()
     Y.connect()
     sql='''
     select a1.taskname,path,schedule,owner,start_date,priority_weight
     from 
     (
     select taskname,path,output_table,rely_on,schedule,owner,start_date,priority_weight
     from basisdata.job_config
     where length(rely_on)=0 
     )a1
     left join 
     (
     select UNNEST(regexp_split_to_array(rely_on, '[/,]')) as used_table
     from basisdata.job_config
     where length(rely_on)<>0 
     )a2
     on a1.output_table=a2.used_table
     where a2.used_table is null
     '''
     df_independecy=Y.sqldataframe(sql)
     sql='''
     select taskname,path,output_table,rely_on,schedule,owner,start_date,priority_weight
     from basisdata.job_config
     '''
     df=Y.sqldataframe(sql)
     df_dependecy=df[~df['taskname'].isin(df_independecy['taskname'])]
     sql='''
     select a1.taskname
     from 
     (
     select taskname,path,output_table,rely_on,schedule,owner,start_date,priority_weight
     from basisdata.job_config
     where length(rely_on)<>0 
     )a1
     left join 
     (
     select UNNEST(regexp_split_to_array(rely_on, '[/,]')) as used_table
     from basisdata.job_config
     where length(rely_on)<>0 
     )a2
     on a1.output_table=a2.used_table
     where a2.used_table is null
     '''
     top_task=Y.sqldataframe(sql)
     Y.unconnect()
     return df_independecy,df_dependecy,top_task

df_independecy,df_dependecy,top_task=load_data()
default_args = {
    'depends_on_past': False,
    'email': ['songshutong@treefinance.com.cn'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'pool': 'first_pool',
    'dagrun_timeout':timedelta(hours=2)
}

dag = DAG(
    dag_id='real_test',default_args=default_args,
    schedule_interval='@hourly',
    )

rely_task_done = DummyOperator(
    task_id='rely_task_done',
    dag=dag,
    owner='airflow',
    start_date=datetime(2017,6,16,12),
    trigger_rule='all_done'
    )

# for i in df_independecy['taskname']:
#     tasktable=df_independecy[df_independecy['taskname']==i]
#     task = BashOperator(
#            task_id=i,
#            bash_command=('python '+tasktable['path'].values[0]+i+'.py'),
#            owner=tasktable['owner'].values[0],
#            start_date=datetime.strptime(tasktable['start_date'].values[0],'%Y-%m-%d %H:%M:%S'),
#            priority_weight=tasktable['priority_weight'].values[0],
#            dag=dag)
#     task.set_upstream(rely_task_done)


for i in df_dependecy['taskname']:
    tasktable=df_dependecy[df_dependecy['taskname']==i]
    if not dag.has_task(i):
       task = BashOperator(
           task_id=i,
           bash_command=('python '+tasktable['path'].values[0]+i+'.py'),
           owner=tasktable['owner'].values[0],
           start_date=datetime.strptime(tasktable['start_date'].values[0],'%Y-%m-%d %H:%M:%S'),
           priority_weight=tasktable['priority_weight'].values[0],
           dag=dag)
    else:
        task=dag.get_task(i)
    dep=tasktable['rely_on'].values[0]
    # if  i in  top_task.values:
    #     task.set_downstream(rely_task_done)
    if dep<>'':
          depend=dep.split(',')
          for l in depend:
              try:
                    rely_task=df_dependecy[df_dependecy['output_table']==l]['taskname'].values[0]
                    tasktable_2=df_dependecy[df_dependecy['taskname']==rely_task]
                    if  not dag.has_task(rely_task):
                        rely_on=BashOperator(
                                   task_id=rely_task,
                                   bash_command=('python '+tasktable_2['path'].values[0]+rely_task+'.py'),
                                   owner=tasktable_2['owner'].values[0],
                                   start_date=datetime.strptime(tasktable_2['start_date'].values[0],'%Y-%m-%d %H:%M:%S'),
                                   priority_weight=tasktable_2['priority_weight'].values[0],
                                   dag=dag)
                        task.set_upstream(rely_on) 
                    else:
                        task.set_upstream(dag.get_task(rely_task))
              except Exception,e:
                continue