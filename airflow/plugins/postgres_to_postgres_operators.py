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

import csv
import logging
import MySQLdb
from  airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import cStringIO
from airflow.exceptions import AirflowException
import psycopg2.extras as pe
from airflow.plugins_manager import AirflowPlugin


class PostgresToPostgres(BaseOperator):
    """
    Copy data from Postgres to Postgres.
    """
    template_fields = ('sql','preoperator')
    template_ext = ('.sql',)
    ui_color = '#a2c4c9'
    @apply_defaults
    def __init__(
            self,
            sql,
            target_table,
            postgres_conn_id_source,
            postgres_conn_id_target,
            preoperator=None,
            insert_check=True,
            insert_type='normal',
            *args, **kwargs):
        """
        :parm sql(string): The sql to execute on the Mysql table. 
        :parm postgres_table(string):  The table to insert data which store in Postgres and must have schema name like basisdata.etllog
        :parm postgres_conn_id_source(int): Reference to source date hook
        :parm postgres_conn_id_target(int): Reference to insert target hook
        :parm preoperator:(string):  The sql to execute before data is inserted like 'delete' or 'truncate'
        :parm insert_check(bool): if True,the programe will raise a error when 0 row is inserted
        :parm insert_type(sting): 'fast' or 'normal',if set to 'fast',the programe will use copy_from to insert data

        """
        super(PostgresToPostgres, self).__init__(*args, **kwargs)
        self.sql = sql
        self.target_table = target_table
        self.postgres_conn_id_source = postgres_conn_id_source
        self.postgres_conn_id_target = postgres_conn_id_target
        self.preoperator=preoperator
        self.insert_check=insert_check
        self.insert_type=insert_type

        self.sql_check()


    def execute(self, context):
        try :
            pg_source_conn,pg_source_cursor=self.get_postgres_cursor(self.postgres_conn_id_source)
            pg_target_conn,pg_target_cursor=self.get_postgres_cursor(self.postgres_conn_id_target)            
            if  self.preoperator:
                logging.info("Running preoperator")
                logging.info(self.preoperator)
                pg_source_cursor.execute(self.preoperator)
            if self.postgres_conn_id_source==self.postgres_conn_id_target:
               self.same_conn_insert(pg_source_cursor)
            else:
               self.different_conn_insert(pg_source_cursor,pg_target_cursor)
        except Exception,e:
                logging.info(str(e))
                try:
                  self.close_connect(pg_source_conn,pg_source_cursor)
                  self.close_connect(pg_target_conn,pg_target_cursor,commit=True)
                  logging.info("connection closed successfully")
                except:
                  logging.info("connection already has failed,There is nothing need to be closed") 
                raise  ValueError('The pg_to_pg task failed')



    def get_postgres_cursor(self,postgres_conn_id):
        postgres=PostgresHook(postgres_conn_id=postgres_conn_id)
        logging.info("Success connect to %s Postgres databases"%(postgres_conn_id))
        pg_conn = postgres.get_conn()
        pg_cursor=pg_conn.cursor()
        return pg_conn,pg_cursor

    def _insert_check(self,pg_cursor):
        n=pg_cursor.rowcount
        logging.info(n)
        if self.insert_check and n<=0:
           raise AirflowException("Data Quality is bad")

    def close_connect(self,conn,
                           cursor,
                           commit=False):
        if commit:
           conn.commit()
        cursor.close()
        conn.close() 

    def same_conn_insert(self,cursor):
        insert_columns=self.get_columns(cursor)
        insert_sql=("insert into %s%s  "%(self.target_table,insert_columns)).replace("\'","")
        logging.info(insert_sql)
        final_sql=insert_sql+self.sql
        cursor.execute(final_sql)
        self._insert_check(cursor)
        logging.info("same conn insert success")
   
    def different_conn_insert(self,pg_source_cursor,
                                   pg_target_cursor):
        pg_source_cursor.execute(self.sql)
        logging.info("query success")
        insert_columns=tuple([i[0] for i in pg_source_cursor.description])
        if  self.insert_type=='fast':         
            self._fast_insert(pg_source_cursor,pg_target_cursor,insert_columns)
            logging.info("fast insert success")
    
        elif self.insert_type=='normal':
            self._normal_insert(pg_source_cursor,pg_target_cursor,insert_columns)
            logging.info("normal insert success")

    def _fast_insert(self,source_cursor,
                          target_cursor,
                          insert_columns):
        s_buf = cStringIO.StringIO()
        csv_writer = csv.writer(s_buf,delimiter='\t')
        csv_writer.writerows(source_cursor)
        s_buf.seek(1)
        target_cursor.copy_from(s_buf,self.postgres_table,columns=insert_columns,null='')
        self._insert_check(target_cursor)
        s_buf.close()


    def _normal_insert(self,source_cursor,
                            target_cursor,
                            insert_columns):
        parm=source_cursor.fetchall()
        sql_insert="INSERT INTO %s%s"%(self.postgres_table,insert_columns)+'VALUES %s'
        pe.execute_values(target_cursor,sql_insert.replace("\'",""),parm,page_size=500)
        self._insert_check(target_cursor)      


    def sql_check(self):
        sql_columns=self.sql.split('from')[0]
        if '*' in sql_columns:
            raise AirflowException(" 'select *'  is forbidden")
        self.sql_columns=sql_columns


    def get_columns(self,cursor):
        schema,table=self.target_table.split('.')
        cursor.execute(" select column_name from information_schema.columns where table_name='%s' and table_schema='%s' "%(schema,table))
        table_columns=cursor.fetchall()
        final_columns=[i[0] for i in  table_columns if i[0] in self.sql_columns]
        return tuple(final_columns)

class PostgresToPostgresPlugin(AirflowPlugin):
        name = "PostgresToPostgresPlugin"
        operators = [PostgresToPostgres]