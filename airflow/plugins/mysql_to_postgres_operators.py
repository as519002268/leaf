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
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import cStringIO
from airflow.exceptions import AirflowException
import psycopg2.extras as pe
from airflow.plugins_manager import AirflowPlugin


class MySqlToPostgres(BaseOperator):
    """
    Copy data from MySQL to Postgres.
    """
    template_fields = ('sql','preoperator')
    template_ext = ('.sql',)
    ui_color = '#87CEEB'

    @apply_defaults
    def __init__(
            self,
            sql,
            postgres_table,
            mysql_conn_id,
            postgres_conn_id,
            preoperator=None,
            insert_check=True,
            insert_type='normal',
            *args, **kwargs):
        """
        :parm sql(string): The sql to execute on the Mysql table. 
        :parm postgres_table(string):  The table to insert data which store in Postgres and must have schema name like basisdata.etllog
        :parm mysql_conn_id(int): Reference to a specific MySQL hook
        :parm postgres_conn_id(int): Reference to a specific Postgres hook
        :parm preoperator:(string):  The sql to execute before data is inserted like 'delete' or 'truncate'
        :parm insert_check(bool): if True,the programe will raise a error when 0 row is inserted
        :parm insert_type(sting): 'fast' or 'normal',if set to 'fast',the programe will use copy_from to insert data

        """
        super(MySqlToPostgres, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_table = postgres_table
        self.mysql_conn_id = mysql_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.preoperator=preoperator
        self.insert_check=insert_check
        self.insert_type=insert_type

        self.sql_check()

    def sql_check(self):
        sql_columns=self.sql.split('from')[0]
        if '*' in sql_columns:
            raise AirflowException(" 'select *'  is forbidden")


    def get_mysql_cursor(self):
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        logging.info("Success connect to %s Mysql databases"%(self.mysql_conn_id))
        mysql_conn = mysql.get_conn()
        mysql_cursor = mysql_conn.cursor()
        return mysql_conn,mysql_cursor

    def get_postgres_cursor(self):
        postgres=PostgresHook(postgres_conn_id=self.postgres_conn_id)
        logging.info("Success connect to %s Postgres databases"%(self.postgres_conn_id))
        pg_conn = postgres.get_conn()
        pg_cursor=pg_conn.cursor()
        return pg_conn,pg_cursor

    def _fast_insert(self,mysql_cursor,
                          pg_cursor,
                          insert_columns):
        s_buf = cStringIO.StringIO()
        csv_writer = csv.writer(s_buf,delimiter='\t')
        csv_writer.writerows(mysql_cursor)
        s_buf.seek(1)
        pg_cursor.copy_from(s_buf,self.postgres_table,columns=insert_columns,null='')
        self._insert_check(pg_cursor)
        s_buf.close()

    def _insert_check(self,pg_cursor):
        n=pg_cursor.rowcount
        logging.info(n)
        if self.insert_check and n<=0:
           raise AirflowException("Data Quality is bad")

    def _normal_insert(self,mysql_cursor,
                            pg_cursor,
                            insert_columns):
        parm=mysql_cursor.fetchall()
        sql_insert="INSERT INTO %s%s"%(self.postgres_table,insert_columns)+'VALUES %s'
        pe.execute_values(pg_cursor,sql_insert.replace("\'",""),parm,page_size=500)
        self._insert_check(pg_cursor)
     
    def close_connect(self,conn,
                           cursor,
                           commit=False):
        if commit:
           conn.commit()
        cursor.close()
        conn.close() 

    def execute(self,context):
        try:
            mysql_conn,mysql_cursor=self.get_mysql_cursor()
            pg_conn,pg_cursor=self.get_postgres_cursor()            
            logging.info("RUN MySQL query")
            mysql_cursor.execute(self.sql)
            logging.info("MySQL query success")
            insert_columns=tuple([i[0] for i in mysql_cursor.description])
            if self.preoperator:
                logging.info("Running preoperator")
                logging.info(self.preoperator)
                pg_cursor.execute(self.preoperator) 
    
            if self.insert_type=='fast':   
                self._fast_insert(mysql_cursor,pg_cursor,insert_columns)
                logging.info("fast insert success")
    
            elif self.insert_type=='normal':
                self._normal_insert(mysql_cursor,pg_cursor,insert_columns)
                logging.info("normal insert success")
        except Exception,e:
                logging.info(str(e))
                try:
                  self.close_connect(mysql_conn,mysql_cursor)
                  self.close_connect(pg_conn,pg_cursor,commit=True)
                  logging.info("connection closed successfully")
                except:
                  logging.info("connection already has failed,There is nothing need to be closed") 
                raise  ValueError('The mysql_to_pg task failed')
class MySqlToPostgresPlugin(AirflowPlugin):
        name = "MySqlToPostgresPlugin"
        operators = [MySqlToPostgres]