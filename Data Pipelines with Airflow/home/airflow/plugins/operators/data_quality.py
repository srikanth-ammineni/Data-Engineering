from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 tables=[],
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.checks=checks
        self.tables=tables


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Checking for counts')
        for table in self.tables:
            records = redshift.get_records(f"Select count(*) from {table}")[0]
            if records[0]==0:
               raise ValueError(f"Data quality check failed. {table} contains zero rows")
            else:
                self.log.info(f"Count checks passed for {table},{records[0]} records present in the table")
                              
        self.log.info('Checking for Null ids')
        for check in self.checks:
            records = redshift.get_records(check['check_sql'])[0]
            if records[0] != check['expected_result']: 
                raise ValueError(f"Data quality check failed. {check['table']} contains null in id column, , got {records[0]} records instead")
            else:
                self.log.info(f"Null id checks passed for {check['table']}")              
        