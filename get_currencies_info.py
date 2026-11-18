import airflow
from airflow.decorators import task
import datetime

with airflow.DAG(dag_id = 'get_currencies_info', schedule = None, max_active_runs = 1) as dag:
    @task(task_id = 'get_info_from_api', retries = 3, retry_delay = datetime.timedelta(minutes = 15))
    def get_info_from_api():
        import zeep
        import zeep.helpers

        wsdl = 'https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?wsdl'

        with zeep.Client(wsdl=wsdl) as client:
            resp = client.service.EnumValutes(False)
            data = zeep.helpers.serialize_object(resp, dict)['_value_1']['_value_1']

        currencies = []

        written_codes = []

        for val in data:
            val = val['EnumValutes']
            if val['VnumCode'] is not None and val['VcharCode'] not in written_codes:
                currencies.append((val['VnumCode'], val['VcharCode'], val['VEngname']))
                written_codes.append(val['VcharCode'])

        return currencies


    @task(task_id = 'load_into_stage', retries = 3, retry_delay = datetime.timedelta(minutes = 5))
    def load_into_stage(currencies):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id = 'postgres_conn')

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = 'TRUNCATE TABLE staging_currencies'
                cur.execute(query)

                query = 'INSERT INTO staging_currencies (crid, code, name) VALUES (%s, %s, %s)'
                cur.executemany(query, currencies)


    @task(task_id = 'merge_stage_and_database', retries = 3, retry_delay = datetime.timedelta(minutes = 5))
    def merge_stage():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id = 'postgres_conn')

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = '''
                MERGE INTO currencies AS target USING staging_currencies AS source ON target.crid = source.crid AND target.code = source.code 
                WHEN NOT MATCHED THEN INSERT (crid, code, name) VALUES (source.crid, source.code, source.name)
                '''

                cur.execute(query)


    currencies = get_info_from_api()
    staging_area_load = load_into_stage(currencies)
    database_load = merge_stage()

    currencies >> staging_area_load >> database_load