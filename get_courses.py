import datetime
import airflow
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with airflow.DAG(dag_id = 'get_courses', schedule = '0 20 * * *', start_date = datetime.datetime(2026, 1, 1), catchup = False, max_active_runs = 1, dagrun_timeout = datetime.timedelta(hours = 3)) as dag:
    @task(task_id = 'get_courses_from_api', retries = 3, retry_delay = datetime.timedelta(minutes = 10))
    def get_courses_from_api(ds):
        import zeep
        import zeep.helpers

        wsdl = 'https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?wsdl'

        with zeep.Client(wsdl=wsdl) as client:
            resp = client.service.GetCursOnDate(ds)

            data = zeep.helpers.serialize_object(resp, dict)['_value_1']['_value_1']

        curr_data = [(el['VchCode'], float(el['Vcurs']), int(el['Vnom']), el['Vcode']) for el in map(lambda string: string['ValuteCursOnDate'], data)]

        return curr_data


    @task.branch(task_id = 'check_existing_currs', retries = 3, retry_delay = datetime.timedelta(minutes = 5))
    def check_existing_currs(task_instance):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        curr_data = task_instance.xcom_pull(task_ids = 'get_courses_from_api')

        hook = PostgresHook(postgres_conn_id='postgres_conn')

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = 'SELECT crid FROM currencies'

                cur.execute(query)
                existing_courses = list(map(lambda string: string[0], cur.fetchall()))

        curr_data = list(map(lambda string: string[3], curr_data))

        if set(curr_data) <= set(existing_courses):
            return 'save_cb_cours'

        else:
            return 'update_currs'


    update_currs = TriggerDagRunOperator(task_id = 'update_currs', trigger_dag_id = 'get_currencies_info', wait_for_completion = True, deferrable = True, retries = 3, retry_delay = datetime.timedelta(minutes = 15))


    @task(task_id = 'save_cb_cours', retries = 3, retry_delay = datetime.timedelta(minutes = 5), trigger_rule = 'none_failed_min_one_success')
    def save_cb_cours(task_instance, ds):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        curr_data = task_instance.xcom_pull(task_ids='get_courses_from_api')

        hook = PostgresHook(postgres_conn_id = 'postgres_conn')

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = 'TRUNCATE TABLE staging_curr_courses'
                cur.execute(query)

                currencies = [(curr_data[i][3], 1, ds, curr_data[i][1], curr_data[i][2]) for i in range(len(curr_data))]

                query = 'INSERT INTO staging_curr_courses (crid, bid, date, rate_against_rub, nominal) VALUES (%s, %s, %s, %s, %s)'

                cur.executemany(query, currencies)


    @task(task_id = 'save_other_courses', retries = 3, retry_delay = datetime.timedelta(minutes = 5))
    def save_other_courses(task_instance, ds):
        import random
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        curr_data = task_instance.xcom_pull(task_ids='get_courses_from_api')

        hook = PostgresHook(postgres_conn_id = 'postgres_conn')

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                banks_query = "SELECT bid FROM banks WHERE name <> 'Central Bank'"

                cur.execute(banks_query)

                banks = list(map(lambda string: string[0], cur.fetchall()))

                currencies = []

                for i in range(len(curr_data)):
                    for bid in banks:
                        for type in ['buy', 'sell']:
                            currencies.append((curr_data[i][3], bid, ds, curr_data[i][1] + random.randint(0, 2) if type == 'sell' else curr_data[i][1] - random.randint(0, 2), type, curr_data[i][2]))

                query = 'INSERT INTO staging_curr_courses (crid, bid, date, rate_against_rub, operation_type, nominal) VALUES (%s, %s, %s, %s, %s, %s)'

                cur.executemany(query, currencies)


    @task(task_id = 'merge_stage_and_db', retries = 3, retry_delay = datetime.timedelta(minutes = 5))
    def merge_stage():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id='postgres_conn')

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = '''
                        MERGE INTO curr_courses AS target USING staging_curr_courses AS source ON target.crid = source.crid AND 
                        target.bid = source.bid AND target.date = source.date AND target.operation_type IS NOT DISTINCT FROM source.operation_type 
                        WHEN NOT MATCHED THEN INSERT (crid, bid, date, rate_against_rub, operation_type, nominal) VALUES 
                        (source.crid, source.bid, source.date, source.rate_against_rub, source.operation_type, source.nominal)
                        '''

                cur.execute(query)


    crs_data = get_courses_from_api()
    check_task = check_existing_currs()
    save_cb_crs = save_cb_cours()
    save_other_crs = save_other_courses()
    merging_stage = merge_stage()

    crs_data >> check_task >> update_currs >> save_cb_crs >> save_other_crs >> merging_stage
    check_task >> save_cb_crs