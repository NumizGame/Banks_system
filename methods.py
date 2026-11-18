import psycopg
from psycopg import OperationalError
import datetime
import colorama

colorama.init()


class User:
    def __init__(self, user, password):
        self.user = user
        self.password = password

        try:
            with psycopg.connect(dbname='banks system', user=user, password=password):
                self.isValid = True
                print(f'Welcome, {user} !')
                if user == 'postgres':
                    self.isAdmin = True

        except OperationalError:
            self.isValid = False
            self.isAdmin = False

            print('Incorrect password. Try again')


    def get_course_on_date(self, date, bank_code, *args):
        if self.isValid:
            try:
                with psycopg.connect(dbname='banks system', user=self.user, password=self.password) as conn:
                    with conn.cursor() as cur:
                        if bank_code == 'ALL':
                            if args[0] == 'ALL':
                                query = t"""
                                SELECT currencies.name, currencies.code, cc.rate_against_rub / cc.nominal, cc.operation_type, banks.bank_code, banks.name
                                FROM currencies JOIN curr_courses AS cc USING (crid) JOIN banks USING (bid)
                                WHERE cc.date = {date} ORDER BY banks.bank_code, currencies.code, cc.operation_type
                                """

                            else:
                                query = t"""
                                SELECT currencies.name, currencies.code, cc.rate_against_rub / cc.nominal, cc.operation_type, banks.bank_code, banks.name
                                FROM currencies JOIN curr_courses AS cc USING (crid) JOIN banks USING (bid)
                                WHERE cc.date = {date} AND currencies.code = ANY({list(args)})
                                ORDER BY banks.bank_code, currencies.code, cc.operation_type
                                """

                        else:
                            if args[0] == 'ALL':
                                query = t"""
                                SELECT currencies.name, currencies.code, cc.rate_against_rub / cc.nominal, cc.operation_type, banks.bank_code, banks.name 
                                FROM currencies JOIN curr_courses AS cc USING (crid) JOIN banks USING (bid)
                                WHERE cc.date = {date} AND banks.bank_code = {bank_code} ORDER BY currencies.code, cc.operation_type
                                """

                            else:
                                query = t"""
                                    SELECT currencies.name, currencies.code, cc.rate_against_rub / cc.nominal, cc.operation_type, banks.bank_code, banks.name 
                                    FROM currencies JOIN curr_courses AS cc USING (crid) JOIN banks USING (bid)
                                    WHERE cc.date = {date} AND banks.bank_code = {bank_code} AND currencies.code = ANY({list(args)})
                                    ORDER BY currencies.code, cc.operation_type
                                    """

                        cur.execute(query)
                        resp = cur.fetchall()

                if len(resp) == 0:
                    print('Something went wrong, try again')

            except Exception as e:
                print(str(e))

            else:
                for i in range(len(resp)):
                    if resp[i - 1][4] != resp[i][4] and i != 0:
                        print('-' * 75)

                    print(f'{resp[i][0]}({resp[i][1]}) '.ljust(25, ' ') + f'| {resp[i][5]}({resp[i][4]}) '.ljust(21, ' ') + f'| {resp[i][3]} | {resp[i][2]:.5f} Ruble(RUB)')

        else:
            pass


    def get_today_course(self, bank_code, *args):
        self.get_course_on_date(datetime.date.today(), bank_code, *args)


    def get_course_dynamics(self, bank_code, days, *args):
        if self.isValid:
            try:
                with psycopg.connect(dbname='banks system', user=self.user, password=self.password) as conn:
                    with conn.cursor() as cur:
                        days = int(days)

                        if bank_code == 'ALL':
                            if args[0] == 'ALL':
                                query = t"""
                                SELECT currencies.name, currencies.code, cc.date, cc.operation_type, cc.rate_against_rub / cc.nominal, (cc.rate_against_rub - LAG(cc.rate_against_rub) OVER wind) * 1.0 / (LAG(cc.rate_against_rub) OVER wind) AS diff, 
                                banks.bank_code, banks.name FROM currencies JOIN curr_courses AS cc USING (crid) JOIN banks USING (bid) WHERE cc.date >= {datetime.date.today()}::date - {days} 
                                WINDOW wind AS (PARTITION BY currencies.code, cc.operation_type ORDER BY cc.date) ORDER BY banks.bank_code, currencies.code, cc.operation_type, cc.date DESC
                                """

                            else:
                                query = t"""
                                SELECT currencies.name, currencies.code, cc.date, cc.operation_type, cc.rate_against_rub / cc.nominal, (cc.rate_against_rub - LAG(cc.rate_against_rub) OVER wind) * 1.0 / (LAG(cc.rate_against_rub) OVER wind) AS diff, 
                                banks.bank_code, banks.name FROM currencies JOIN curr_courses AS cc USING (crid) JOIN banks USING (bid) WHERE cc.date >= {datetime.date.today()}::date - {days} AND currencies.code = ANY({list(args)})
                                WINDOW wind AS (PARTITION BY currencies.code, cc.operation_type ORDER BY cc.date) ORDER BY banks.bank_code, currencies.code, cc.operation_type, cc.date DESC
                                """

                        else:
                            if args[0] == 'ALL':
                                query = t"""
                                SELECT currencies.name, currencies.code, cc.date, cc.operation_type, cc.rate_against_rub / cc.nominal, (cc.rate_against_rub - LAG(cc.rate_against_rub) OVER wind) * 1.0 / (LAG(cc.rate_against_rub) OVER wind) AS diff, 
                                banks.bank_code, banks.name FROM currencies JOIN curr_courses AS cc USING (crid) JOIN banks USING (bid) WHERE banks.bank_code = {bank_code} AND cc.date >= {datetime.date.today()}::date - {days}
                                WINDOW wind AS (PARTITION BY currencies.code, cc.operation_type ORDER BY cc.date) ORDER BY currencies.code, cc.operation_type, cc.date DESC
                                """

                            else:
                                query = t"""
                                SELECT currencies.name, currencies.code, cc.date, cc.operation_type, cc.rate_against_rub / cc.nominal, (cc.rate_against_rub - LAG(cc.rate_against_rub) OVER wind) * 1.0 / (LAG(cc.rate_against_rub) OVER wind) AS diff, 
                                banks.bank_code, banks.name FROM currencies JOIN curr_courses AS cc USING (crid) JOIN banks USING (bid) WHERE banks.bank_code = {bank_code} AND cc.date >= {datetime.date.today()}::date - {days} AND currencies.code = ANY({list(args)})
                                WINDOW wind AS (PARTITION BY currencies.code, cc.operation_type ORDER BY cc.date) ORDER BY currencies.code, cc.operation_type, cc.date DESC
                                """

                        cur.execute(query)
                        resp = cur.fetchall()

                if len(resp) == 0:
                    print('Something went wrong, try again')

            except Exception as e:
                print(str(e))

            else:
                for i in range(len(resp)):
                    if resp[i - 1][3] != resp[i][3] and i != 0:
                        print('-' * 75)

                    print(f'{resp[i][0]}({resp[i][1]}) '.ljust(25, ' ') + f'| {resp[i][7]}({resp[i][6]}) '.ljust(21, ' ') + f'| {resp[i][3]} | {resp[i][2]} | {resp[i][4]:.5f} Ruble(RUB) | {'NAN' if not resp[i][5] else (colorama.Fore.RED if resp[i][5] < 0 else colorama.Fore.GREEN + '+') + str(round(resp[i][5] * 100, 3)) + '%' + colorama.Style.RESET_ALL}')

        else:
            pass


    def buy_or_sell_currency(self, operation_type, bank_code, curr, amount):
        if self.isValid:
            try:
                with psycopg.connect(dbname='banks system', user=self.user, password=self.password) as conn:
                    with conn.cursor() as cur:
                        amount = float(amount)

                        if bank_code == 'ALL':
                            query = t"""
                            SELECT currencies.name, currencies.code, cc.rate_against_rub / cc.nominal * {amount} AS value, banks.bank_code, banks.name FROM currencies JOIN curr_courses AS cc 
                            USING (crid) JOIN banks USING (bid) WHERE currencies.code = {curr} AND cc.date = {datetime.date.today()} AND cc.operation_type = {operation_type} ORDER BY banks.bank_code
                            """

                        else:
                            query = t"""
                            SELECT currencies.name, currencies.code, cc.rate_against_rub / cc.nominal * {amount} AS value, banks.bank_code, banks.name FROM currencies JOIN curr_courses AS cc 
                            USING (crid) JOIN banks USING (bid) WHERE banks.bank_code = {bank_code} AND currencies.code = {curr} AND cc.date = {datetime.date.today()} AND cc.operation_type = {operation_type}
                            """

                        cur.execute(query)
                        resp = cur.fetchall()

                        if len(resp) == 0:
                            print('Something went wrong, try again')

            except Exception as e:
                print(str(e))

            else:
                for el in resp:
                    print(f'{el[4]}({el[3]}) '.ljust(21, ' ') + f'| {amount} {el[0]}({el[1]}) = {el[2]:.5f} Ruble(RUB)')

        else:
            pass


    def buy_currency(self, bank_code, curr, amount):
        self.buy_or_sell_currency('sell', bank_code, curr, amount)

    def sell_currency(self, bank_code, curr, amount):
        self.buy_or_sell_currency('buy', bank_code, curr, amount)


    def best_convert_currency(self, curr1, curr2, amount):
        if self.isValid:
            try:
                with psycopg.connect(dbname='banks system', user=self.user, password=self.password) as conn:
                    with conn.cursor() as cur:
                        amount = float(amount)

                        query = t'''
                        SELECT currencies.name, currencies.code, cc.rate_against_rub / cc.nominal AS value, banks.bank_code, banks.name FROM
                        currencies JOIN curr_courses AS cc USING (crid) JOIN banks USING (bid) WHERE cc.date = {datetime.date.today()} AND currencies.code = {curr1} AND 
                        operation_type = 'buy' AND cc.rate_against_rub = (SELECT MAX(rate_against_rub) FROM curr_courses WHERE date = {datetime.date.today()} AND crid = (SELECT crid FROM currencies WHERE code = {curr1}) AND operation_type = 'buy')
                        UNION ALL
                        SELECT currencies.name, currencies.code, cc.rate_against_rub / cc.nominal AS value, banks.bank_code, banks.name FROM
                        currencies JOIN curr_courses AS cc USING (crid) JOIN banks USING (bid) WHERE cc.date = {datetime.date.today()} AND currencies.code = {curr2} AND 
                        operation_type = 'sell' AND cc.rate_against_rub = (SELECT MIN(rate_against_rub) FROM curr_courses WHERE date = {datetime.date.today()} AND crid = (SELECT crid FROM currencies WHERE code = {curr2}) AND operation_type = 'sell')
                        '''

                        cur.execute(query)

                        resp = cur.fetchmany(2)

                        if len(resp) == 0:
                            print('Something went wrong, try again')

            except Exception as e:
                print(str(e))

            else:
                res = resp[0][2] * amount / resp[1][2]

                print(f'{resp[0][4]}({resp[0][3]}) --> {resp[1][4]}({resp[1][3]}) | {amount} {resp[0][0]}({resp[0][1]}) = {res:.5f} {resp[1][0]}({resp[1][1]})')


        else:
            pass


    def all_currencies(self):
        if self.isValid:
            try:
                with psycopg.connect(dbname='banks system', user=self.user, password=self.password) as conn:
                    with conn.cursor() as cur:
                        query = 'SELECT code, name FROM currencies ORDER BY code'

                        cur.execute(query)

                        resp = cur.fetchall()

                        if len(resp) == 0:
                            print('Something went wrong, try again')

            except Exception as e:
                print(str(e))

            else:
                for el in resp:
                    print(f'{el[1]} | {el[0]})')

        else:
            pass


    def all_banks(self):
        if self.isValid:
            try:
                with psycopg.connect(dbname='banks system', user=self.user, password=self.password) as conn:
                    with conn.cursor() as cur:
                        query = 'SELECT bank_code, name, main_address FROM banks ORDER BY bank_code'

                        cur.execute(query)

                        resp = cur.fetchall()

                        if len(resp) == 0:
                            print('Something went wrong, try again')

            except Exception as e:
                print(str(e))

            else:
                for el in resp:
                    print(f'{el[1]}({el[0]}) '.ljust(21, ' ') + f'| {el[2]}')

        else:
            pass