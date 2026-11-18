from methods import *
import os
from dotenv import load_dotenv

load_dotenv()
admin_password = os.environ.get('PASSWORD')

usr = User('postgres', admin_password)

while True:
    command = input('~ ')

    parsed_command = command.split(' ')

    if parsed_command[0] == 'get_course_on_date':
        usr.get_course_on_date(*parsed_command[1:])

    elif parsed_command[0] == 'get_today_course':
        usr.get_today_course(*parsed_command[1:])

    elif parsed_command[0] == 'get_course_dynamics':
        usr.get_course_dynamics(*parsed_command[1:])

    elif parsed_command[0] == 'buy_currency':
        usr.buy_currency(*parsed_command[1:])

    elif parsed_command[0] == 'sell_currency':
        usr.sell_currency(*parsed_command[1:])

    elif parsed_command[0] == 'best_convert_currency':
        usr.best_convert_currency(*parsed_command[1:])

    elif parsed_command[0] == 'all_currencies':
        usr.all_currencies()

    elif parsed_command[0] == 'all_banks':
        usr.all_banks()