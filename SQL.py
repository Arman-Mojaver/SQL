from random import randint, uniform, choice, shuffle
from sshtunnel import SSHTunnelForwarder
import MySQLdb as mdb
import pandas as pd
import os


_ = '-----------------------------------------------------------------------------------'


ssh_host = "192.168.137.129"
ssh_user = os.environ.get('ssh_user')
ssh_password = os.environ.get('ssh_password')
ssh_port = 22
sql_hostname = 'localhost'
sql_username = 'root'
sql_password = ''
sql_main_database = 'db'
sql_port = 3306


class Person:
    def __init__(self):
        self.id = None
        self.age = randint(20, 60)  # int. Numeric(Discrete)
        self.weight = randint(60, 95)  # int. Numeric(Discrete)
        self.gender = ['Male' if randint(0, 1) == 0 else 'Female'][0]  # str. Categorical(Nominal)
        self.smokes = ['Yes' if randint(0, 1) == 0 else 'No'][0]  # str. Categorical(Nominal)
        self.income = round(uniform(15000, 90000), 2)  # float. Numeric(Continuous)
        self.country = self.get_country()  # Countries.
        self.time = round(uniform(8, 15), 2)  # float. Minutes. Numeric(Continuous)
        self.position = None  # str. Categorical(Ordinal). A dictionary can be used to order the category.
        self.coffee_size = self.get_coffee_size()  # str. Categorical(Ordinal).
        self.kids = randint(0, 4)

    @staticmethod
    def get_country():
        list_of_countries = ['Sweden', 'France', 'USA', 'Germany', 'China', 'Austria', 'Finland', 'Spain']
        return choice(list_of_countries)

    @staticmethod
    def get_coffee_size():
        list_of_sizes = ['small', 'medium', 'large']
        return choice(list_of_sizes)

    def __repr__(self):
        return '{}, {}, {}, {}, {}, {}, {}, {}, {}'.format(self.age, self.weight, self.gender, self.smokes,
                                                           self.income, self.country, self.time,
                                                           self.position, self.coffee_size)


def cardinal(number):
    last_digit = str(number)[-1]

    if len(str(number)) >= 2:
        second_to_last_digit = str(number)[-2]
        if second_to_last_digit == str(1):
            return str(number) + ' th'

    if last_digit == str(1):
        return str(number) + ' st'
    elif last_digit == str(2):
        return str(number) + ' nd'
    elif last_digit == str(3):
        return str(number) + ' rd'
    else:
        return str(number) + ' th'


def create_db(size):
    units_ol = [Person() for _ in range(size)]
    numbers_list = list(range(1, size + 1))
    shuffle(numbers_list)
    for index, (unit, number) in enumerate(zip(units_ol, numbers_list)):
        unit.position = cardinal(number)
        unit.id = index + 1
    return pd.DataFrame([vars(o) for o in units_ol])


def get_key_strings(key_list):
    final_string = '('
    for key in key_list:
        final_string = final_string + key + ', '
    final_string = final_string[:-2] + ')'
    return final_string


# -------------------------------------------------- Commands ----------------------------------------------------------


def execute_command(_query):
    with SSHTunnelForwarder((ssh_host, ssh_port),
                            ssh_password=ssh_password,
                            ssh_username=ssh_user,
                            remote_bind_address=('127.0.0.1', 3306)) as server:
        connection = mdb.connect(user=sql_username,
                                 passwd=sql_password,
                                 db=sql_main_database,
                                 host='127.0.0.1',
                                 port=server.local_bind_port)
        cursor = connection.cursor()

        cursor.execute(_query)
        connection.commit()


def query_result(_query):
    with SSHTunnelForwarder((ssh_host, ssh_port),
                            ssh_password=ssh_password,
                            ssh_username=ssh_user,
                            remote_bind_address=('127.0.0.1', 3306)) as server:
        connection = mdb.connect(user=sql_username,
                                 passwd=sql_password,
                                 db=sql_main_database,
                                 host='127.0.0.1',
                                 port=server.local_bind_port)
        return pd.read_sql(_query, connection)


def add_rows_to_table(table_name, df):
    list_of_row_values = [list(row.to_dict().values()) for index, row in df.iterrows()]

    initial_string = 'INSERT INTO ' + table_name
    keys_string = get_key_strings(list(df.keys()))
    intermediate_string = ' VALUES '

    add_one_row_command_ol = []
    for row in list_of_row_values:
        values_string = '(' + str(row)[1:-1] + ');'
        complete_string = initial_string + keys_string + intermediate_string + values_string
        add_one_row_command_ol.append(complete_string)

    for index, add_one_row_command in enumerate(add_one_row_command_ol):
        execute_command(add_one_row_command)
        print('finished ' + str(index + 1) + '/' + str(len(add_one_row_command_ol)))


def load_table(table_name):
    command = 'Select * from ' + table_name + ';'
    return query_result(command)


def create_table(table_name, column_names, column_types):
    data_name_type = '('
    for name, type in zip(column_names, column_types):
        data_name_type = data_name_type + name + ' ' + type + ', '
    data_name_type = data_name_type[:-2] + ');'

    command = 'CREATE TABLE ' + table_name + ' ' + data_name_type
    execute_command(command)


def delete_table(table_name):
    execute_command('DROP TABLE ' + table_name + ';')


def delete_all_records_from_table(table_name):
    execute_command('DELETE FROM ' + table_name + ';')

    
# ----------------------------------------------------------------------------------------------------------------------


# Numpy and Pandas printing settings.
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 4000)
pd.set_option('display.width', 1000)


# ---------------------------------------------- SQL string generation -------------------------------------------------


# df2 = create_db(100)
# print(df2)

# keys = ['id', 'age', 'weight', 'gender', 'smokes', 'income', 'country', 'time', 'position', 'coffee_size', 'kids']
# types = ['int', 'int', 'int', 'char(255)', 'char(255)', 'double', 'char(255)', 'double', 'char(255)', 'char (255)', 'int']
# create_table('people', keys, types)
# create_table('people2', keys, types)

# delete_table('people')
# delete_all_records_from_table('people2')
# add_rows_to_table('people2', df2)




# ----------------------------------------------------------------------------------------------------------------------


# # Using WHERE. Using Operators: and, or, between, !=, in
# print(query_result("Select * from people WHERE age > 40;"))
# print(query_result("Select * from people WHERE age between 40 and 50;"))
# print(query_result("Select * from people WHERE weight > 80;"))
# print(query_result("Select * from people WHERE gender = 'Female';"))
# print(query_result("Select * from people WHERE coffee_size = 'Large' and gender = 'Female';"))
# print(query_result("Select * from people WHERE coffee_size = 'small' or gender = 'Male';"))
# print(query_result("Select * from people WHERE time < 10 or income > 70000;"))
# print(query_result("Select age, gender, coffee_size from people WHERE coffee_size = 'Large' and gender = 'Female';"))
# print(query_result("SELECT DISTINCT gender from people;"))
# print(query_result("Select * from people where country != 'USA';"))
# print(query_result("Select * from people where country != 'USA';"))
# print(query_result("Select * from people where country in ('USA', 'Sweden');"))
