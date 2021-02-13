import json
from mysql.connector import Error
from mysql.connector import connect
import datetime
import options
import logging
JSAnswer = ''

file_log = logging.FileHandler('Log.log', 'w')
console_out = logging.StreamHandler()
# noinspection PyArgumentList
logging.basicConfig(format='[%(asctime)s]: %(message)s',
                    datefmt='%m.%d.%Y %H:%M:%S',
                    level=logging.INFO,
                    handlers=[file_log, console_out]
                    )
# logging.info('text')


def create_connection(host_name: str, user_name: str, user_password: str, db_name: str) -> connect:
    connection = None
    try:
        connection = connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        # print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
        quit(f'ERROR! Program has been stopped! There is no connection to the database.')

    return connection


def get_track_numbers():
    """
    :return: array [ID] [TrackNumber] of track numbers where ID > StartIndex
    """
    connection = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connection.cursor()
    query.execute(f'SELECT ID, Trackcode FROM {options.Main_Table} WHERE ID > {options.start_track_id}')
    query_result = query.fetchall()
    return query_result


def jprint(obj: json):
    """
    :param obj: JSON array
    :return: displaying an array to the screen. for example, jprint(JSAnswer)
    """
    # create a formatted string of the Python JSON object
    # ensure_ascii = False => is using for display russian characters
    text = json.dumps(obj, sort_keys=True, ensure_ascii=False, indent=4)
    print(text)


def protect_day(tracknumber: str, track_id: int):
    """
    :define: Receives information and processes it according to the rules.
    Writes the result of the form: "status. Location" to the Database in the Status column
    :param: tracknumber: current Track Number
    :param: id: current ID from database
    """
    # Get current date
    cd = datetime.datetime.now()

    # Get current track order date
    connection = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connection.cursor()
    query.execute(f'SELECT date FROM {options.Main_Table} WHERE Trackcode = "{tracknumber}" AND id = "{track_id}" ')
    query_result = query.fetchone()
    temp = query_result
    order_date = temp[0]
    if order_date is not None and len(temp) != 0:
        delta_days = order_date - cd
        protect_days = delta_days.days + options.pd
        if protect_days < 0:
            protect_days = 0
        # Write proctect_days in DB
        try:
            query.execute(f'UPDATE {options.Main_Table} SET Protect_days = "{protect_days}"'
                          f' WHERE Trackcode = "{tracknumber}" AND id = "{track_id}"')
        except Error as e:
            print(f'ОШИБКА при записи количества дней защиты покупателей: {e} по {tracknumber}.')
        connection.commit()
    else:
        print(f'ОШИБКА: Пустая дата в строке с ID = {track_id}')


results = get_track_numbers()
for number in range(options.pd_track_count):
    if number < len(results):
        ID = results[number][0]
        TrackNumber = results[number][1]
        print(f'{number + 1} из {len(results)}. Обработка трек-номера c ID: {ID} TrackCode: {TrackNumber}')
        if TrackNumber is not None and len(TrackNumber) != 0:
            protect_day(TrackNumber, ID)
        if number == (options.pd_track_count - 1) or number == len(results) - 1:
            print(f'Завершение... Последний обработанный элемент: ID = {ID}')
            logging.info(f'Последний обработанный элемент: ID = {ID}')
        elif len(results) == 0:
            print('Список трек-номеров для обработки пуст. Проверьте StartIndex')
