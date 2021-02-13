import requests
import json
from mysql.connector import Error
from mysql.connector import connect
import time
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


def jprint(obj: json):
    """
    :param obj: JSON array
    :return: displaying an array to the screen. for example, jprint(JSAnswer)
    """
    # create a formatted string of the Python JSON object
    # ensure_ascii = False => is using for display russian characters
    text = json.dumps(obj, sort_keys=True, ensure_ascii=False, indent=4)
    print(text)


def tracking(track: str, try_count: int) -> json:
    """
    :define: delivery service and get track info
    :param track: Track number from BD
    :param try_count: Start value of try count, default = 0
    :return: json track info
    """
    global JSAnswer
    response = requests.get
    response.status_code = 0
    if try_count > options.attempts:
        return
    if try_count > 0:
        time.sleep(5)
        print(f'Попытка {try_count}...')
    time.sleep(2)
    try:
        response = requests.get(f'https://gdeposylka.ru/api/v4/tracker/detect/{track}',
                                headers=options.headers, timeout=options.timeout)
    except requests.Timeout:
        print('Упс!! Время ожидания истекло.')
        try_count = try_count + 1
        tracking(track, try_count)
    except requests.ConnectionError:
        print('Упс!! Ошибка подключения к интернету.')
        try_count = try_count + 1
        tracking(track, try_count)
    except requests.RequestException as e:
        print('Упс!! Возникла непредвиденная ошибка!')
        print(str(e))
        try_count = try_count + 1
        tracking(track, try_count)
    if response.status_code == 200:
        answer = response.json()
        # if result of detecting delivery service is successful
        if answer['result'] == 'success':
            slug = answer['data'][0]['courier']['slug']
            # getting info for track
            time.sleep(2)
            try:
                response = requests.get(f'https://gdeposylka.ru/api/v4/tracker/{slug}/{track}',
                                        headers=options.headers, timeout=options.timeout)
            except requests.Timeout:
                print('Упс!! Время ожидания истекло.')
                try_count = try_count + 1
                tracking(track, try_count)
            except requests.ConnectionError:
                print('Упс!! Ошибка подключения к интернету.')
                try_count = try_count + 1
                tracking(track, try_count)
            except requests.RequestException as e:
                print('Упс!! Возникла непредвиденная ошибка!')
                print(str(e))
                try_count = try_count + 1
                tracking(track, try_count)
            else:
                if response.status_code == 200:
                    JSAnswer = response.json()
                    return JSAnswer
                elif response.status_code == 404:
                    return 'Error 404'
        else:
            return 'Unknown Error... Check Track Number'
    elif response.status_code == 404:
        return 'Error 404'


def get_track_numbers():
    """
    :return: array [ID] [TrackNumber] of track numbers where ID > StartIndex
    """
    connection = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connection.cursor()
    query.execute(f'SELECT ID, Trackcode FROM {options.Main_Table} WHERE ID > '
                  f'(SELECT LastProcessedID FROM {options.Support_Table})')
    query_result = query.fetchall()
    return query_result


def get_recorded_status(tracknumber: str):
    """
    :param tracknumber: current TrackNumber
    :return: recorded status for given TrackNumber from DataBase
    """
    connection = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connection.cursor()
    query.execute(f'SELECT Status FROM {options.Main_Table} WHERE Trackcode = "{tracknumber}"')
    query_result = query.fetchone()

    return query_result[0]


def rename_status(status_name: str, track_location: str) -> str:
    """
    :define: rename status from JSON
    :param status_name: status_name from JSON
    :param track_location: track_location from JSON
    :return: new status_name for recording into Database
    """
    if track_location in options.location_stoplist:
        track_location = ''
    if track_location in options.location_renamelist:
        track_location = options.renamed_location
    if track_location is None:
        track_location = ''
    if status_name in options.status_renamelist:
        status_name = options.status_renamelist_renamed
    # change delivery status
    for status_number in range(len(options.sfr)):
        if status_name == options.sfr[status_number]:
            status_name = options.sr[status_number]

    return f'{status_name}. {track_location}'


def parsing(trackinfo: json, tracknumber: str):
    """
    # ToDo: parsing function
    :define: Receives information and processes it according to the rules.
     Writes the result of the form: "status. Location" to the Database in the Status column
    :param: trackinfo: JSON info from function tracking(track: str)
    :param: tracknumber: current Track Number
    :param: id: current ID from database
    :return: none
    """
    recorded_status = get_recorded_status(tracknumber)
    if recorded_status in options.status_stoplist:
        print(f'ПРОПУСК ОБРАБОТКИ... Причина: Статус трек-номера'
              f' в базе данных с ID {ID} в Стоп-листе - "{recorded_status}"')
        return

    try:
        status_name = trackinfo["data"]["checkpoints"][0]["status_name"]
    except TypeError:
        # if Status in array JSON doesnt exist - recording temporary status
        status_name = options.status_waiting
    except IndexError as e:
        print(f'Произошла ошибка обновления статуса для трек-номера {tracknumber}: {e} Попробуйте позже...')
        status_name = options.status_waiting

    try:
        track_location = trackinfo["data"]["checkpoints"][0]["location_translated"]
        if track_location == "":
            track_location = trackinfo["data"]["checkpoints"][0]["courier"]["name"]
    except TypeError:
        track_location = ''
    except IndexError:
        track_location = ''
    status = rename_status(status_name, track_location)
    # jprint(trackinfo)
    connection = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connection.cursor()
    # writing status and location into Database, column "status"
    try:
        query.execute(f'UPDATE {options.Main_Table} SET Status = "{status}" WHERE Trackcode = "{tracknumber}"')
        print(f'УСПЕХ! Для трек-номера {tracknumber} в базу данных записан статус: {status} ')
    except Error as e:
        print(f'ОШИБКА при записи статуса в БД: {e}.')

    connection.commit()


def protect_day(tracknumber: str, track_id:int):
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
    query.execute(f'SELECT date FROM {options.Main_Table} WHERE Trackcode = "{tracknumber}" AND id = "{track_id}"')
    query_result = query.fetchone()
    temp = query_result
    if temp is not None and len(temp) != 0:
        order_date = temp[0]
        delta_days = order_date-cd
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


def write_empty_trackcode(empty_track_id: int) -> None:
    """
    :define: writting default status for empty track number. Status is defined in options file
    :return: none
    :param: empty_track_id: this id for string where finded empty track number
    """
    connection = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connection.cursor()
    status = options.emptystatus
    print(f'ВНИМАНИЕ... Причина: Пустой трек-номер в строке с ID {ID}')
    try:
        query.execute(f'UPDATE {options.Main_Table} SET Status = "{status}" WHERE ID = "{empty_track_id}"')
        print(f'Для пустого трек-номера в строке с {ID} в базу данных записан статус: {status}')
    except Error as e:
        print(f'ОШИБКА при записи статуса в БД: {e}.')

    connection.commit()
        

def write_last_elem(last_elem: int):
    """
    :define: writing number of last processed ID in DataBase, Table - StartIndex
    :return: none
    :param last_elem: last processed ID from array of function get_track_numbers or 0 if last_elem = last database elem
    """
    connection = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connection.cursor()
    # get bigger ID from database
    query.execute(f'SELECT ID FROM {options.Main_Table} ORDER BY id DESC LIMIT 1')
    query_result = query.fetchone()
    if query_result[0] == last_elem:
        last_elem = 0
    try:
        query.execute(f'UPDATE {options.Support_Table} SET LastProcessedID = {last_elem}')
        connection.commit()
        print(f'Завершено... Запись последнего обработанного элемента прошла успешно.')
    except Error as e:
        print(f'Ошибка записи последнего обработанного элемента в БД: {e}')

    return


results = get_track_numbers()
# initial run. Doesnt write info to database
# ToDo: Первый круг в холостую, не пишем данные в базу.
for number in range(options.track_count):
    if number < len(results):
        ID = results[number][0]
        TrackNumber = results[number][1]
        print(f'{number + 1} из {len(results)}. Холостая обработка трек-номера c ID: {ID} TrackCode: {TrackNumber}')
        if TrackNumber is not None and len(TrackNumber) != 0:
            tracking(TrackNumber, 0)
        if number == (options.track_count - 1) or number == len(results) - 1:
            print(f'Завершение холостой обработки. Последний обработанный ID = {ID}')
    elif len(results) == 0:
        print('Список трек-номеров для обработки пуст. Проверьте StartIndex')
# in range(count) count - the number of processed tracks per run
for number in range(options.track_count):
    if number < len(results):
        ID = results[number][0]
        TrackNumber = results[number][1]
        print(f'{number + 1} из {len(results)}. Обработка трек-номера c ID: {ID} TrackCode: {TrackNumber}')
        if TrackNumber is not None and len(TrackNumber) != 0:
            tracking(TrackNumber, 0)
            parsing(JSAnswer, TrackNumber)
            protect_day(TrackNumber, ID)
        else:
            write_empty_trackcode(ID)            
        # writing ID for last processed Track in DataBase StartIndex Table
        if number == (options.track_count - 1) or number == len(results) - 1:
            print(f'Завершение... Запись в базу данных ID последнего обработанного элемента: ID = {ID}')
            write_last_elem(ID)
    elif len(results) == 0:
        print('Список трек-номеров для обработки пуст. Проверьте StartIndex')
