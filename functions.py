import requests
import json
from mysql.connector import Error
from mysql.connector import connect
import time
import datetime
import options
import logging
JSAnswer = ''
track_consolidation = ''

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


def j_print(obj: json):
    """
    :param: obj: JSON array
    :return: displaying an array to the screen. for example, j_print(JSAnswer)
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
    JSAnswer = None
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
                    JSAnswer = None
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
    :param: tracknumber: current TrackNumber
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


def parsing(trackinfo: json, tracknumber: str, track_id: int):
    """
    :define: Receives information and processes it according to the rules.
    Writes the result of the form: "status. Location" to the Database in the Status column
    :param: trackinfo: JSON info from function tracking(track: str)
    :param: tracknumber: current Track Number
    :param: track_id: current ID from database
    :return: none
    """
    global track_consolidation
    track_consolidation = None

    recorded_status = get_recorded_status(tracknumber)
    if recorded_status in options.status_stoplist:
        print(f'ПРОПУСК ОБРАБОТКИ... Причина: Статус трек-номера'
              f' в базе данных с ID {track_id} в Стоп-листе - "{recorded_status}"')
        return

    try:
        status_name = trackinfo["data"]["checkpoints"][0]["status_name"]
    except TypeError:
        # if Status in array JSON doesn't exist - recording temporary status
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

    try:
        track_consolidation = trackinfo["data"]["tracking_number_current"]
    except TypeError:
        track_consolidation = ''
    except IndexError:
        track_consolidation = ''

    status = rename_status(status_name, track_location)
    # j_print(trackinfo)
    connection = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connection.cursor()
    # writing status and location into Database, column "status"
    try:
        query.execute(f'UPDATE {options.Main_Table} SET Status = "{status}" WHERE Trackcode = "{tracknumber}"')
        print(f'УСПЕХ! Для трек-номера {tracknumber} в базу данных записан статус: {status} ')
    except Error as e:
        print(f'ОШИБКА при записи статуса в БД: {e}.')

    connection.commit()


def protect_day(tracknumber: str, track_id: int):
    """
    :define: Receives information and processes it according to the rules.
    Writes the result of the form: days in the PD column
    :param: tracknumber: current Track Number
    :param: track_id: current ID from database
    """
    # Get current date
    cd = datetime.datetime.now()

    # Get current track order date
    connection = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connection.cursor()
    query.execute(f'SELECT date FROM {options.Main_Table} WHERE Trackcode = "{tracknumber}" AND id = "{track_id}"')
    query_result = query.fetchone()
    temp = query_result
    order_date = temp[0]
    if order_date is not None and len(temp) != 0:
        delta_days = order_date-cd
        protect_days = delta_days.days + options.pd
        # if protect_days < 0:
        #    protect_days = 0
        # Write protect_days in DB
        try:
            query.execute(f'UPDATE {options.Main_Table} SET Protect_days = "{protect_days}"'
                          f' WHERE Trackcode = "{tracknumber}" AND id = "{track_id}"')
        except Error as e:
            print(f'ОШИБКА при записи количества дней защиты покупателей: {e} по {tracknumber}.')
        connection.commit()
    else:
        print(f'Пустая дата в строке с ID = {track_id}')


def write_empty_trackcode(track_id: int) -> None:
    """
    :define: writing default status for empty track number. Status is defined in options file
    :return: none
    :param: empty_track_id: this id for string where found empty track_number
    """
    connection = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connection.cursor()
    status = options.emptystatus
    print(f'ВНИМАНИЕ... Причина: Пустой трек-номер в строке с ID {track_id}')
    try:
        query.execute(f'UPDATE {options.Main_Table} SET Status = "{status}" WHERE ID = "{track_id}"')
        print(f'Для пустого трек-номера в строке с {track_id} в базу данных записан статус: {status}')
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


def write_track_consolidation(track_number_consolidation: str, track_id: int, track_number: str) -> None:
    """
    :define: writing to database track number for consolidation package
    :param track_number_consolidation: track number consolidation
    :param track_id:id for current track number
    :param track_number: current track number
    """
    if track_number_consolidation is not None and len(track_number_consolidation) != 0:
        print(f'ID элемента в базе: {track_id}. Трек-номер: {track_number}. '
              f'Трек консолидации: {track_number_consolidation}')
    else:
        print(f'Для трек-номера {track_number} отсутствует консолидация')


def main() -> None:
    """
    :rtype: None
    """
    results = get_track_numbers()
    if options.track_count < len(results):
        all_track_count = options.track_count
    else:
        all_track_count = len(results)
    # initial run. Doesn't write info to database
    for number in range(options.track_count):
        if number < len(results):
            track_id = results[number][0]
            track_number = results[number][1]
            print(f'{number + 1} из {all_track_count}. Холостая обработка трек-номера c ID:'
                  f' {track_id} TrackCode: {track_number}')
            if track_number is not None and len(track_number) != 0:
                tracking(track_number, 0)
            if number == (options.track_count - 1) or number == len(results) - 1:
                print(f'Завершение холостой обработки. Последний обработанный ID = {track_id}')
        elif len(results) == 0:
            print('Список трек-номеров для обработки пуст. Проверьте StartIndex')
            # in range(count) count - the number of processed tracks per run
    # Working run. Doesn't write info to database
    for number in range(options.track_count):
        if number < len(results):
            track_id = results[number][0]
            track_number = results[number][1]
            print(f'{number + 1} из {all_track_count}. Обработка трек-номера c ID:'
                  f' {track_id} TrackCode: {track_number}')
            if track_number is not None and len(track_number) != 0:
                tracking(track_number, 0)
                parsing(JSAnswer, track_number, track_id)
                # j_print(JSAnswer)
                write_track_consolidation(track_consolidation, track_id, track_number)
                protect_day(track_number, track_id)
            else:
                write_empty_trackcode(track_id)
            # writing id for last processed Track in DataBase StartIndex Table
            if number == (options.track_count - 1) or number == len(results) - 1:
                print(f'Завершение... Запись в базу данных ID последнего обработанного элемента: ID = {track_id}')
                write_last_elem(track_id)
                logging.info(f'Последний обработанный элемент: ID = {track_id}')
        elif len(results) == 0:
            print('Список трек-номеров для обработки пуст. Проверьте StartIndex')
