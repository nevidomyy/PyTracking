import requests
import json
from mysql.connector import Error
from mysql.connector import connect
import time
import options


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


def tracking(track: str) -> json:
    """
    :define: delivery service and get track info
    :param track: Track number from BD
    :return: json track info
    """
    response = requests.get('https://gdeposylka.ru/api/v4/tracker/detect/' + track, headers=options.headers)
    if response.status_code == 200:
        answer = response.json()
        # if result of detecting delivery service is successful
        if answer['result'] == 'success':
            slug = answer['data'][0]['courier']['slug']
            # getting info for track
            time.sleep(1)
            response = requests.get('https://gdeposylka.ru/api/v4/tracker/' + slug + '/' +
                                    track, headers=options.headers)
            if response.status_code == 200:
                answer = response.json()
                return answer
        else:
            return 'Unknown Error... Check Track Number'

    elif response.status_code == 404:
        return 'Error: 404'


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


def parsing(trackinfo: json, tracknumber: str):
    """
    :define: Receives information and processes it according to the rules.
     Writes the result of the form: "status. Location" to the Database in the Status column
    :param trackinfo: JSON info from function tracking(track: str)
    :param tracknumber: current TrackNumber
    :return: none
    """
    recorded_status = get_recorded_status(tracknumber)
    status_stoplist = ['Home', 'Delivery', 'Fck']
    status_renamelist = ['returned', 'returned2']
    location_stoplist = ['city', 'city2', 'city3']
    location_renamelist = ['location to rename']

    if recorded_status in status_stoplist:
        print(f'ПРОПУСК ОБРАБОТКИ... Причина: Статус из БД = {recorded_status}')
        return

    try:
        status_name = trackinfo["data"]["checkpoints"][0]["status_name"]
    except TypeError:
        # if Status in array JSON doesnt exist - recording temporary status
        status_name = 'Ожидается отправка'

    try:
        track_location = trackinfo["data"]["checkpoints"]["0"]["location_translated"]
    except TypeError:
        track_location = ''

    if track_location in location_stoplist:
        track_location = ''
    if track_location in location_renamelist:
        track_location = 'renamed location'
    if status_name in status_renamelist:
        status_name = 'Прибыл в пункт назначения'
    status = f'{status_name}.{track_location}'
    # jprint(trackinfo)

    connection = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connection.cursor()
    # writing status and location into Database, column "status"
    try:
        query.execute(f'UPDATE {options.Main_Table} SET Status = "{status}" WHERE Trackcode = "{tracknumber}"')
        print(f'УСПЕХ. Статус записан в БД для трек-номера {tracknumber}. Записанный статус: {status}')
    except Error as e:
        print(f'ОШИБКА при записи статуса в БД: {e}.')
    connection.commit()
    return


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
    query.execute(f'UPDATE {options.Support_Table} SET LastProcessedID = {last_elem}')
    connection.commit()
    return


results = get_track_numbers()
num = 0
# in range(count) count - the number of processed tracks per run
for number in range(options.track_count):
    if number < len(results):
        TrackNumber = results[number][1]
        if TrackNumber is not None:
            num = num + 1
            print(f'{num}. Обработка трек-номера: {results[number][0]} {results[number][1]}')
            JSAnswer = tracking(TrackNumber)
            parsing(JSAnswer, TrackNumber)
        else:
            print(f'ПРОПУСК ОБРАБОТКИ... Причина: Пустой трек-номер в строке с ID {results[number][0]}')
        time.sleep(1)
        # writing ID for last processed Track in DataBase StartIndex Table
        if number == (options.track_count - 1) or number == len(results) - 1:
            print(f'Запись в базу данных ID последнего обработанного элемента: ID = {results[number][0]}')
            write_last_elem(results[number][0])
