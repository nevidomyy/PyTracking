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


def tracking(track: str):
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
    query.execute("SELECT ID, Trackcode FROM aop_rsticketspro_ticket_notes WHERE ID > "
                  "(SELECT LastProcessedID FROM StartIndex)")
    query_result = query.fetchall()
    return query_result


def parsing(trackinfo: json):
    """
    :param trackinfo: JSON info from function tracking(track: str)
    :return: none
    """
    # ToDo: function parsing track info to database from json
    jprint(trackinfo)
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
    query.execute('SELECT ID FROM aop_rsticketspro_ticket_notes ORDER BY id DESC LIMIT 1')
    query_result = query.fetchone()
    if query_result[0] == last_elem:
        last_elem = 0
    query.execute(f'UPDATE StartIndex SET LastProcessedID = {last_elem}')
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
        else:
            print(f'ОШИБКА: Пустой трек-номер в строке с ID {results[number][0]}')
        time.sleep(1)
        # parsing(JSAnswer)
        # writing ID for last processed Track in DataBase StartIndex Table
        if number == (options.track_count - 1) or number == len(results) - 1:
            print(f'Запись в базу данных ID последнего обработанного элемента: ID = {results[number][0]}')
            write_last_elem(results[number][0])
