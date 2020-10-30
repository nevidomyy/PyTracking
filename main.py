import requests
import json
import mysql.connector
from mysql.connector import Error
import time
import options


def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


# displaying an array to the screen. for example, jprint(JSAnswer)
def jprint(obj):
    # create a formatted string of the Python JSON object
    # ensure_ascii = False => is using for display russian characters
    text = json.dumps(obj, sort_keys=True, ensure_ascii=False, indent=4)
    print(text)


def tracking(track):
    # define the delivery service
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


# contains conditions for selecting track numbers
def get_track_numbers():
    connect = create_connection(options.My_Host, options.My_User, options.My_Password, options.My_DB_name)
    query = connect.cursor()
    query.execute("SELECT ID, Trackcode FROM aop_rsticketspro_ticket_notes WHERE ID > "
                  "(SELECT LastProcessedID FROM StartIndex)")
    query_result = query.fetchall()
    return query_result


def parsing(track):
    jprint(track)
    return


results = get_track_numbers()
row, number = 0, 0
# in range(count) count - the number of processed tracks per run
for number in range(options.track_count):
    if number < len(results):
        print(str(row+1) + '. ' + 'Обработка трек-номера: ' + str(results[row][0]) + ' ' + str(results[row][1]))
        TrackNumber = results[row][1]
        # start post tracking function and writing result into JSAnswer
        JSAnswer = tracking(TrackNumber)
        parsing(JSAnswer)
        row = row + 1
        time.sleep(1)
