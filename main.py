import requests
import json

headers = {
    'X-Authorization-Token': 'fd0199ae570d0f68101675aef2eedeae9b56d9fc4217a771e669ba2e53cc43056956fcd22eb62687'
    }


def jprint(obj):
    # create a formatted string of the Python JSON object
    # ensure_ascii = False => is using for display russian characters
    text = json.dumps(obj, sort_keys=True, ensure_ascii=False, indent=4)
    print(text)


def tracking(track):

    # define the delivery service
    response = requests.get('https://gdeposylka.ru/api/v4/tracker/detect/' + track, headers=headers)
    if response.status_code == 200:
        answer = response.json()
        # if result of detecting delivery service is successful
        if answer['result'] == 'success':
            slug = answer['data'][0]['courier']['slug']
            # getting info for track
            response = requests.get('https://gdeposylka.ru/api/v4/tracker/' + slug + '/' + track, headers=headers)
            if response.status_code == 200:
                answer = response.json()
                return answer
        else:
            return 'Unknown Error... Check Track Number'

    elif response.status_code == 404:
        return 'Error: 404'


TrackNumber = '10209751370135'
# Start post tracking function and writing result into JSAnswer
JSAnswer = tracking(TrackNumber)
# Displaying an array to the screen
jprint(JSAnswer)
