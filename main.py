import requests
import json

def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)

headers = {
    'X-Authorization-Token': 'fd0199ae570d0f68101675aef2eedeae9b56d9fc4217a771e669ba2e53cc43056956fcd22eb62687',
}

track = "LM951174328CN"


#Определяем службу доставки
response = requests.get('https://gdeposylka.ru/api/v4/tracker/detect/'+track, headers=headers)
if response.status_code == 200:
    answer = response.json()
    #print(answer[data][0]["courier"]["slug"])   #Print SLUG from an array
    slug =(answer["data"][0]["courier"]["slug"])
    print(slug)

        #Запрос по посылке
    response = requests.get('https://gdeposylka.ru/api/v4/tracker/' +slug + "/" + track, headers=headers)
    if response.status_code == 200:
        answer = response.json()
    jprint(answer)

elif response.status_code == 404:
    print('Ошибка 404')