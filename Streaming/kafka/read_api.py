from kafka import KafkaProducer
import time
import requests
import json


#Creamos una función utilitaria
def read_api(url):
  response = requests.get(url)
  jsonData = json.loads(response.content)
  return jsonData


#Convierte los datos obtenidos de la fuente en un formato escribible en el producer (string)
def convertDataToFormatProducer(dataFromSource):
	#Convertimos los datos a string
	dataFormatted = json.dumps(dataFromSource)
	return dataFormatted


def writeDataToTopic(data):
	producer = KafkaProducer(bootstrap_servers="localhost:9092")
	producer.send("quickstart-events", data.encode('utf-8'))
	producer.flush()


def run():
	url = 'http://api.open-notify.org/iss-now.json'

	dataFromSource = read_api(url)   
	print(type(dataFromSource))  # Dictionary
	
	dataFormatted = convertDataToFormatProducer(dataFromSource)
	print(type(dataFormatted))   # Str

	# writeDataToTopic(dataFormatted)   -- demo

	for n in range(0, 100000):
		dataFromSource = read_api(url)
		dataFormatted = convertDataToFormatProducer(dataFromSource)
		writeDataToTopic(dataFormatted)
		print("Iteration # "+str(n))
		time.sleep(1)


if __name__ == '__main__':
    run()