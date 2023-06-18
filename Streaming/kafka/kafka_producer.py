from kafka import KafkaProducer

data = "Hola mundo desde PYTHON"

# Implementación de función utilitaria para Producer
def writeDataToTopic(data):
	#Instanciamos la conexión al servidor KAFKA
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    #Preparamos el envío de los datos al tópico
    producer.send("quickstart-events", data.encode())

    #Envíamos los datos al tópico
    producer.flush()


#Enviamos un mensaje
# writeDataToTopic(data)

# #Enviamos un mensaje
# writeDataToTopic("Esta es una prueba")

# #Enviamos un mensaje
# writeDataToTopic("De escritura en un topico kafka")

# #Enviamos un mensaje
writeDataToTopic('[{"ID_PERSONA": 89, "ID_EMPRESA": 4, "MONTO": 780}, {"ID_PERSONA": 66, "ID_EMPRESA": 5, "MONTO": 1447}, {"ID_PERSONA": 50, "ID_EMPRESA": 3, "MONTO": 736}]')