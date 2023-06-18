# Kafka Settings

Aprende mas sobre Kafka en:
- <https://www.enmilocalfunciona.io/aprendiendo-apache-kafka-parte-1>
- <https://kafka.apache.org/documentation/#quickstart>

## Instalar y configurar  Zookeeper + Kafka

1. Descargar binarios de <https://kafka.apache.org/downloads>.

    ```shell
        wget https://downloads.apache.org/kafka/3.2.3/kafka_2.12-3.2.3.tgz
    ```

2. Descomprimir `tar -xzf kafka_2.12-3.2.3.tgz`
3. Entramos en la carpeta descomprimida de kafka `cd kafka_2.12-xxx`.
4. (*Opcional*) Creamos dos carpetas para almacenar datos de kafka y zookeeper.

    ```shell
    $mkdir -p data/zookeeper
    $mkdir -p data/kafka
    ```

5. (*Opcional*) Modificar la variable log.dirs dentro del archivo `config/zookeeper.properties`.

    ```bash
    $vim config/zookeeper.properties
    dataDir=/.../.../kafka_2.12-2.4.0/data/zookeeper
    ```

6. Ejecutamos zookeeper pasando como argumento la ruta del archivo `config/zookeeper.properties`.

    ```bash
    $zookeeper-server-start.sh config/zookeeper.properties
    ```

7. (*Opcional*) Modificar la variable log.dirs dentro del archivo `config/server.properties`.

    ```bash
    $vim config/server.properties
    log.dirs=/.../.../kafka_2.12-2.4.0/data/kafka
    ```

8. Inicializar Kafka server pasando como argumento el archivo `config/server.properties`.

    ```bash
    $kafka-server-start.sh config/server.properties
    ```

## Kafka Topics

Crear un topico en kafka.

```bash
$kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
```

Describir kafka topic

```bash
$kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092
```

### Escribir y leer eventos mediante un Producer + Consumer

Ejecute el cliente *Producer* de la consola para escribir algunos eventos en su Topic. De manera predeterminada, cada línea que ingrese generará un evento separado que se escribirá en el Topic.

```bash
$kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
```

Abra otra terminal en paralelo y ejecute el cliente *consumer* de la consola para leer los eventos que acaba de crear

```bash
$kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
```

Enviar mensaje desde desde la shell mediante la consola del *producer*

```shell
$echo Hola desde la consola | kafka-console-producer.sh --broker-list localhost:9092 --topic quickstart-events
```
