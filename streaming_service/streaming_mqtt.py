"""
streaming_mqtt.py
~~~~~~~~~~

  $SPARK_HOME/bin/spark-submit --master local[*] \
    --files $HOME/projects/pyspark-streaming-mqtt/configs/streaming_config.json \
    --jars external/mqtt-assembly/target/spark-streaming-mqtt-assembly_*.jar \
    --packages org.apache.bahir:spark-streaming-mqtt_2.11:2.4.0 \
    $HOME/projects/pyspark-streaming-mqtt/streaming_service/streaming_mqtt.py    

  $SPARK_HOME/bin/spark-submit --master local[*] --files $HOME/projects/pyspark-streaming-mqtt/configs/streaming_config.json --jars external/mqtt-assembly/target/spark-streaming-mqtt-assembly_*.jar --packages org.apache.bahir:spark-streaming-mqtt_2.11:2.4.0 $HOME/projects/pyspark-streaming-mqtt/streaming_service/streaming_mqtt.py

"""

from os import listdir, path
import json
import datetime

from pyspark import SparkContext, SparkFiles
from pyspark.streaming import StreamingContext
from mqtt import MQTTUtils
import pyspark.pandas as pspd
import pandas as pd


sc = SparkContext(appName="MyMQTTStreaming")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("checkpoint")

# Obter o arquivo de configuração enviado ao cluster com --files
spark_files_dir = SparkFiles.getRootDirectory()
config_files = [filename
                for filename in listdir(spark_files_dir)
                if filename.endswith('config.json')]

config=None
if config_files:
    path_to_config_file = path.join(spark_files_dir, config_files[0])
    with open(path_to_config_file, 'r') as config_file:
        config = json.load(config_file)
    print('loaded config from ' + config_files[0])
else:
    print('no config file found')
    config = None

# Identificador Uniforme de Recurso (URI) do Broker obtidas no arquivo config.json
brokerUrl = config['broker_url']
topic = config['topic']
username = config['username']
password = config['password']

# Cria fluxo de entrada que recebe mensagens enviadas por um publisher MQTT
mqttStream = MQTTUtils.createStream(ssc, brokerUrl, topic, username, password)

# cria tupla com um único elemento
df_tuple = mqttStream.map(lambda x: (x,))

# Função para converter os dados RDD em Parquet 
def process_rdd(rdd):
    if not rdd.isEmpty():
        data = rdd.collect()
        print(tuple(data[0])[0])
        print(type(tuple(data[0])[0]))

        # Extrair dict da mensagem        
        dict_data = tuple(data[0])[0]
        # Converter para Pandas DataFrame
        df_pd = pd.DataFrame(eval(dict_data), index=[0])
        # Converter para PySpark Pandas DataFrame
        df_pspd = pspd.DataFrame(df_pd)
        print(df_pspd)
        # Concatenação da data atual com o diretório de saída de dados
        current_date = datetime.date.today()
        dir = config['parquet_dir']+str(current_date)+'/streaming_output.parquet'
        # Converter e salvar em Parquet
        df_pspd.to_parquet(dir, mode='append')

# Processar cada RDD individualmente
df_tuple.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination()