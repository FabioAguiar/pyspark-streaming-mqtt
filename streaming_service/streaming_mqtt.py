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

# Informações do broker URI no arquivo config.json
brokerUrl = config['broker_url']
topic = config['topic']
username = config['username']
password = config['password']

mqttStream = MQTTUtils.createStream(ssc, brokerUrl, topic, username=None, password=None)


# cria tupla com um único elemento
df = mqttStream.map(lambda x: (x,))

# Função para converter os dados RDD em Parquet 
def processar_rdd(rdd):
    if not rdd.isEmpty():
        data = rdd.collect()
        print(tuple(data[0])[0])
        print(type(tuple(data[0])[0]))

        # extrair a dict        
        dict_data = tuple(data[0])[0]
        # converter para o Pandas DataFrame
        df_pandas = pd.DataFrame(eval(dict_data), index=[0])
        # converter para o PySpark Pandas
        df_pyspark = pspd.DataFrame(df_pandas)
        print(df_pyspark)
        # converter e salvar em Parquet        
        df_pyspark.to_parquet(config['parquet_dir'], mode='append')

# Processar cada RDD individualmente
df.foreachRDD(processar_rdd)

ssc.start()
ssc.awaitTermination()