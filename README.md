# Pyspark Streaming MQTT
MQTT is a machine-to-machine (M2M)/"Internet of Things" connectivity protocol. It was designed as an extremely lightweight publish/subscribe messaging transport. It is useful for connections with remote locations where a small code footprint is required and/or network bandwidth is at a premium.

To run the streaming service, use the following script:
``` 
$SPARK_HOME/bin/spark-submit --master local[*] \  
--files $HOME/projects/pyspark-streaming-mqtt/configs/streaming_config.json \  
--jars external/mqtt-assembly/target/spark-streaming-mqtt-assembly_*.jar \  
--packages org.apache.bahir:spark-streaming-mqtt_2.11:2.4.0 \  
$HOME/projects/pyspark-streaming-mqtt/streaming_service/streaming_mqtt.py  
```
## Virtual Machine
To run the streaming service on a virtual server having the same settings for which it was developed.
* Use the [ubuntu-22.04.2-live-server-amd64 ISO](https://releases.ubuntu.com/jammy/ubuntu-22.04.2-live-server-amd64.iso)
* Run the `vm_server_config.sh` file

## IoT Devices
The device projects that communicate with this streaming service can be found at https://github.com/FabioAguiar/iot-projects

##Dataframe view
This project also features a notebook file to demonstrate how records can be viewed in a dataframe.

## Credits
This project uses the `mqtt.py` file, which provides an MQTT Spark Streaming Connector, from the  [Apache Bahir project](https://github.com/apache/bahir), under the Apache License 2.0. Thanks to the Apache Bahir project team for the provided code.
