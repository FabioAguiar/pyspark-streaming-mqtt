#!/bin/bash

echo "ATUALIZANDO REPOSITÓRIOS"
if ! (apt update) ; then
	echo "Não foi possível atualizar os repositórios. Verifique seu arquivo /etc/apt/source.list"
	exit 1
fi
echo "Atualização realizada com sucesso"

if ! (apt update -y); then
	echo "Não foi possível atualizar os pacotes"
	exit 1
fi
echo "Atualização dos pacotes realizada com sucesso"


#Instalação do java
apt install curl mlocate default-jdk

#Instalação do spark
echo "instalação do spark"
cd /opt/
if (find . "spark-3.3.2-bin-hadoop3.tgz") ; then
	echo "O arquivo spark-3.3.2-bin-hadoop3.tgz já existe"
else
	echo "Baixar arquivo spark-3.3.2-bin-hadoop3.tgz"
	wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
fi
rm -r /opt/spark
tar xvf /opt/spark-3.3.2-bin-hadoop3.tgz
mv /opt/spark-3.3.2-bin-hadoop3 /opt/spark

#Configurações personalizadas do Spark
echo "" >> $HOME/.bashrc 
echo "" >> $HOME/.bashrc
echo "export SPARK_HOME=/opt/spark" >> $HOME/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" >> $HOME/.bashrc
#Carregar configurações
source $HOME/.bashrc

#Pip e bibliotecas python necessárias
apt install python3-pip -y
pip install numpy
pip install pandas
pip install jupyter
pip install findspark
pip install pyarrow
pip install matplotlib
pip install seaborn
pip install plotly
pip install scipy

#Configurando o diretório do projeto e clonando repositório
apt install git
mkdir $HOME/projects
cd $HOME/projects
git clone https://github.com/FabioAguiar/pyspark-streaming-mqtt.git

reboot