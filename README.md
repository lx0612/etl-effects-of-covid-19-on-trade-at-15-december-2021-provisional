# etl-effects-of-covid-19-on-trade-at-15-december-2021-provisional

file spark-lamdo used to build and config cluster apche spark with one master and two worker

the version of spark : 3.2.1

file include two file apps and data

Job.py file in /apps is written based on pyspark for the purpose of ETL (extract, transform and load data) into Postgresql.

file csv and jar in /data


#To run:

          docker build -t cluster-apache-spark:3.2.1 .


          docker compose up -d


#To submit the app connect to one of the workers or the master and execute:

          /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
          --jars /opt/spark-data/postgresql-42.2.22.jar \
          --driver-memory 1G \
          --executor-memory 1G \
          /opt/spark-apps/Job.py
          
         
#submit
![spark_3](https://user-images.githubusercontent.com/100019383/174433810-b5a33410-3b1f-4a97-a63d-5d3eb0e7bb45.png)


#run
![spark_4](https://user-images.githubusercontent.com/100019383/174433801-329f51fc-5cf8-40f5-98cf-42266e40b9f6.png)


#succes
![spark_5](https://user-images.githubusercontent.com/100019383/174433815-89c24c45-1408-4727-baf9-285c4761b4e4.png)






Check the completion time of spark-cluster.

