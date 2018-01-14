while true
do
    $SPARK_HOME/bin/spark-submit --packages datastax:spark-cassandra-connector:2.0.6-s_2.11 --conf spark.cassandra.connection.host='172.17.0.2' --master local[2] /home/jovyan/work/spark_event_enhance_job.py
    sleep 2
done