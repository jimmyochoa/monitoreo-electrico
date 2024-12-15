from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import subprocess
import json

def check_hdfs_connection(hdfs_url="hdfs://namenode:9000"):
    """Función para verificar la conectividad con HDFS"""
    try:
        # Ejecutar el comando `hdfs dfs -ls /` para verificar si HDFS está accesible
        subprocess.check_call(['hdfs', 'dfs', '-ls', hdfs_url])
        print(f"Conexión con HDFS en {hdfs_url} exitosa.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error al intentar acceder a HDFS: {e}")
        return False

try:
    # Verificar conexión con HDFS
    if not check_hdfs_connection():
        print("No se pudo establecer conexión con HDFS. Abortando el job.")
    else:
        # Crear sesión de Spark
        spark = SparkSession.builder \
            .appName("ConsumoElectricoJob") \
            .config("spark.master", "spark://spark-master:7077") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .getOrCreate()

        print("Spark session created successfully.")

        # Esquema de los datos que se van a recibir de Kafka
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("consumption_kWh", FloatType(), True),
            StructField("location", StructType([
                StructField("lat", FloatType(), True),
                StructField("lon", FloatType(), True),
            ]), True),
            StructField("meter_id", StringType(), True),
            StructField("city", StringType(), True),
        ])

        # Leer datos de Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "consumo_samborondon,consumo_daule") \
            .load()

        print("Reading data from Kafka...")

        # Decodificar los valores de Kafka
        df = df.selectExpr("CAST(value AS STRING)")

        # Convertir de JSON a dataframe usando el esquema
        df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

        # Calcular el promedio de consumo por ciudad, en ventanas de 1 minuto
        df_windowed = df.withWatermark("timestamp", "1 minute") \
            .groupBy(window(col("timestamp"), "1 minute"), col("city")) \
            .agg(avg("consumption_kWh").alias("avg_consumption"))

        print("Processing the data with windowing and averaging...")

        # Detectar picos de consumo (valores atípicos) – Definimos un umbral arbitrario
        threshold = 4.0
        df_picos = df_windowed.filter(df_windowed.avg_consumption > threshold)

        print(f"Detecting consumption spikes above {threshold}...")

        # Escribir los resultados en HDFS
        query = df_picos.writeStream \
            .outputMode("complete") \
            .format("parquet") \
            .option("checkpointLocation", "/tmp/spark-checkpoints") \
            .option("path", "hdfs://namenode:9000/results") \
            .start()

        print("Writing results to HDFS...")

        # Esperar hasta que termine el procesamiento
        query.awaitTermination()
        print("Job completed successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
