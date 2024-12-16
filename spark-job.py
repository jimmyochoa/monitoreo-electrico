from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, from_json, to_timestamp, date_format, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType

try:
    # Crear sesión de Spark con el conector de Kafka incluido
    spark = SparkSession.builder \
        .appName("Kafka Spark Streaming") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.master", "local[1]") \
        .config("spark.sql.warehouse.dir", "hdfs://hadoop-namenode:9000/user/hive/warehouse") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()

    print("Spark session created successfully.")

    # Esquema de los datos que se van a recibir de Kafka
    schema = StructType([
        StructField("timestamp", StringType(), True),  # Inicialmente como STRING
        StructField("consumption_kWh", FloatType(), True),
        StructField("location", StructType([
            StructField("lat", FloatType(), True),
            StructField("lon", FloatType(), True),
        ]), True),
        StructField("meter_id", StringType(), True),
        StructField("city", StringType(), True),
    ])

    # Leer los datos de Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "consumo_samborondon,consumo_daule") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    print("Reading data from Kafka...")

    # Decodificar los valores de Kafka
    df = df.selectExpr("CAST(value AS STRING)")

    # Convertir de JSON a dataframe usando el esquema
    df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    # Convertir la columna `timestamp` de STRING a TIMESTAMP
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    # Calcular el promedio de consumo por ciudad, en ventanas de 1 minuto
    df_windowed = df.withWatermark("timestamp", "1 minute") \
        .groupBy(window(col("timestamp"), "1 minute"), col("city")) \
        .agg(avg("consumption_kWh").alias("avg_consumption"))

    # Descomponer la ventana en columnas `start` y `end`
    df_windowed = df_windowed.withColumn("start", col("window.start")) \
                             .withColumn("end", col("window.end")) \
                             .drop("window")

    # Convertir la columna `start` al formato deseado para simplificar el texto
    df_windowed = df_windowed.withColumn("start", date_format(col("start"), "yyyy-MM-dd HH:mm:ss"))

    print("Processing the data with windowing and averaging...")

    # Detectar picos de consumo (valores atípicos) – Definimos un umbral arbitrario
    threshold = 0.1  # Umbral reducido para ver resultados

    # Crear la columna 'is_peak' basada en el umbral
    df_windowed = df_windowed.withColumn(
        "is_peak", 
        when(col("avg_consumption") > threshold, True).otherwise(False)
    )

    print(f"Detecting consumption spikes above {threshold}...")

    # Formatear los resultados en una sola columna para exportar como texto
    df_formatted = df_windowed.selectExpr(
        "concat(start, ', ', city, ', ', cast(avg_consumption as string), ', ', cast(is_peak as string)) as value"
    )

    # Escribir los resultados en HDFS como archivo de texto único
    query = df_formatted.coalesce(1).writeStream \
        .outputMode("append") \
        .format("text") \
        .option("path", "hdfs://hadoop-namenode:9000/user/spark/output") \
        .option("checkpointLocation", "hdfs://hadoop-namenode:9000/tmp/spark-checkpoints") \
        .start()

    print("Writing results to HDFS...")

    # Esperar hasta que termine el procesamiento
    query.awaitTermination()  # Esto se asegura de que el proceso siga en ejecución.

    print("Job completed successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
