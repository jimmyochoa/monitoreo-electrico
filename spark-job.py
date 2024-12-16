from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, from_json, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import asyncio
import websockets

async def send_to_websocket(uri, message):
    async with websockets.connect(uri) as websocket:
        await websocket.send(message)

try:
    # Crear sesión de Spark con el conector de Kafka incluido
    spark = SparkSession.builder \
        .appName("Kafka Spark Streaming") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.master", "local[1]") \
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

    # Calcular el promedio de consumo, latitud y longitud por ciudad, en ventanas de 1 minuto
    df_windowed = df.withWatermark("timestamp", "1 minute") \
        .groupBy(window(col("timestamp"), "1 minute"), col("city")) \
        .agg(
            avg("consumption_kWh").alias("avg_consumption"),
            avg("location.lat").alias("avg_lat"),  # Promedio de latitud
            avg("location.lon").alias("avg_lon")  # Promedio de longitud
        )

    # Descomponer la ventana en columnas `start` y `end`
    df_windowed = df_windowed.withColumn("start", col("window.start")) \
                             .withColumn("end", col("window.end")) \
                             .drop("window")

    # Convertir la columna `start` al formato deseado para simplificar el texto
    df_windowed = df_windowed.withColumn("start", date_format(col("start"), "yyyy-MM-dd HH:mm:ss"))

    print("Processing the data with windowing and averaging...")

    # Detectar picos de consumo (valores atípicos) – Definimos un umbral arbitrario
    threshold = 0.1  # Umbral reducido para ver resultados
    df_picos = df_windowed.filter(df_windowed.avg_consumption > threshold)

    print(f"Detecting consumption spikes above {threshold}...")

    # Formatear los resultados en una sola columna para enviar al WebSocket
    df_formatted = df_picos.selectExpr(
        "concat(start, ', ', city, ', ', cast(avg_consumption as string), ', ', cast(avg_lat as string), ', ', cast(avg_lon as string)) as value"
    )

    async def process_row_and_send(batch_df, epoch_id):
        rows = batch_df.collect()
        for row in rows:
            message = row["value"]
            await send_to_websocket("ws://websocket-server:6789", message)

    query = df_formatted.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda batch_df, epoch_id: asyncio.run(process_row_and_send(batch_df, epoch_id))) \
        .start()

    print("Sending results to WebSocket...")

    # Esperar hasta que termine el procesamiento
    query.awaitTermination()

    print("Job completed successfully.")

except Exception as e:
    print(f"An error occurred: {e}")