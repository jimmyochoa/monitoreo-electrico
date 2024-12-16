import time
import random
import json
from datetime import datetime
from kafka import KafkaProducer

# Crear el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Serializa el objeto a JSON y luego a bytes
)

# Función para generar datos simulados de consumo
def generate_consumption_data(meter_id):
    # Definir coordenadas de Samborondon y Daule
    locations = [
        {"city": "Samborondon", "lat": -2.205, "lon": -79.948},
        {"city": "Daule", "lat": -2.287, "lon": -79.679}
    ]
    
    # Elegir una ciudad aleatoria
    location = random.choice(locations)
    
    # Crear un timestamp actual
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    # Simular un consumo eléctrico con valores variados
    consumption = round(random.uniform(1.5, 5.0), 2)  # Asegúrate de que el consumo sea más alto para evitar que se filtre

    # Aumentar la probabilidad de picos para pruebas
    if random.random() > 0.8:  # 20% de probabilidad de generar un consumo de pico
        consumption = round(random.uniform(5.0, 8.0), 2)  # Picos de consumo mayores

    # Crear el mensaje con la información simulada
    message = {
        "timestamp": timestamp,
        "consumption_kWh": consumption,
        "meter_id": meter_id,
        "location": location,
        "city": location["city"]
    }
    
    return message

# Función para enviar datos a Kafka
def send_to_kafka():
    meter_id = 1  # Puedes cambiar este valor o hacerlo dinámico si tienes múltiples medidores
    
    while True:
        # Generar un mensaje simulado
        message = generate_consumption_data(meter_id)
        
        # Enviar el mensaje al tema correspondiente dependiendo de la ciudad
        producer.send(f'consumo_{message["city"].lower()}', value=message)  # 'message' se convierte a bytes automáticamente
        producer.flush()  # Asegúrate de que los mensajes sean enviados
        
        print(f"Mensaje enviado: {message}")  # Imprimir para ver lo que se está enviando
        
        time.sleep(1)  # Espera 1 segundo antes de enviar el siguiente mensaje

# Llamar a la función para enviar datos en tiempo real a Kafka
send_to_kafka()
