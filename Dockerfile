# Usar imagen de Python base
FROM python:3.8-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar el archivo requirements.txt al contenedor
COPY requirements.txt /app/requirements.txt

# Instalar las dependencias desde requirements.txt
RUN pip install -r requirements.txt

# Copiar el código del productor
COPY producer.py /app/producer.py

# Comando para ejecutar el productor
CMD ["python", "producer.py"]
