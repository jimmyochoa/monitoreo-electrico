# sistema-electrico
Sistema de Monitoreo de Consumo Eléctrico en Tiempo Real

## Descripción
Este proyecto tiene como objetivo desarrollar un sistema de monitoreo de consumo eléctrico en tiempo real, el cual permita visualizar el consumo de energía eléctrica de un hogar o empresa en tiempo real, a través de una interfaz web.

## Integrantes
- Jimmy Ochoa
- Jose Balda

## Tecnologías
- [Hadoop](https://hadoop.apache.org/)
- [Vite](https://vitejs.dev/)
- [Docker](https://www.docker.com/)
- [Kafka](https://kafka.apache.org/)
- [Spark](https://spark.apache.org/)
- [Python](https://www.python.org/)

## Requisitos
- Docker
- Docker Compose
- Node.js
- NPM

## Instalación
1. Clonar el repositorio
```bash
git clone https://github.com/jimmyochoa/monitoreo-electrico.git
```

2. Ingresar al directorio del proyecto
```bash
cd monitoreo-electrico
```

3. Iniciar los servicios
```bash
docker-compose up
```

4. Verificar que los servicios estén corriendo
```bash
docker-compose ps
```
5. Iniciar el frontend
```bash
cd dashboard/dashboard_vite
npm install
npm run dev
```
6. Acceder a la interfaz web
```
http://localhost:5173/
```
