import asyncio
import websockets
from websocket import create_connection

# A set to keep track of all connected clients (WebSocket connections)
clients = set()
async def hello_world(websocket):
    print("New client connected!")
    clients.add(websocket)
    try:
        async for message in websocket:
            print(f"Received message from client: {message}")
            # Send message to all connected clients
            await send_data_to_clients(message)
    except websockets.exceptions.ConnectionClosed as e:
        print(f"Client disconnected: {e}")
    finally:
        clients.remove(websocket)
        print("Client disconnected!")
# Function to send data from Spark to WebSocket clients
async def send_data_to_clients(message):
    # Broadcast the message to all connected clients
    for client in clients:
        try:
            await client.send(message)
            print(f"Sent to client: {message}")
        except websockets.exceptions.ConnectionClosed as e:
            print(f"Error sending message to client: {e}")

async def main():
    # Start the WebSocket server
    async with websockets.serve(hello_world, "0.0.0.0", 6789):  # Bind to all IPs for Docker
        print("WebSocket server started on ws://0.0.0.0:6789")
        await asyncio.Future()  # Keep server running until manually stopped

asyncio.run(main())