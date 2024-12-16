import React, { useEffect, useState, useRef } from 'react';
import { Line } from 'react-chartjs-2';
import Alerts from './Alerts';
import AverageConsumption from './AvarageConsumption';
import { Chart as ChartJS, Title, Tooltip, Legend, LineElement, CategoryScale, LinearScale, PointElement } from 'chart.js';
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';

// Fix marker icon issue in Leaflet
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
});

ChartJS.register(Title, Tooltip, Legend, LineElement, CategoryScale, LinearScale, PointElement);

const Dashboard = () => {
  const [messages, setMessages] = useState([]);
  const [dauleData, setDauleData] = useState([]);
  const [samborondonData, setSamborondonData] = useState([]);
  const [timeStamps, setTimeStamps] = useState([]);
  const [markers, setMarkers] = useState([]); // Back to tracking all markers dynamically

  const chartRef = useRef(null);

  // WebSocket setup
  useEffect(() => {
    const socket = new WebSocket('ws://localhost:6789');

    socket.onopen = () => {
      console.log('Connected to WebSocket');
    };

    socket.onmessage = (event) => {
      console.log('Message received:', event.data);

      // Process the incoming message
      const message = event.data.split(', ');
      const timestamp = message[0];
      const city = message[1];
      const consumption = parseFloat(message[2]);
      const lat = parseFloat(message[3]); // Latitude
      const lon = parseFloat(message[4]); // Longitude

      // Add a new marker for the city
      setMarkers((prevMarkers) => [
        ...prevMarkers,
        { city, lat, lon, timestamp, consumption },
      ]);

      // Update state with incoming message
      setMessages((prevMessages) => [...prevMessages, event.data]);

      if (city === 'Daule') {
        setDauleData((prevData) => [...prevData, consumption]);
      } else if (city === 'Samborondon') {
        setSamborondonData((prevData) => [...prevData, consumption]);
      }

      // Update timestamps for X-axis
      setTimeStamps((prevTimestamps) => [...prevTimestamps, timestamp]);
    };

    socket.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

    socket.onclose = (event) => {
      console.log('WebSocket closed:', event);
    };

    return () => {
      socket.close();
    };
  }, []);

  // Prepare the data for the chart
  const chartData = {
    labels: timeStamps,
    datasets: [
      {
        label: 'Daule',
        data: dauleData,
        fill: false,
        borderColor: 'rgb(75, 192, 192)',
        tension: 0.1,
      },
      {
        label: 'Samborondon',
        data: samborondonData,
        fill: false,
        borderColor: 'rgb(255, 99, 132)',
        tension: 0.1,
      },
    ],
  };

  return (
    <div className="dashboard-container">
      <div className="chart-container">
        <Alerts data={markers} threshold={3}></Alerts>
        <Line ref={chartRef} data={chartData} />
        <div className="label-container">
          <AverageConsumption data={dauleData} title={'Daule'}></AverageConsumption>
          <AverageConsumption
            data={samborondonData}
            title={'Samborondon'}
          ></AverageConsumption>
        </div>
      </div>
      <div className="map-container">
        <MapContainer
          center={[-1.9, -79.95]} // Initial center
          zoom={11}
          style={{ height: '500px', width: '100%' }}
        >
          <TileLayer
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            attribution='&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
          />
          {markers.map((marker, index) => (
            <Marker key={index} position={[marker.lat, marker.lon]}>
              <Popup>
                <h4>{marker.city}</h4>
                <p>Timestamp: {marker.timestamp}</p>
                <p>Consumption: {marker.consumption} kWh</p>
              </Popup>
            </Marker>
          ))}
        </MapContainer>
      </div>
    </div>
  );
};

export default Dashboard;
