import React, { useEffect, useState } from 'react';

const AverageConsumption = ({ data, title }) => {
const [avg, setAvg] = useState(0);
useEffect(()=>{
    if(data.length > 0)
    setAvg( data.reduce((acc, curr) => acc + curr, 0) / data.length)
},[data])
  return (
    <div>
      <h3>Consumo Promedio {title}</h3>
      <p>{avg.toFixed(2)} kWh</p>
    </div>
  );
};

export default AverageConsumption;