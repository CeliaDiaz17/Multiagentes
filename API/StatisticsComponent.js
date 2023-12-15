import React, { useEffect, useState } from 'react';

const StatisticsComponent = () => {
    const [statistics, setStatistics] = useState([]);

    useEffect(() => {
        fetch('http://localhost:8000')
            .then((response) => response.json())
            .then((data) => setStatistics(data))
            .catch((error) => console.error(error));
    }, []);

    return (
        <div>
            <h1>Statistics</h1>
            <ul>
                {statistics.map((statistic) => (
                    <li key={statistic.id}>
                        {statistic.name} - {statistic.value}
                    </li>
                ))}
            </ul>
        </div>
    );
};
export default StatisticsComponent;