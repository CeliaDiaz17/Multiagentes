<template>
    <div>
        <h1>Interfaz API</h1>
        <select v-model="endpointSeleccionado" @change="cargarDatos">
            <option disabled value="">Seleccione un Endpoint</option>
            <option value="mortalidad">Estadísticas de Mortalidad</option>
            <option value="estadisticas_genero">Estadísticas de Género</option>
            <option value="rates/unemployment_data">Tasas de Suicidio y Desempleo</option>
            <option value="rates/age_mean">Unemployment_data stats</option>
            <!-- Agrega más opciones si hay más endpoints -->
        </select>
        <div v-if="datosCargados && chartData">
            <h2>Resultados:</h2>
            <BarChart :chart-data="chartData" :options="chartOptions" />
        </div>
    </div>
</template>

<script>
import axios from 'axios';
import BarChart from './BarChart.vue';

export default {
    components: {
        BarChart
    },
    data() {
        return {
            endpointSeleccionado: '',
            datosAPI: [],
            datosCargados: false,
            chartData: null,
            chartOptions: {
                responsive: true,
                maintainAspectRatio: false
            }
        };
    },
    methods: {
        async cargarDatos() {
            if (!this.endpointSeleccionado) return;

            try {
                const response = await axios.get(`http://127.0.0.1:8000/${this.endpointSeleccionado}`);
                console.log("RESPUESTA", response.data)
                this.datosAPI = response.data;
                this.chartData = this.formatChartData(this.datosAPI);
                this.datosCargados = true;
            } catch (error) {
                console.error('Error al cargar datos:', error);
            }
        },
        formatChartData(data) {
            if (!Array.isArray(data.age_means_by_year)) {
                console.log('Los datos no están en el formato esperado:', data);
                console.error('Los datos no están en el formato esperado:', data);
                return;
            }

            return {
                labels: data.age_means_by_year.map(item => item.year),
                datasets: [{
                    label: 'Edad Media',
                    backgroundColor: '#42A5F5',
                    data: data.age_means_by_year.map(item => item.mean_age)
                }]
            };
        }
    }
};
</script>