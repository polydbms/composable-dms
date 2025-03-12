<script>
import axios from "axios";
import * as echarts from "echarts";
import SubstraitVisualizer from "./SubstraitVisualizer.vue";

export default {
  name: "CompoDB-results",
  components: {SubstraitVisualizer},
  data() {
    return {
      chartInstance: null,
      results: null,
      selectedQuery: null,
      imagePopupVisible: false,
      imageResults: [],
    };
  },
  computed: {
    availableQueries() {
      return this.results ? [...new Set(this.results.map(item => item.query_name))] : [];
    }
  },
  watch: {
    results(newResults) {
      // Reset selected query if the results change
      if (!newResults || newResults.length === 0) {
        this.selectedQuery = null;
      }
    }
  },
  mounted() {
    this.initChart();
  },
  methods: {
    initChart() {
      const chartDom = document.getElementById("bar-chart");
      this.chartInstance = echarts.init(chartDom);
      this.updateChart([]);
    },
    updateChart(results) {
      console.log("RESULTS:");
      console.log(results);
      this.results = results;
      this.clearChart();

      const combinations = results.reduce((acc, item) => {
        const combination = `${item.parser_name} + ${item.execution_engine_name}`;
        if (!acc[combination]) {
          acc[combination] = [];
        }
        acc[combination].push(item);
        return acc;
      }, {});

      const labels = [...new Set(results.map(item => item.query_name))];

      const series = [];
      Object.keys(combinations).forEach(combination => {
        const combinationData = combinations[combination];
        series.push({
          name: combination,
          type: 'bar',
          data: labels.map(label => {
            const matchingItem = combinationData.find(d => d.query_name === label && `${d.parser_name} + ${d.execution_engine_name}` === combination);
            return matchingItem ? matchingItem.runtime : 0;
          }),
          color: `rgba(${Math.random() * 255}, ${Math.random() * 255}, ${Math.random() * 255}, 0.9)`,
        });
      });

      const options = {
        tooltip: {
          trigger: 'axis',
          textStyle: {
            color: 'black',
          },
          backgroundColor: 'rgba(50, 50, 50, 0.8)',
        },
        legend: {// Move the legend slightly up
          textStyle: {
            color: 'black',
          },
          data: series.map(s => s.name),
        },
          grid: {// Push chart content further down so it doesn't overlap with the legend
          left: '10%',
          right: '10%',
          bottom: '15%', // Ensure space for the x-axis name
        },
        xAxis: {
          type: 'category',
          data: labels.length ? labels : [''],
          name: 'TPC-H Query',
          nameLocation: 'middle', // Center the name under the x-axis
          nameGap: 30, // Adjust spacing from axis
          axisLabel: {
            color: 'black',
          },
          axisLine: {
            lineStyle: {
              color: 'black',
            },
          },
          splitLine: {
            show: true, // Show grid lines
            lineStyle: {
              color: 'rgba(200, 200, 200, 0.5)', // Light gray grid
            },
          },
          show: true, // Ensure axis is visible even if no data
        },
        yAxis: {
          type: 'value',
          name: 'Runtime (ms)',
          nameLocation: 'middle', // Move name to the side, centered on the y-axis
          nameGap: 50, // Adjust spacing from axis
          axisLabel: {
            color: 'black',
          },
          axisLine: {
            lineStyle: {
              color: 'black',
            },
          },
          splitLine: {
            show: true, // Show grid lines
            lineStyle: {
              color: 'rgba(200, 200, 200, 0.5)', // Light gray grid
            },
          },
          show: true, // Ensure axis is visible even if no data
        },
        series,
      };
      this.chartInstance.setOption(options, true);
    },
    clearChart() {
      if (this.chartInstance) {
        this.chartInstance.clear();
      }
    },
    async fetchSubstraitPlans() {
      if (!this.selectedQuery) {
        alert("Please select a query first.");
        return;
      }
      const selectedPlans = this.results.filter(item => item.query_name === this.selectedQuery);
      console.log(selectedPlans)
      if (selectedPlans.length === 0) {
        alert("No query plan found for the selected query.");
        return;
      }

      const requestData = selectedPlans.map(q => ({
        query_name: q.query_name,
        parser_name: q.parser_name,  // Ensure the field matches FastAPI model
        query_plan: JSON.stringify(q.substrait_query),  // Parse string if it's JSON-encoded
      }));

      try {
        const response = await axios.post("http://localhost:8000/visualize-substrait", requestData)
          .then(response => {
            if (response && response.data && Array.isArray(response.data.images)) {
              console.log("Success:", response.data);  // Log the entire response for clarity
              if (response.data.images.length > 0) {
                this.imageResults = response.data.images.map(image => ({
                  query_name: image.query_name,
                  parser_name: image.parser_name,
                  image_url: `http://localhost:8000${image.image_url}`
                }));
                console.log("IMAGERESULTS")
                console.log(this.imageResults);
                this.imagePopupVisible = true;
              } else {
                alert("No images found.");
              }
            } else {
              alert("Failed to generate visualization or images are missing.");
            }
          })
          .catch(error => {
            console.error("Error fetching DAG:", error);  // Log the error for further inspection
            if (error.response) {
              console.error("API Response error:", error.response.data);
            } else if (error.request) {
              console.error("No response received:", error.request);
            } else {
              console.error("Error in setting up the request:", error.message);
            }
          });
      } catch (error) {
        console.error("Error fetching DAG:", error);
      }
    },
  },
};
</script>

<template>
  <div class="container">
    <h2>Performance Results</h2>
    <div id="bar-chart" style="width: 100%; height: 500px;"></div>

    <!-- Query Selection & Button -->
    <div class="query-selection">
      <label for="query-dropdown">Select Query for visualization:</label>
      <select id="query-dropdown" v-model="selectedQuery">
        <option v-for="query in availableQueries" :key="query" :value="query">
          {{ query }}
        </option>
      </select>
      <button @click="fetchSubstraitPlans" :disabled="!selectedQuery">Visualize Query Plans</button>
    </div>

    <!-- SubstraitPlanPopup Component -->
    <SubstraitVisualizer
      :visible="imagePopupVisible"
      :images="imageResults"
      @close="imagePopupVisible = false"
    />
  </div>
</template>

<style scoped>
h2 {
    margin-bottom: 20px;
    margin-top: 0;
}
label {
  margin: 0 0 0 190px;
}
#query-dropdown {
  width: 80px;
  margin: 10px 0 0 25px;
}
#bar-chart {
  display: block;
}
button {
  padding: 10px 25px;
  border-radius: 4px;
  font-size: 1em;
  background-color: green;
  color: white;
  border: none;
  cursor: pointer;
  float: right;
}
button:hover {
  background-color: darkgreen;
}
</style>