<script>
import * as echarts from "echarts";

export default {
  name: "CompoDB-results",
  data() {
    return {
      chartInstance: null,
    };
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
        title: {
          text: 'Benchmark Results (Runtime in ms)',
          left: 'center',
          textStyle: {
            color: 'black',
          },
        },
        tooltip: {
          trigger: 'axis',
          textStyle: {
            color: 'black',
          },
          backgroundColor: 'rgba(50, 50, 50, 0.8)',
        },
        legend: {
          top: '10%',
          textStyle: {
            color: 'black',
          },
          data: series.map(s => s.name),
        },
        xAxis: {
          type: 'category',
          data: labels,
          axisLabel: {
            color: 'black',
          },
          axisLine: {
            lineStyle: {
              color: 'black',
            },
          },
        },
        yAxis: {
          type: 'value',
          axisLabel: {
            color: 'black',
          },
          axisLine: {
            lineStyle: {
              color: 'black',
            },
          },
          splitLine: {
            lineStyle: {
              color: 'rgba(255, 255, 255, 0.2)',
            },
          },
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
    showSubstraitPlans() {
      // TODO
    },

  },
};
</script>

<template>
  <div class="container">
    <h2>Performance Results</h2>
    <div id="bar-chart" style="width: 100%; height: 500px;"></div>
    <button @click="showSubstraitPlans">Visualize Query Plans</button>
  </div>
</template>

<style scoped>
h2 {
    margin-bottom: 20px;
    margin-top: 0;
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