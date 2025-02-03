<script>
export default {
  name: "CompoDB-overview",
  data() {
    return {
      compoDBs: [],
      queries: [],
      inputFormat: '',
    };
  },
  methods: {
    addCompoDB(parser, optimizer, executionEngine) {
      const newCompoDB = {
        parser: parser,
        optimizer: optimizer,
        executionEngine: executionEngine
      };
      this.compoDBs.push(newCompoDB);
    },
    async clear() {
      this.compoDBs = [];
      this.queries = [];
      this.inputFormat = '';
      try {
        const response = await fetch('http://localhost:8000/clear', {
          method: 'POST',
        });
        if (response.ok) {
          const data = await response.json();
          console.log('Clear result:', data);
        } else {
          console.error('Error clearing data:', response.statusText);
        }
      } catch (error) {
        console.error('Network error:', error);
      } finally {
        this.$emit('reset-selection');
      }
    },
    updateQueries(newQueries) {
      this.queries = newQueries;
    },
    updateInputFormat(newInputFormat) {
      this.inputFormat = newInputFormat;
    },
    async runBenchmark() {
      this.$emit("show-loading");
      const requestData = {
        compoDBs: this.compoDBs,
        queries: this.queries,
        inputFormat: this.inputFormat,
      };
      console.log("BENCHMARK")
      console.log(requestData)
      try {
        const response = await fetch('http://localhost:8000/run-benchmark', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(requestData),
        });
        if (response.ok) {
          const data = await response.json();
          console.log('Benchmark result:', data);
          this.$emit('benchmark-update', data.data);
        } else {
          console.error('Error running benchmark:', response.statusText);
        }
      } catch (error) {
        console.error('Network error:', error);
      }
      finally {
        this.$emit("hide-loading");
      }
    },
  }
}
</script>

<template>
<div class="container">
    <h2>Selected Compositions</h2>
    <div v-for="(compoDB, index) in compoDBs" :key="index" class="composition">
      <strong>Parser:</strong> {{ compoDB.parser }}<br>
      <span v-if="compoDB.optimizer && compoDB.optimizer.length">
        <strong>Optimizer:</strong> {{ compoDB.optimizer.join(', ') }}<br>
      </span>
      <strong>Execution Engine:</strong> {{ compoDB.executionEngine }}
    </div>
    <div class="buttons">
        <button @click="runBenchmark" class="button-primary">Run Benchmark</button>
        <button @click="clear" class="button-secondary">Clear Benchmark</button>
    </div>
</div>
</template>

<style scoped>
h2 {
    margin-bottom: 20px;
    margin-top: 0;
}
.composition {
    border: 1px solid #ddd;
    border-radius: 8px;
    padding: 15px;
    margin-bottom: 15px;
    background-color: rgba(241, 244, 246, 0.8);
}
.composition strong {
    margin-bottom: 5px;
}
.buttons {
  margin-top: 30px;
  display: flex;
  flex-direction: column;
  gap: 10px; /* Adds spacing between buttons */
  width: 100%;
}

.buttons button {
  width: 100%; /* Makes buttons span the full width */
  padding: 12px; /* Adds some padding for better appearance */
  text-align: center;
}
.button-primary {
    background-color: #007bff;
    color: white;
    padding: 10px 25px;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-size: 1em;
}
.button-primary:hover {
    background-color: #0056b3;
}
.button-secondary {
    background-color: darkgrey;
    color: white;
    padding: 10px 25px;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-size: 1em;
}
.button-secondary:hover {
    background-color: grey;
}
</style>