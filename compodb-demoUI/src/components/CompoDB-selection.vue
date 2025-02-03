<script>
export default {
  name: "CompoDB-selection",
  data() {
    return {
      parser: '',
      optimizer: [],
      tempOptimizers: [],
      executionEngine: '',
      queries: [],
      inputFormat: 'parquet',
      selectedQuerySet: 'tpch',
      query_set_tpch: [
        'tpch-q1', 'tpch-q2', 'tpch-q3', 'tpch-q4', 'tpch-q5', 'tpch-q6',
        'tpch-q7', 'tpch-q8', 'tpch-q9', 'tpch-q10', 'tpch-q11', 'tpch-q12',
        'tpch-q13', 'tpch-q14', 'tpch-q15', 'tpch-q16', 'tpch-q17', 'tpch-q18',
        'tpch-q19', 'tpch-q20', 'tpch-q21', 'tpch-q22'
      ],
      query_set_reduced_tpch: [
      'reduced_tpch_q1', 'reduced_tpch_q2', 'reduced_tpch_q3', 'reduced_tpch_q4',
      'reduced_tpch_q5', 'reduced_tpch_q6', 'reduced_tpch_q7', 'reduced_tpch_q8',
      'reduced_tpch_q9', 'reduced_tpch_q10', 'reduced_tpch_q11', 'reduced_tpch_q12',
      'reduced_tpch_q13', 'reduced_tpch_q14', 'reduced_tpch_q15', 'reduced_tpch_q16',
      'reduced_tpch_q17', 'reduced_tpch_q18', 'reduced_tpch_q19', 'reduced_tpch_q20',
      'reduced_tpch_q21', 'reduced_tpch_q22'
    ],
      selectAll: false,
    };
  },
  watch: {
    queries(newQueries) {
      // Emit the update-queries event when the queries array changes
      this.$emit("update-queries", newQueries);
    },
    selectedQuerySet(newValue) {
      this.queries = []; // Reset queries when changing sets
      this.selectAll = false;
      this.$emit("update-queries", this.queries);
    }
  },
  computed: {
    currentQuerySet() {
      return this.selectedQuerySet === 'tpch' ? this.query_set_tpch : this.query_set_reduced_tpch;
    }
  },
  methods: {
    addOptimizer() {
      this.tempOptimizers.push('');
    },
    removeOptimizer(index) {
      this.tempOptimizers.splice(index, 1);
    },
    async submitForm(event) {
      this.$emit("show-loading");
      event.preventDefault();

      const formData = {
        parser: this.parser,
        optimizer: [...this.tempOptimizers],
        executionEngine: this.executionEngine,
      };

      try {
        const response = await fetch('http://localhost:8000/new-compodb', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(formData),
        });

        if (response.ok) {
          const data = await response.json();
          this.$emit("new-composition", formData.parser, formData.optimizer, formData.executionEngine);
        } else {
          console.error('Error submitting form:', response.statusText);
        }
      } catch (error) {
        console.error('Network error:', error);
      }
      finally {
        this.$emit("hide-loading");
      }
    },

    resetState() {
      this.parser = '';
      this.optimizer = [];
      this.executionEngine = '';
      this.queries = [];
      this.inputFormat = 'parquet';
      this.selectAll = false;

      const checkboxes = document.querySelectorAll('.query-options input[type="checkbox"]');
      checkboxes.forEach(checkbox => checkbox.checked = false);

      const inputFormatSelect = document.getElementById('input-format');
      if (inputFormatSelect) {
        inputFormatSelect.value = '';
      }
      this.$emit("update-queries", []);
      this.$emit("update-input-format", '');
    },
    toggleAll() {
      this.selectAll = !this.selectAll;
      this.queries = this.selectAll ? [...this.currentQuerySet] : [];
      this.$emit("update-queries", this.queries);
    },
    switchQuerySet() {
      this.queries = [];  // Reset selected queries when switching sets
      this.selectAll = false;
      this.$emit("update-queries", this.queries);
    },
    handleInputFormatChange() {
      this.$emit("update-input-format", this.inputFormat);
    },
  },
};
</script>

<template>
  <div class="container">
    <h2>Add System Composition</h2>
    <form @submit.prevent="submitForm">

      <!-- Parser Selection -->
      <label for="parser">Select Parser</label>
      <select id="parser" v-model="parser" required>
          <option value="Calcite">Calcite</option>
          <option value="DataFusion">DataFusion</option>
          <option value="DuckDB">DuckDB</option>
          <option value="Ibis">Ibis</option>
      </select>

      <!-- Optimizer Selection -->
      <div style="display: flex; align-items: center; gap: 11px;">
        <label>Add Optimizer</label>
        <button type="button" @click="addOptimizer" id="plus">
          +
        </button>
      </div>

      <div v-for="(opt, index) in tempOptimizers" :key="index" style="display: flex; align-items: center; gap: 3px;">
        <select v-model="tempOptimizers[index]" required>
          <option value="Calcite">Calcite Optimizer</option>
          <option value="DataFusion">DataFusion Optimizer</option>
          <option value="DuckDB">DuckDB Optimizer</option>
        </select>
        <button type="button" @click="removeOptimizer(index)" id="minus">
          -
        </button>
      </div>

      <!-- Execution Engine Selection -->
      <label for="execution-engine">Select Execution Engine</label>
      <select id="execution-engine" v-model="executionEngine" required>
          <option value="DuckDB">DuckDB Engine</option>
          <option value="DataFusion">DataFusion Engine</option>
          <option value="Acero">Acero Engine</option>
      </select>

      <!-- Submit Button -->
      <button type="submit">Add System</button>
    </form>

    <!-- Query Selection -->
    <div class="query-selection">
        <label id="selection-label">Query Selection</label>
        <select id="query-set" v-model="selectedQuerySet">
          <option value="tpch">TPC-H</option>
          <option value="reduced_tpch">Reduced TPC-H</option>
        </select>
        <button @click="toggleAll" class="select-all-btn">{{ selectAll ? 'Deselect All' : 'Select All' }}</button>
    </div>
    <!-- Query Options -->
    <div class="query-options">
      <div v-for="query in currentQuerySet" :key="query">
        <input type="checkbox" :id="query" :value="query" v-model="queries" />
        <label :for="query">{{ query.replace('tpch-q', 'TPC-H Q').replace('reduced_tpch_q', 'Reduced TPC-H Q') }}</label>
      </div>
    </div>

    <!-- Input Format -->
    <label for="input-format">Input Format</label>
    <select id="input-format" v-model="inputFormat" @change="handleInputFormatChange" required>
        <option value="parquet">Parquet</option>
        <option value="csv">CSV</option>
    </select>
  </div>
</template>

<style scoped>
h2 {
    margin-bottom: 20px;
    margin-top: 0;
}
label {
    display: block;
    margin-top: 25px;
    margin-bottom: 5px;
    font-size: 1.1em;
}
#plus {
  width: 23px;
  height: 23px;
  padding: 0 0 3px 0;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 18px;
  border-radius: 6px;
  margin-top: 20px;
  margin-bottom: 0px;
  background-color: darkgrey;
}
#plus:hover {
  background-color: grey;
}
#minus {
  width: 39px;
  height: 39px;
  padding: 0 0 4px 0;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 25px;
  font-stretch: expanded;
  border-radius: 4px;
  background-color: darkgrey;
  margin-top: 6px;
  margin-bottom: 1px;
  border: 1px solid dimgrey;
}
#minus:hover {
  background-color: grey;
}
button {
    width: 100%;
    padding: 10px;
    font-size: 1.1em;
    background-color: #007BFF;
    color: white;
    border: none;
    cursor: pointer;
    margin-top: 25px;
    border-radius: 4px;
}
button:hover {
    background-color: #0056b3;
}
#query-set {
  width: 115px;
  height: 25px;
  font-size: 0.7em;
  padding: 0 0 1px 5px;
  margin: 30px 0 0 40px;
}
select {
    width: 100%;
    padding: 8px;
    margin-top: 5px;
    font-size: 16px;
}
.query-options {
    padding: 5px 8px;
    border: 1px solid #ddd;
    border-radius: 4px;
    background-color: rgba(241, 244, 246, 0.8);
    max-height: 187px;
    overflow-y: auto;
}
.query-options label {
    display: block;
    font-size: 1em;
    padding: 5px 10px;
    margin: 3px 0;
    border-radius: 4px;
    cursor: pointer;
    transition: background-color 0.3s ease;
}
.query-options label:hover {
    background-color: darkgrey;
}
.query-options input[type="checkbox"] {
    display: none;
}
.query-options input[type="checkbox"]:checked + label {
    background-color: dimgrey;
    color: white;
}
.query-selection {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 4px;
}
#selection-label {
  margin-top: 30px;
  margin-bottom: 0;
}
.select-all-btn {
  background-color: darkgrey;
  color: white;
  font-size: smaller;
  border: none;
  padding: 4px 12px;
  cursor: pointer;
  border-radius: 4px;
  transition: background-color 0.2s;
  width: 100px;
  height: 25px;
  margin-top: 35px;
  margin-bottom: 5px;
}
.select-all-btn:hover {
  background-color: grey;
}
</style>
