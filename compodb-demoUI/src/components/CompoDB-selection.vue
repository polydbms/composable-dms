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
      query_set_tpcds: [
        'tpcds-query1', 'tpcds-query2', 'tpcds-query3', 'tpcds-query4', 'tpcds-query5', 'tpcds-query6', 'tpcds-query7', 'tpcds-query8', 'tpcds-query9', 'tpcds-query10',
        'tpcds-query11', 'tpcds-query12', 'tpcds-query13', 'tpcds-query14', 'tpcds-query15', 'tpcds-query16', 'tpcds-query17', 'tpcds-query18', 'tpcds-query19', 'tpcds-query20',
        'tpcds-query21', 'tpcds-query22', 'tpcds-query23', 'tpcds-query24', 'tpcds-query25', 'tpcds-query26', 'tpcds-query27', 'tpcds-query28', 'tpcds-query29', 'tpcds-query30',
        'tpcds-query31', 'tpcds-query32', 'tpcds-query33', 'tpcds-query34', 'tpcds-query35', 'tpcds-query36', 'tpcds-query37', 'tpcds-query38', 'tpcds-query39', 'tpcds-query40',
        'tpcds-query41', 'tpcds-query42', 'tpcds-query43', 'tpcds-query44', 'tpcds-query45', 'tpcds-query46', 'tpcds-query47', 'tpcds-query48', 'tpcds-query49', 'tpcds-query50',
        'tpcds-query51', 'tpcds-query52', 'tpcds-query53', 'tpcds-query54', 'tpcds-query55', 'tpcds-query56', 'tpcds-query57', 'tpcds-query58', 'tpcds-query59', 'tpcds-query60',
        'tpcds-query61', 'tpcds-query62', 'tpcds-query63', 'tpcds-query64', 'tpcds-query65', 'tpcds-query66', 'tpcds-query67', 'tpcds-query68', 'tpcds-query69', 'tpcds-query70',
        'tpcds-query71', 'tpcds-query72', 'tpcds-query73', 'tpcds-query74', 'tpcds-query75', 'tpcds-query76', 'tpcds-query77', 'tpcds-query78', 'tpcds-query79', 'tpcds-query80',
        'tpcds-query81', 'tpcds-query82', 'tpcds-query83', 'tpcds-query84', 'tpcds-query85', 'tpcds-query86', 'tpcds-query87', 'tpcds-query88', 'tpcds-query89', 'tpcds-query90',
        'tpcds-query91', 'tpcds-query92', 'tpcds-query93', 'tpcds-query94', 'tpcds-query95', 'tpcds-query96', 'tpcds-query97', 'tpcds-query98', 'tpcds-query99'
      ],
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
      query_set_job: [
        'j-o-b_1a', 'j-o-b_1b', 'j-o-b_1c', 'j-o-b_1d', 'j-o-b_2a', 'j-o-b_2b', 'j-o-b_2c', 'j-o-b_2d', 'j-o-b_3a',
        'j-o-b_3b', 'j-o-b_3c', 'j-o-b_4a', 'j-o-b_4b', 'j-o-b_4c', 'j-o-b_5a', 'j-o-b_5b', 'j-o-b_5c', 'j-o-b_6a',
        'j-o-b_6b', 'j-o-b_6c', 'j-o-b_6d', 'j-o-b_6e', 'j-o-b_6f', 'j-o-b_7a', 'j-o-b_7b', 'j-o-b_7c', 'j-o-b_8a',
        'j-o-b_8b', 'j-o-b_8c', 'j-o-b_8d', 'j-o-b_10a', 'j-o-b_10b', 'j-o-b_10c', 'j-o-b_11a', 'j-o-b_11b', 'j-o-b_11c',
        'j-o-b_11d', 'j-o-b_12a', 'j-o-b_12b', 'j-o-b_12c', 'j-o-b_13a', 'j-o-b_13b', 'j-o-b_13c', 'j-o-b_13d',
        'j-o-b_14a', 'j-o-b_14b', 'j-o-b_14c', 'j-o-b_15a', 'j-o-b_15b', 'j-o-b_15c', 'j-o-b_15d', 'j-o-b_16a',
        'j-o-b_16b', 'j-o-b_16c', 'j-o-b_16d', 'j-o-b_17a', 'j-o-b_17b', 'j-o-b_17c', 'j-o-b_17d', 'j-o-b_17e', 'j-o-b_17f',
        'j-o-b_18a', 'j-o-b_18b', 'j-o-b_18c', 'j-o-b_19a', 'j-o-b_19b', 'j-o-b_19c', 'j-o-b_19d', 'j-o-b_20a', 'j-o-b_20b',
        'j-o-b_20c', 'j-o-b_21a', 'j-o-b_21b', 'j-o-b_21c', 'j-o-b_22a', 'j-o-b_22b', 'j-o-b_22c', 'j-o-b_22d', 'j-o-b_23a',
        'j-o-b_23b', 'j-o-b_23c', 'j-o-b_24a', 'j-o-b_24b', 'j-o-b_25a', 'j-o-b_25b', 'j-o-b_25c', 'j-o-b_26a',
        'j-o-b_26b', 'j-o-b_26c', 'j-o-b_27a', 'j-o-b_27b', 'j-o-b_27c', 'j-o-b_28a', 'j-o-b_28b', 'j-o-b_28c',
        'j-o-b_29a', 'j-o-b_29b', 'j-o-b_29c', 'j-o-b_30a', 'j-o-b_30b', 'j-o-b_30c', 'j-o-b_31a', 'j-o-b_31b',
        'j-o-b_31c', 'j-o-b_32a', 'j-o-b_32b', 'j-o-b_33a', 'j-o-b_33b', 'j-o-b_33c'
      ],
      tpch_duckdb_demo: [
          'tpch-q1', 'tpch-q3', 'tpch-q5', 'tpch-q6', 'tpch-q7', 'tpch-q10', 'tpch-q11', 'tpch-q12', 'tpch-q19'
      ],
      tpch_datafusion_demo: [
          'tpch-q1', 'tpch-q3', 'tpch-q5', 'tpch-q6', 'tpch-q10', 'tpch-q12', 'tpch-q13', 'tpch-q14', 'tpch-q18', 'tpch-q19'
      ],
      tpcds_duckdb_demo: [
        'tpcds-query3', 'tpcds-query7', 'tpcds-query22', 'tpcds-query25', 'tpcds-query26', 'tpcds-query29', 'tpcds-query42', 'tpcds-query43',
        'tpcds-query52', 'tpcds-query55', 'tpcds-query59', 'tpcds-query65', 'tpcds-query93', 'tpcds-query97'
      ],
      job_duckdb_demo: [
        'j-o-b_2a', 'j-o-b_2b', 'j-o-b_2c', 'j-o-b_2d', 'j-o-b_6f', 'j-o-b_8c', 'j-o-b_8d', 'j-o-b_11d', 'j-o-b_12a', 'j-o-b_12c',
        'j-o-b_13a', 'j-o-b_13d', 'j-o-b_14a', 'j-o-b_14c', 'j-o-b_16a', 'j-o-b_16b', 'j-o-b_16c', 'j-o-b_16d', 'j-o-b_25a', 'j-o-b_25c',
        'j-o-b_32a', 'j-o-b_32b', 'j-o-b_33a', 'j-o-b_33c'
      ],
      selectAll: false,
    };
  },
  watch: {
    queries(newQueries) {
      this.$emit("update-queries", newQueries);
    },
    selectedQuerySet(newValue) {
      const autoSelectSets = [
        "tpch_duckdb_demo",
        "tpch_datafusion_demo",
        "tpcds_duckdb_demo",
        "job_duckdb_demo"
      ];
      this.queries = [];
      this.selectAll = false;
      this.$emit("update-queries", this.queries);
      if (autoSelectSets.includes(newValue)) {
        this.toggleAll();
      }
    }
  },
  computed: {
    availableParsers() {
      const parsers = [
        { value: "Calcite", label: "Calcite" },
        { value: "DataFusion", label: "DataFusion" },
        { value: "DuckDB", label: "DuckDB" }
      ];
      if (this.selectedQuerySet === "tpch"
          || this.selectedQuerySet === "tpch_duckdb_demo"
          || this.selectedQuerySet === "tpch_datafusion_demo") {
        parsers.push({ value: "Ibis", label: "Ibis" });
      }
      return parsers;
    },
    currentQuerySet() {
      if (this.selectedQuerySet ==='tpch') {
        return this.query_set_tpch;
      } else if (this.selectedQuerySet === 'reduced_tpch') {
        return this.query_set_reduced_tpch;
      } else if (this.selectedQuerySet === 'tpcds') {
        return this.query_set_tpcds;
      } else if (this.selectedQuerySet === 'job') {
        return this.query_set_job;
      } else if (this.selectedQuerySet === 'tpch_duckdb_demo') {
        return this.tpch_duckdb_demo;
      } else if (this.selectedQuerySet === 'tpch_datafusion_demo') {
        return this.tpch_datafusion_demo;
      } else if (this.selectedQuerySet === 'tpcds_duckdb_demo') {
        return this.tpcds_duckdb_demo;
      } else if (this.selectedQuerySet === 'job_duckdb_demo') {
        return this.job_duckdb_demo;
      } else {
        return [];
      }
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
      this.queries = [];
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
      <label for="parser">Select Parser/Optimizer</label>
      <select id="parser" v-model="parser" required>
          <option v-for="option in availableParsers" :key="option.value" :value="option.value">
            {{ option.label }}
          </option>
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
          <!--<option value="Velox">Velox Engine</option>-->
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
          <option value="tpcds">TPC-DS</option>
          <option value="job">Join-Order-Benchmark</option>
          <option value="tpch_duckdb_demo">TPC-H DuckDB Demo</option>
          <option value="tpch_datafusion_demo">TPC-H DataFusion Demo</option>
          <option value="tpcds_duckdb_demo">TPC-DS DuckDB Demo</option>
          <option value="job_duckdb_demo">Join-Order DuckDB Demo</option>
        </select>
        <button @click="toggleAll" class="select-all-btn">{{ selectAll ? 'Deselect All' : 'Select All' }}</button>
    </div>
    <!-- Query Options -->
    <div class="query-options">
      <div v-for="query in currentQuerySet" :key="query">
        <input type="checkbox" :id="query" :value="query" v-model="queries" />
        <label :for="query">{{ query.replace('tpch-q', 'TPC-H Q').replace('reduced_tpch_q', 'Reduced TPC-H Q').replace('j-o-b_', 'JOB ').replace('tpcds-query', 'TPC-DS Q') }}</label>
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
