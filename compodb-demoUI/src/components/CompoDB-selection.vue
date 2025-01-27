<script>
export default {
  name: "CompoDB-selection",
  data() {
    return {
      optimizer: '',
      executionEngine: '',
      queries: [],
      inputFormat: '',
    };
  },
  methods: {
    async submitForm(event) {
      this.$emit("show-loading");
      event.preventDefault();

      const formData = {
        optimizer: this.optimizer,
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
          this.$emit("new-composition", formData.optimizer, formData.executionEngine);
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
      this.optimizer = '';
      this.executionEngine = '';
      this.queries = [];
      this.inputFormat = '';

      const checkboxes = document.querySelectorAll('.query-options input[type="checkbox"]');
      checkboxes.forEach(checkbox => checkbox.checked = false);

      const inputFormatSelect = document.getElementById('input-format');
      if (inputFormatSelect) {
        inputFormatSelect.value = '';
      }
      this.$emit("update-queries", []);
      this.$emit("update-input-format", '');
    },

    handleQueryChange(event) {
      const queryValue = event.target.value;
      if (event.target.checked) {
        this.queries.push(queryValue);
      } else {
        this.queries = this.queries.filter(query => query !== queryValue);
      }
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

      <!-- Optimizer Selection -->
      <label for="optimizer">Select Optimizer</label>
      <select id="optimizer" v-model="optimizer" required>
          <option value="calcite">Calcite Optimizer</option>
          <option value="datafusion">DataFusion Optimizer</option>
          <option value="duckdb">DuckDB Optimizer</option>
          <option value="ibis">Ibis Compiler</option>
      </select>

      <!-- Execution Engine Selection -->
      <label for="execution-engine">Select Execution Engine</label>
      <select id="execution-engine" v-model="executionEngine" required>
          <option value="duckdb">DuckDB Engine</option>
          <option value="datafusion">DataFusion Engine</option>
          <option value="acero">Acero Engine</option>
      </select>

      <!-- Submit Button -->
      <button type="submit">Add System</button>
    </form>

    <!-- Query Selection -->
    <label>Query Selection</label>
    <div class="query-options">
        <input type="checkbox" id="tpch-q1" value="tpch-q1" @change="handleQueryChange"/>
        <label for="tpch-q1">TPC-H Q1</label>

        <input type="checkbox" id="tpch-q2" value="tpch-q2" @change="handleQueryChange"/>
        <label for="tpch-q2">TPC-H Q2</label>

        <input type="checkbox" id="tpch-q3" value="tpch-q3" @change="handleQueryChange"/>
        <label for="tpch-q3">TPC-H Q3</label>

        <input type="checkbox" id="tpch-q4" value="tpch-q4" @change="handleQueryChange"/>
        <label for="tpch-q4">TPC-H Q4</label>

        <input type="checkbox" id="tpch-q5" value="tpch-q5" @change="handleQueryChange"/>
        <label for="tpch-q5">TPC-H Q5</label>

        <input type="checkbox" id="tpch-q6" value="tpch-q6" @change="handleQueryChange"/>
        <label for="tpch-q6">TPC-H Q6</label>

        <input type="checkbox" id="tpch-q7" value="tpch-q7" @change="handleQueryChange"/>
        <label for="tpch-q7">TPC-H Q7</label>

        <input type="checkbox" id="tpch-q8" value="tpch-q8" @change="handleQueryChange"/>
        <label for="tpch-q8">TPC-H Q8</label>

        <input type="checkbox" id="tpch-q9" value="tpch-q9" @change="handleQueryChange"/>
        <label for="tpch-q9">TPC-H Q9</label>

        <input type="checkbox" id="tpch-q10" value="tpch-q10" @change="handleQueryChange"/>
        <label for="tpch-q10">TPC-H Q10</label>

        <input type="checkbox" id="tpch-q11" value="tpch-q11" @change="handleQueryChange"/>
        <label for="tpch-q11">TPC-H Q11</label>

        <input type="checkbox" id="tpch-q12" value="tpch-q12" @change="handleQueryChange"/>
        <label for="tpch-q12">TPC-H Q12</label>

        <input type="checkbox" id="tpch-q13" value="tpch-q13" @change="handleQueryChange"/>
        <label for="tpch-q13">TPC-H Q13</label>

        <input type="checkbox" id="tpch-q14" value="tpch-q14" @change="handleQueryChange"/>
        <label for="tpch-q14">TPC-H Q14</label>

        <input type="checkbox" id="tpch-q15" value="tpch-q15" @change="handleQueryChange"/>
        <label for="tpch-q15">TPC-H Q15</label>

        <input type="checkbox" id="tpch-q16" value="tpch-q16" @change="handleQueryChange"/>
        <label for="tpch-q16">TPC-H Q16</label>

        <input type="checkbox" id="tpch-q17" value="tpch-q17" @change="handleQueryChange"/>
        <label for="tpch-q17">TPC-H Q17</label>

        <input type="checkbox" id="tpch-q18" value="tpch-q18" @change="handleQueryChange"/>
        <label for="tpch-q18">TPC-H Q18</label>

        <input type="checkbox" id="tpch-q19" value="tpch-q19" @change="handleQueryChange"/>
        <label for="tpch-q19">TPC-H Q19</label>

        <input type="checkbox" id="tpch-q20" value="tpch-q20" @change="handleQueryChange"/>
        <label for="tpch-q20">TPC-H Q20</label>

        <input type="checkbox" id="tpch-q21" value="tpch-q21" @change="handleQueryChange"/>
        <label for="tpch-q21">TPC-H Q21</label>

        <input type="checkbox" id="tpch-q22" value="tpch-q22" @change="handleQueryChange"/>
        <label for="tpch-q22">TPC-H Q22</label>
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
    margin-top: 20px;
    margin-bottom: 10px;
    font-size: 1.1em;
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
select {
    width: 100%;
    padding: 8px;
    margin-top: 5px;
    font-size: 16px;
}
.query-options {
    padding: 5px 8px;
    border: 1px solid #ccc;
    border-radius: 4px;
    background-color: rgba(40, 40, 50, 1);
    max-height: 187px;
    overflow-y: auto;
}
.query-options label {
    display: block;
    font-size: 1em;
    color: white;
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
    background-color: #0056b3;
    color: white;
}
</style>
