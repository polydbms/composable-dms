<script>
import Grid from './components/Grid.vue';
import CompoDB from './components/CompoDB.vue';
import CompoDBSelection from "./components/CompoDB-selection.vue";
import CompoDBOverview from "./components/CompoDB-overview.vue";
import CompoDBResults from "./components/CompoDB-results.vue";
import { ref } from "vue";

export default {
  name: 'App',
  components: {
    CompoDBResults,
    CompoDBOverview,
    CompoDBSelection,
    Grid,
    CompoDB,
  },
  setup() {
    const isLoading = ref(false);
    let loadingTimeout = null;

    const showLoading = () => {
      clearTimeout(loadingTimeout);

      loadingTimeout = setTimeout(() => {
        isLoading.value = true;
      }, 1500);
    };

    const hideLoading = () => {
      clearTimeout(loadingTimeout);
      isLoading.value = false;
    };

    return {
      isLoading,
      showLoading,
      hideLoading,
    };
  },
  methods: {
    handleNewComposition(parser, optimizer, executionEngine) {
      this.$refs.overviewComponent.addCompoDB(parser, optimizer, executionEngine);
    },
    updateQueries(queries) {
      this.$refs.overviewComponent.updateQueries(queries);
    },
    updateInputFormat(inputFormat) {
      this.$refs.overviewComponent.updateInputFormat(inputFormat);
    },
    clear() {
      this.$refs.selectionComponent.resetState();
      this.$refs.resultsComponent.clearChart();
    },
    handleBenchmarkResults(results) {
      this.$refs.resultsComponent.updateChart(results);
    },
  },
};
</script>

<template>
  <div>
    <div v-if="isLoading" class="loading-overlay">
      <div class="spinner"></div>
    </div>
    <Grid>
      <div class="inner-grid" style="grid-column: 1 / 2; margin: 0;">
        <CompoDB class="module"/>
        <CompoDBSelection class="module" ref="selectionComponent" @new-composition="handleNewComposition"
                                                                  @update-queries="updateQueries"
                                                                  @update-input-format="updateInputFormat"
                                                                  @show-loading="showLoading"
                                                                  @hide-loading="hideLoading"/>
      </div>
      <div class="inner-grid" style="grid-column: 2 / 3; margin: 0;">
        <CompoDBOverview class="module" ref="overviewComponent" @reset-selection="clear"
                                                                @benchmark-update="handleBenchmarkResults"
                                                                @show-loading="showLoading"
                                                                @hide-loading="hideLoading"/>
      </div>
      <div class="inner-grid" style="grid-column: 3 / 6; margin: 0;">
        <CompoDBResults class="module" ref="resultsComponent"/>
      </div>
    </Grid>
  </div>
</template>

<style scoped>
.module {
  background-color: white;
  padding: 16px;
  border-radius: 8px;
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
}
#compodb {
  background-color: white;
  padding: 16px;
  border-bottom-left-radius: 8px;
  border-bottom-right-radius: 8px;
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
}
.inner-grid {
  display: grid;
  grid-template-columns: repeat(1, 1fr);
  gap: 10px;
  width: 100%;
  margin: 10px auto;
  align-items: start;
  grid-auto-flow: row dense;
}
.loading-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100vw;
  height: 100vh;
  background-color: rgba(0, 0, 0, 0.5);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 9999;
}
.spinner {
  width: 60px;
  height: 60px;
  border: 6px solid rgba(255, 255, 255, 0.3);
  border-top: 6px solid black;
  border-radius: 50%;
  animation: spin 1.5s linear infinite;
}
@keyframes spin {
  0% {
    transform: rotate(0deg);
  }
  100% {
    transform: rotate(360deg);
  }
}
</style>