<template>
  <div id="grid-container">
    <slot></slot> <!-- This will render the CompoDB component inside the grid -->
  </div>
</template>

<script>
export default {
  name: 'Grid',
};
</script>

<style scoped>
#grid-container {
  display: grid;
  grid-template-columns: 3fr 3fr 5fr; /* 3 equal-width columns */
  gap: 1.5vw; /* Space between grid items */
  width: 90%; /* Grid width */
  margin: 20px auto; /* Center the grid */
  align-items: start; /* Align items to the top of their columns */
  grid-auto-rows: minmax(100px, auto); /* Allow rows to expand based on content */
  grid-auto-flow: row dense; /* Enable the dense algorithm to avoid unnecessary gaps */
}
</style>