<template>
  <div v-if="visible" class="overlay">
    <div class="popup">
      <button class="close-btn" @click="closePopup">✖</button>
      <h2>Substrait Plan Visualizations</h2>
      <div v-if="images.length" class="image-wrapper">
        <div v-for="image in images" :key="image.image_url" class="image-container">
          <h3>{{ image.query_name }} ({{ image.parser_name }})</h3>
          <img :src="image.image_url" alt="Query Plan DAG" />
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    visible: Boolean,
    images: Array // Expecting an array of images
  },
  methods: {
    closePopup() {
      this.$emit("close"); // Emits event to close the popup
    }
  }
};
</script>

<style scoped>
/* Background Overlay */
.overlay {
  padding: 16px;
  border-radius: 8px;
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15); /* Soft shadow */
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  justify-content: center;
  align-items: center;
}

/* Popup Container */
.popup {
  background: white;
  padding: 20px;
  border-radius: 10px;
  text-align: center;
  min-width: 300px;
  max-width: 90vw; /* Limits width to 90% of viewport */
  max-height: 90vh; /* Limits height to 90% of viewport */
  overflow: auto; /* Enables scrolling if content exceeds max size */
  box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2);
}

/* Close Button */
.close-btn {
  background: red;
  color: white;
  border: none;
  padding: 5px 10px;
  float: right;
  cursor: pointer;
}

/* Wrapper for Images */
.image-wrapper {
  display: flex;
  flex-wrap: wrap; /* Wrap images to the next line if necessary */
  gap: 16px;
  justify-content: center;
  max-height: 80vh; /* Ensures images don't overflow viewport */
  overflow-y: auto;
}

/* Image Container */
.image-container {
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
}

/* Image Styling */
.image-container img {
  max-width: 100%;
  max-height: 70vh; /* Prevents images from exceeding viewport height */
  object-fit: contain; /* Keeps aspect ratio */
  border: 2px solid #ccc;
  border-radius: 8px;
  padding: 5px;
  background: white;
}
</style>