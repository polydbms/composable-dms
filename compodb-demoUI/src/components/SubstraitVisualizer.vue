<template>
  <div v-if="visible" class="overlay">
    <div
      class="popup"
      ref="popup"
      @mousedown="startDrag"
      :style="{ top: `${position.top}px`, left: `${position.left}px`, width: `${position.width}px`, height: `${position.height}px` }"
    >
      <button class="close-btn" @click="closePopup">✖</button>
      <h4>Substrait Plan Visualizations</h4>
      <div v-if="images.length" class="image-wrapper">
        <div v-for="image in images" :key="image.image_url" class="image-container">
          <h5>{{ image.query_name }} ({{ image.parser_name }})</h5>
          <img :src="image.image_url" alt="Query Plan DAG" />
        </div>
      </div>
      <!-- Resize Handle -->
      <div class="resize-handle" @mousedown="startResize"></div>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    visible: Boolean,
    images: Array
  },
  data() {
    return {
      position: { top: 100, left: 100, width: 600, height: 400 }, // Initial position and size
      dragging: false,
      resizing: false,
      offset: { x: 0, y: 0 },
      resizeStart: { width: 0, height: 0, mouseX: 0, mouseY: 0 }
    };
  },
  methods: {
    closePopup() {
      this.$emit("close");
    },
    startDrag(event) {
      if (this.resizing) return; // Prevent dragging while resizing

      this.dragging = true;
      this.offset.x = event.clientX - this.position.left;
      this.offset.y = event.clientY - this.position.top;
      document.addEventListener("mousemove", this.drag);
      document.addEventListener("mouseup", this.stopDrag);
    },
    drag(event) {
      if (this.dragging) {
        this.position.left = event.clientX - this.offset.x;
        this.position.top = event.clientY - this.offset.y;
      }
    },
    stopDrag() {
      this.dragging = false;
      document.removeEventListener("mousemove", this.drag);
      document.removeEventListener("mouseup", this.stopDrag);
    },
    startResize(event) {
      event.preventDefault(); // Prevent default mouse events that may conflict

      this.resizing = true;
      this.resizeStart.width = this.position.width;
      this.resizeStart.height = this.position.height;
      this.resizeStart.mouseX = event.clientX;
      this.resizeStart.mouseY = event.clientY;
      document.addEventListener("mousemove", this.resize);
      document.addEventListener("mouseup", this.stopResize);
    },
    resize(event) {
      if (this.resizing) {
        const deltaX = event.clientX - this.resizeStart.mouseX;
        const deltaY = event.clientY - this.resizeStart.mouseY;

        // Update the popup size
        this.position.width = Math.max(this.resizeStart.width + deltaX, 200); // Minimum width
        this.position.height = Math.max(this.resizeStart.height + deltaY, 200); // Minimum height

        // Adjust images size dynamically
        this.$nextTick(() => {
          const images = this.$refs.popup.querySelectorAll('img');
          images.forEach(img => {
            img.style.maxWidth = `${this.position.width - 40}px`; // Adjust for padding/margin
            img.style.maxHeight = `${this.position.height - 100}px`; // Adjust for header and padding
          });
        });
      }
    },
    stopResize() {
      this.resizing = false;
      document.removeEventListener("mousemove", this.resize);
      document.removeEventListener("mouseup", this.stopResize);
    }
  }
};
</script>

<style scoped>
h4 {
  margin: 5px;
}
h5 {
  margin: 0 0 5px 0;
}
/* Background Overlay */
.overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(0, 0, 0, 0);
  display: flex;
  justify-content: center;
  align-items: center;
  border-radius: 8px;
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
}

/* Popup Container (Draggable and Resizable) */
.popup {
  position: absolute;
  background: white;
  padding: 5px 5px 0 5px;
  border-radius: 10px;
  text-align: center;
  min-width: 300px;
  max-width: 90vw;
  max-height: 90vh;
  overflow: hidden; /* Hide overflow outside of popup */
  box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2);
  cursor: grab; /* Show grab cursor */
  resize: none; /* Disable CSS resizing for precise control */
}

/* Close Button */
.close-btn {
  background: white;
  color: red;
  border: none;
  padding: 0 5px;
  float: right;
  cursor: pointer;
}

/* Wrapper for Images */
.image-wrapper {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  justify-content: center;
  overflow-y: auto; /* Allow scrolling when content overflows */
  overflow-x: hidden; /* Hide horizontal scroll if not needed */
  width: 100%;
  height: 100%;
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
  max-height: 100%;
  object-fit: contain;
  border: 2px solid #ccc;
  border-radius: 8px;
  padding: 5px;
  background: white;
  flex-shrink: 0; /* Prevent images from shrinking */
}

/* Resize Handle */
.resize-handle {
  width: 16px;
  height: 16px;
  background-color: #aaa;
  position: absolute;
  right: 0;
  bottom: 0;
  cursor: se-resize;
}
</style>