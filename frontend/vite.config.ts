import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";


export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.indexOf("node_modules") === -1) {
            return undefined;
          }

          if (id.indexOf("chart.js") !== -1 || id.indexOf("react-chartjs-2") !== -1) {
            return "charts";
          }
          if (id.indexOf("react") !== -1) {
            return "react-vendor";
          }

          return "vendor";
        },
      },
    },
  },
  server: {
    host: "0.0.0.0",
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8503",
        changeOrigin: true,
      },
      "/health": {
        target: "http://127.0.0.1:8503",
        changeOrigin: true,
      },
    },
  },
  preview: {
    host: "0.0.0.0",
    port: 4173,
  },
});
