import { defineConfig } from 'vite';
import { copyFileSync } from 'fs';

// The embedded browser inside the cTrader app does not execute
// <script type="module"> (confirmed by testing). So we bundle
// src/main.js into ONE plain classic script (IIFE format — no
// import/export left at runtime), and copy index.html into the
// output folder unchanged.
export default defineConfig({
  plugins: [
    {
      name: 'copy-index-html',
      closeBundle() {
        copyFileSync('index.html', 'dist/index.html');
      },
    },
  ],
  build: {
    rollupOptions: {
      input: 'src/main.js',
      output: {
        format: 'iife',
        entryFileNames: 'main.bundle.js',
        inlineDynamicImports: true,
      },
    },
  },
});
