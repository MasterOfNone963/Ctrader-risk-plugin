import { defineConfig } from 'vite';

// The embedded browser used inside the cTrader app does not reliably
// execute <script type="module">. So instead of letting Vite process
// index.html as a module entry (which forces type="module"), we:
//  1) bundle src/main.js directly into ONE plain IIFE file
//     (no import/export syntax left at runtime), and
//  2) keep index.html as a plain static file (in public/) that loads
//     that bundle with a normal <script> tag.
export default defineConfig({
  publicDir: 'public',
  build: {
    rollupOptions: {
      input: 'src/main.js',
      output: {
        format: 'iife',
        entryFileNames: 'main.bundle.js',
        // Force everything (all imported packages) into the single file.
        inlineDynamicImports: true,
      },
    },
  },
});
