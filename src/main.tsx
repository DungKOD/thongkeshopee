import React from "react";
import ReactDOM from "react-dom/client";
// Self-host Material Symbols Rounded — bundle font local thay vì CDN.
// Lý do: production build (Tauri) khi network chậm/timeout fonts.googleapis.com,
// `font-display: block` fallback sau 3s → text raw "cloud_sync" hiện ra.
// Self-host = font có sẵn trong bundle, render ngay từ frame đầu.
import "material-symbols/rounded.css";
import App from "./App";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { ToastProvider } from "./components/ToastProvider";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <ErrorBoundary>
      <ToastProvider>
        <App />
      </ToastProvider>
    </ErrorBoundary>
  </React.StrictMode>,
);
