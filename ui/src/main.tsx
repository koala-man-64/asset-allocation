import React from "react";
import ReactDOM from "react-dom/client";

import App from "./app/App";
import { BrowserRouter } from "react-router-dom";
import "./styles/index.css";

const rootElement = document.getElementById("root");
if (!rootElement) {
  throw new Error("Root element (#root) not found");
}

ReactDOM.createRoot(rootElement).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
);
