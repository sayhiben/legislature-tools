import { runReportApp } from "./modules/app.js";

runReportApp().catch((error) => {
  console.error("Failed to initialize report UI.", error);
  const busyIndicator = document.getElementById("report-busy-indicator");
  const busyText = document.getElementById("report-busy-text");
  if (busyText) {
    busyText.textContent =
      "Unable to load report UI. Serve this report directory over HTTP and refresh.";
  }
  if (busyIndicator) {
    busyIndicator.classList.remove("hidden");
  }
});
