# Run SmartCare Metadata Explorer App
# Double-click this file or run: Rscript run_app.R

# Set working directory to the script's location
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  # If running in RStudio
  setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
} else {
  # If running in regular R or Rscript
  # Get the directory where this script is located
  args <- commandArgs(trailingOnly = FALSE)
  file_arg_name <- "--file="
  script_name <- sub(file_arg_name, "", args[grep(file_arg_name, args)])
  if (length(script_name) > 0) {
    setwd(dirname(normalizePath(script_name)))
  }
}

# Load required libraries
library(shiny)

# Run the app
cat("Starting SmartCare Metadata Explorer...\n")
cat("The app will open in your default web browser.\n")
cat("Press Ctrl+C or close the browser window to stop the app.\n\n")
shiny::runApp("smartcare_realdata_app.R", launch.browser = TRUE)
