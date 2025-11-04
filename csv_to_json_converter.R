# SmartCare CSV to JSON Converter
# This script converts CSV files from the CSV folder to JSON files in the data/JSON folder
# Author: SmartCare Data Management
# Date: Created for automated data updates

# Load required libraries
library(jsonlite)
library(readr)

# Function to convert CSV to JSON
convert_csv_to_json <- function(csv_file, json_file) {
  cat("Converting", csv_file, "to", json_file, "\n")
  
  tryCatch({
    # Read CSV file with more flexible parsing
    # First try to read with automatic type detection
    data <- read_csv(csv_file, show_col_types = FALSE, locale = locale(date_names = "en", date_format = "%Y-%m-%d", time_format = "%H:%M:%S"))
    
    # If that fails, try reading as character columns first
    if (inherits(data, "try-error")) {
      data <- read_csv(csv_file, col_types = cols(.default = "c"), show_col_types = FALSE)
    }
    
    # Handle NULL values and data types
    # Convert "NULL" strings to actual NULL values
    data[data == "NULL"] <- NA
    
    # Convert the data frame to JSON
    json_data <- toJSON(data, pretty = TRUE, auto_unbox = TRUE, na = "null")
    
    # Write JSON file
    writeLines(json_data, json_file)
    
    cat("Successfully converted", csv_file, "\n")
    cat("Rows converted:", nrow(data), "\n")
    cat("Columns:", ncol(data), "\n\n")
    
  }, error = function(e) {
    # If first attempt fails, try reading all columns as character
    tryCatch({
      cat("Retrying with character type parsing...\n")
      data <- read_csv(csv_file, col_types = cols(.default = "c"), show_col_types = FALSE)
      
      # Handle NULL values
      data[data == "NULL"] <- NA
      
      # Convert the data frame to JSON
      json_data <- toJSON(data, pretty = TRUE, auto_unbox = TRUE, na = "null")
      
      # Write JSON file
      writeLines(json_data, json_file)
      
      cat("Successfully converted", csv_file, "(as character types)\n")
      cat("Rows converted:", nrow(data), "\n")
      cat("Columns:", ncol(data), "\n\n")
    }, error = function(e2) {
      cat("Error converting", csv_file, ":", e2$message, "\n")
    })
  })
}

# Main conversion function
convert_all_csv_files <- function() {
  cat("Starting CSV to JSON conversion process...\n")
  cat("=====================================\n\n")
  
  # Check if CSV directory exists
  if (!dir.exists("CSV")) {
    stop("CSV directory not found. Please ensure you're running this script from the SmartCareApp root directory.")
  }
  
  # Check if JSON directory exists, create if not
  if (!dir.exists("data/JSON")) {
    dir.create("data/JSON", recursive = TRUE)
    cat("Created data/JSON directory\n")
  }
  
  # Find all CSV files in the CSV directory
  csv_files <- list.files("CSV", pattern = "\\.csv$", full.names = TRUE, ignore.case = TRUE)
  
  if (length(csv_files) == 0) {
    cat("No CSV files found in the CSV directory.\n")
    return(invisible(NULL))
  }
  
  cat("Found", length(csv_files), "CSV file(s) to convert:\n")
  for (f in csv_files) {
    cat("  -", basename(f), "\n")
  }
  cat("\n")
  
  # Convert each file
  for (csv_file in csv_files) {
    # Create JSON file path by replacing CSV directory with data/JSON and changing extension
    base_name <- tools::file_path_sans_ext(basename(csv_file))
    json_file <- file.path("data", "JSON", paste0(base_name, ".json"))
    
    convert_csv_to_json(csv_file, json_file)
  }
  
  cat("=====================================\n")
  cat("Conversion process completed!\n")
}

# Function to convert a specific file (useful for individual updates)
convert_single_file <- function(csv_filename) {
  # Remove extension and create mapping
  base_name <- gsub("\\.csv$", "", csv_filename)
  
  csv_path <- file.path("CSV", paste0(base_name, ".csv"))
  json_path <- file.path("data", "JSON", paste0(base_name, ".json"))
  
  if (file.exists(csv_path)) {
    convert_csv_to_json(csv_path, json_path)
  } else {
    cat("Error: File", csv_path, "not found.\n")
  }
}

# Function to check file sizes and provide summary
check_conversion_status <- function() {
  cat("Conversion Status Summary\n")
  cat("========================\n\n")
  
  file_mappings <- list(
    "CSV/global_codes_v1.csv" = "data/JSON/global_codes_v1.json",
    "CSV/SC_fields.csv" = "data/JSON/SC_fields.json",
    "CSV/SC_tables.csv" = "data/JSON/SC_tables.json",
    "CSV/SC_values.csv" = "data/JSON/SC_values.json"
  )
  
  for (csv_file in names(file_mappings)) {
    json_file <- file_mappings[[csv_file]]
    
    csv_exists <- file.exists(csv_file)
    json_exists <- file.exists(json_file)
    
    cat("File:", basename(csv_file), "\n")
    cat("  CSV exists:", csv_exists, "\n")
    cat("  JSON exists:", json_exists, "\n")
    
    if (csv_exists && json_exists) {
      csv_size <- file.size(csv_file)
      json_size <- file.size(json_file)
      csv_time <- file.mtime(csv_file)
      json_time <- file.mtime(json_file)
      
      cat("  CSV size:", format(csv_size, units = "MB"), "\n")
      cat("  JSON size:", format(json_size, units = "MB"), "\n")
      cat("  CSV modified:", as.character(csv_time), "\n")
      cat("  JSON modified:", as.character(json_time), "\n")
      
      if (csv_time > json_time) {
        cat("  Status: CSV is newer - needs conversion\n")
      } else {
        cat("  Status: JSON is up to date\n")
      }
    } else if (csv_exists && !json_exists) {
      cat("  Status: JSON missing - needs conversion\n")
    } else {
      cat("  Status: CSV file missing\n")
    }
    cat("\n")
  }
}

# Main execution
if (!interactive()) {
  # Run the conversion when script is executed directly
  convert_all_csv_files()
} else {
  # If running interactively, provide instructions
  cat("SmartCare CSV to JSON Converter\n")
  cat("==============================\n\n")
  cat("Available functions:\n")
  cat("1. convert_all_csv_files() - Convert all CSV files to JSON\n")
  cat("2. convert_single_file('filename.csv') - Convert a specific file\n")
  cat("3. check_conversion_status() - Check which files need updating\n\n")
  cat("Example usage:\n")
  cat("  convert_all_csv_files()\n")
  cat("  convert_single_file('global_codes_v1.csv')\n")
  cat("  check_conversion_status()\n")
}
