# =====================================================
# SmartCare Metadata Explorer — Real Data Version (Shiny)
# =====================================================
library(shiny)
library(dplyr)
library(DT)
library(shinyjs)
library(bslib)
library(stringr)
library(tibble)
library(purrr)
library(tidyr)
library(jsonlite)
library(digest)

# -------------------------------------------------------------------
# Load SmartCare JSON metadata
# -------------------------------------------------------------------
IMPORT_DATE <- format(Sys.Date(), "%B %d, %Y")
data_dir <- "data/JSON"

safe_read_json <- function(path) {
  tryCatch(jsonlite::fromJSON(path, simplifyVector = TRUE), error = function(e) NULL)
}

# -------------------------------------------------------------------
# User Annotations Helper Functions
# -------------------------------------------------------------------
load_user_annotations <- function() {
  csv_path <- file.path(data_dir, "user_annotations.csv")
  json_path <- file.path(data_dir, "user_annotations.json")
  
  # Try to load from JSON first (faster), then CSV
  if (file.exists(json_path)) {
    tryCatch({
      json_data <- jsonlite::fromJSON(json_path, simplifyVector = TRUE)
      if (is.data.frame(json_data)) {
        return(as_tibble(json_data) %>%
          mutate(
            entity_type = as.character(entity_type),
            table_name = as.character(table_name),
            column_name = as.character(column_name),
            field_name = as.character(field_name),
            value = as.character(value),
            last_updated = as.POSIXct(last_updated)
          ))
      }
    }, error = function(e) NULL)
  }
  
  # Fallback to CSV
  if (file.exists(csv_path)) {
    tryCatch({
      read.csv(csv_path, stringsAsFactors = FALSE) %>%
        as_tibble() %>%
        mutate(
          entity_type = as.character(entity_type),
          table_name = as.character(table_name),
          column_name = as.character(column_name),
          field_name = as.character(field_name),
          value = as.character(value),
          last_updated = as.POSIXct(last_updated)
        )
    }, error = function(e) {
      return(tibble(
        entity_type = character(),
        table_name = character(),
        column_name = character(),
        field_name = character(),
        value = character(),
        last_updated = as.POSIXct(character())
      ))
    })
  } else {
    # Return empty structure if no file exists
    return(tibble(
      entity_type = character(),
      table_name = character(),
      column_name = character(),
      field_name = character(),
      value = character(),
      last_updated = as.POSIXct(character())
    ))
  }
}

save_user_annotations <- function(annotations_df) {
  tryCatch({
    csv_path <- file.path(data_dir, "user_annotations.csv")
    json_path <- file.path(data_dir, "user_annotations.json")
    
    # Ensure directory exists
    if (!dir.exists(data_dir)) {
      dir.create(data_dir, recursive = TRUE)
    }
    
    # Save to CSV
    write.csv(annotations_df, csv_path, row.names = FALSE)
    cat("Saved to CSV:", csv_path, "\n")
    
    # Convert to JSON for faster loading
    jsonlite::write_json(annotations_df, json_path, pretty = TRUE)
    cat("Saved to JSON:", json_path, "\n")
    
  }, error = function(e) {
    cat("Error in save_user_annotations:", e$message, "\n")
    stop(e)
  })
}

get_annotation <- function(entity_type, table_name, column_name = NA, field_name) {
  if (is.na(column_name)) {
    result <- user_annotations_data %>%
      filter(entity_type == !!entity_type,
             table_name == !!table_name,
             is.na(column_name) | column_name == "",
             field_name == !!field_name) %>%
      slice(1)
  } else {
    result <- user_annotations_data %>%
      filter(entity_type == !!entity_type,
             table_name == !!table_name,
             column_name == !!column_name,
             field_name == !!field_name) %>%
      slice(1)
  }
  
  if (nrow(result) > 0) {
    return(result$value)
  } else {
    return("")
  }
}

update_annotation <- function(entity_type, table_name, column_name = NA, field_name, value) {
  tryCatch({
    # Update in-memory data
    current_time <- Sys.time()
    
    # Remove existing entry if it exists
    if (is.na(column_name)) {
      user_annotations_data <<- user_annotations_data %>%
        filter(!(entity_type == !!entity_type & 
                 table_name == !!table_name & 
                 (is.na(column_name) | column_name == "") & 
                 field_name == !!field_name))
    } else {
      user_annotations_data <<- user_annotations_data %>%
        filter(!(entity_type == !!entity_type & 
                 table_name == !!table_name & 
                 column_name == !!column_name & 
                 field_name == !!field_name))
    }
    
    # Add new entry - ensure consistent data types
    new_entry <- tibble(
      entity_type = as.character(entity_type),
      table_name = as.character(table_name),
      column_name = as.character(if (is.na(column_name)) "" else column_name),
      field_name = as.character(field_name),
      value = as.character(value),
      last_updated = as.POSIXct(current_time)
    )
    
    # Ensure existing data has consistent types
    if (nrow(user_annotations_data) > 0) {
      user_annotations_data <<- user_annotations_data %>%
        mutate(
          entity_type = as.character(entity_type),
          table_name = as.character(table_name),
          column_name = as.character(column_name),
          field_name = as.character(field_name),
          value = as.character(value),
          last_updated = as.POSIXct(last_updated)
        )
    }
    
    user_annotations_data <<- bind_rows(user_annotations_data, new_entry)
    
    # Save to disk with error handling
    save_user_annotations(user_annotations_data)
    
    cat("Successfully saved annotation:", entity_type, table_name, field_name, "\n")
    
  }, error = function(e) {
    cat("Error in update_annotation:", e$message, "\n")
    stop(e)
  })
}

# Data will be loaded after authentication based on password
# Password mapping:
# - "casrc_internal" -> metadata_internal_20250826.json
# - "casrc" -> metadata_suppressed_20250826.json

# Load global codes and chatbot data (these are shared across both data sources)
sc_global_codes_raw <- safe_read_json(file.path(data_dir, "Adhoc_GlobalCodes.json"))
sc_chatbot_data <- safe_read_json(file.path(data_dir, "smartcare_chatbot_data.json"))

if (!is.null(sc_global_codes_raw) && is.data.frame(sc_global_codes_raw)) {
  sc_global_codes <- as_tibble(sc_global_codes_raw)
} else {
  sc_global_codes <- sc_global_codes_raw
}

# Load user annotations (shared across both data sources)
user_annotations_data <- load_user_annotations()

# Helper function to parse sample_values string and extract just the values
# Format: "1 (n = 1, 0.3%); 10 (n = 1, 0.3%); ..." -> extract "1; 10; ..."
parse_sample_values <- function(sample_str) {
  if (is.na(sample_str) || sample_str == "" || is.null(sample_str)) {
    return("")
  }
  
  # Split by semicolon to get individual entries
  entries <- strsplit(as.character(sample_str), ";")[[1]]
  
  # Extract just the value part (before the opening parenthesis)
  values <- character()
  for (entry in entries) {
    entry <- trimws(entry)
    # Remove trailing " + others" if present
    if (str_detect(entry, " \\+ others")) {
      entry <- str_replace(entry, " \\+ others.*", "")
    }
    # Extract value before " ("
    if (str_detect(entry, " \\(")) {
      value <- str_extract(entry, "^[^(]+") %>% trimws()
      if (!is.na(value) && value != "") {
        values <- c(values, value)
      }
    } else if (entry != "") {
      # No parentheses, use the whole entry
      values <- c(values, entry)
    }
  }
  
  # Return first 10 values for compact, all for full
  if (length(values) == 0) {
    return("")
  }
  
  return(paste(values, collapse = "; "))
}

# Helper: get data type from chatbot data
get_data_type_from_chatbot <- function(tbl, col) {
  if (!is.null(sc_chatbot_data) && "tables" %in% names(sc_chatbot_data) && tbl %in% names(sc_chatbot_data$tables)) {
    tinfo <- sc_chatbot_data$tables[[tbl]]
    if (!is.null(tinfo$columns)) {
      # Columns are data frames, not lists
      cols_df <- tinfo$columns
      if (col %in% cols_df$name) {
        col_row <- cols_df[cols_df$name == col, ]
        return(col_row$data_type %||% NA_character_)
      }
    }
  }
  return(NA_character_)
}

# Helper: get global codes for a specific field
get_global_codes_for_field <- function(tbl, col, metadata_df) {
  if (!is.null(sc_chatbot_data) && "tables" %in% names(sc_chatbot_data) && tbl %in% names(sc_chatbot_data$tables)) {
    tinfo <- sc_chatbot_data$tables[[tbl]]
    if (!is.null(tinfo$columns)) {
      # Columns are data frames, not lists
      cols_df <- tinfo$columns
      if (col %in% cols_df$name) {
        col_row <- cols_df[cols_df$name == col, ]
        if (col_row$data_type == "type_GlobalCode") {
          # Get actual values from the metadata sample_values
          field_row <- metadata_df %>%
            filter(table_name == tbl, field_name == col) %>%
            slice(1)
          
          if (nrow(field_row) > 0 && !is.na(field_row$sample_values) && field_row$sample_values != "") {
            # Parse sample values to get the actual values
            parsed_values <- parse_sample_values(field_row$sample_values)
            if (parsed_values != "") {
              field_values <- strsplit(parsed_values, "; ")[[1]] %>% trimws()
            } else {
              field_values <- character()
            }
            
            if (length(field_values) > 0 && !is.null(sc_global_codes) && nrow(sc_global_codes) > 0) {
              # Handle BOM character in GlobalCodeId field name in Adhoc_GlobalCodes
              global_code_id_col <- if ("GlobalCodeId" %in% names(sc_global_codes)) "GlobalCodeId" else names(sc_global_codes)[1]
              
              # Try multiple matching strategies and combine results
              matching_results <- list()
              
              # Strategy 1: Match by GlobalCodeId
              if (global_code_id_col %in% names(sc_global_codes)) {
                if (is.numeric(sc_global_codes[[global_code_id_col]])) {
                  # Try to convert field_values to numeric
                  numeric_vals <- suppressWarnings(as.numeric(field_values))
                  numeric_vals <- numeric_vals[!is.na(numeric_vals)]
                  if (length(numeric_vals) > 0) {
                    matching_results[[length(matching_results) + 1]] <- sc_global_codes %>%
                      filter(!!sym(global_code_id_col) %in% numeric_vals)
                  }
                } else {
                  matching_results[[length(matching_results) + 1]] <- sc_global_codes %>%
                    filter(as.character(!!sym(global_code_id_col)) %in% field_values)
                }
              }
              
              # Strategy 2: Match by Code
              if ("Code" %in% names(sc_global_codes)) {
                matching_results[[length(matching_results) + 1]] <- sc_global_codes %>%
                  filter(as.character(Code) %in% field_values)
              }
              
              # Strategy 3: Match by CodeName
              if ("CodeName" %in% names(sc_global_codes)) {
                matching_results[[length(matching_results) + 1]] <- sc_global_codes %>%
                  filter(as.character(CodeName) %in% field_values)
              }
              
              # Strategy 4: Match by Category (for some cases)
              if ("Category" %in% names(sc_global_codes)) {
                matching_results[[length(matching_results) + 1]] <- sc_global_codes %>%
                  filter(as.character(Category) == col)
              }
              
              # Combine all matching results and remove duplicates
              if (length(matching_results) > 0) {
                matching_codes <- bind_rows(matching_results) %>%
                  distinct() %>%
                  arrange(if ("Category" %in% names(.)) Category else 1, 
                          if ("CodeName" %in% names(.)) CodeName else 1) %>%
                  slice_head(n = 20)  # Limit to first 20 to avoid overwhelming display
              } else {
                matching_codes <- sc_global_codes %>% slice_head(n = 0)  # Empty result
              }
              
              if (nrow(matching_codes) > 0) {
                # Format as "GlobalCodeId: CodeName" pairs, one per line
                code_id_val <- if (global_code_id_col %in% names(matching_codes)) {
                  as.character(matching_codes[[global_code_id_col]])
                } else {
                  rep("", nrow(matching_codes))
                }
                code_name_val <- if ("CodeName" %in% names(matching_codes)) {
                  as.character(matching_codes$CodeName)
                } else {
                  rep("", nrow(matching_codes))
                }
                formatted_codes <- paste0(code_id_val, ": ", code_name_val)
                return(paste(formatted_codes, collapse = "\n"))
              }
            }
          }
        }
      }
    }
  }
  return("")
}

# Helper: infer key types using chatbot data and naming conventions
infer_key_type <- function(tbl, col) {
  # Prefer explicit primary key from chatbot data
  if (!is.null(sc_chatbot_data) && "tables" %in% names(sc_chatbot_data) && tbl %in% names(sc_chatbot_data$tables)) {
    tinfo <- sc_chatbot_data$tables[[tbl]]
    if (!is.null(tinfo$columns)) {
      # Columns are data frames, not lists
      cols_df <- tinfo$columns
      if (col %in% cols_df$name) {
        col_row <- cols_df[cols_df$name == col, ]
        if (isTRUE(col_row$is_primary_key)) return("Primary")
      }
    }
    # Foreign keys (explicit)
    if (!is.null(tinfo$foreign_keys) && is.list(tinfo$foreign_keys)) {
      fk_cols <- character(0)
      for (fk in tinfo$foreign_keys) {
        if (is.list(fk) && !is.null(fk$foreign_keys)) {
          fk_cols <- c(fk_cols, fk$foreign_keys)
        }
      }
      if (length(fk_cols) && col %in% fk_cols) return("Foreign")
    }
  }
  # Heuristic: columns ending with Id or _id and not primary get Foreign
  if (str_detect(col, "(?i)(Id|_id)$")) return("Foreign")
  "None"
}

# -------------------------------------------------------------------
# Extract table relationships from database schema
# -------------------------------------------------------------------
extract_table_relationships <- function() {
  # Load database schema
  schema_data <- safe_read_json(file.path(data_dir, "smartcare_database_schema.json"))
  
  if (is.null(schema_data) || !"tables" %in% names(schema_data)) {
    return(tibble(
      `Source Table` = character(),
      `Target Table` = character(),
      `Foreign Key Column` = character(),
      `Relationship Type` = character(),
      `Constraint Name` = character()
    ))
  }
  
  relationships <- list()
  
  for (table_name in names(schema_data$tables)) {
    table_info <- schema_data$tables[[table_name]]
    
    # Check if foreign_keys exists and is a list/array
    if (!is.null(table_info$foreign_keys) && 
        is.list(table_info$foreign_keys) && 
        length(table_info$foreign_keys) > 0) {
      
      # Handle foreign keys as data frame
      if (is.data.frame(table_info$foreign_keys)) {
        for (i in seq_len(nrow(table_info$foreign_keys))) {
          fk <- table_info$foreign_keys[i, ]
          
          # Check if this is a valid foreign key row
          if (length(fk$table_name) == 1 && !is.na(fk$table_name) && 
              length(fk$foreign_keys) == 1 && !is.na(fk$foreign_keys) &&
              fk$table_name != "" && fk$foreign_keys != "") {
            
            relationships[[length(relationships) + 1]] <- list(
              `Source Table` = table_name,
              `Target Table` = fk$table_name,
              `Foreign Key Column` = fk$foreign_keys,
              `Relationship Type` = fk$relationship_type %||% "parent",
              `Constraint Name` = fk$constraint_name %||% "",
              `Cardinality` = fk$cardinality %||% "",
              `Type` = fk$type %||% ""
            )
          }
        }
      } else {
        # Handle foreign keys as list
        for (fk in table_info$foreign_keys) {
          # Check if this is a valid foreign key object
          if (is.list(fk) && 
              !is.null(fk$table_name) && 
              !is.null(fk$foreign_keys) &&
              length(fk$table_name) == 1 && fk$table_name != "" &&
              length(fk$foreign_keys) == 1 && fk$foreign_keys != "") {
            
            relationships[[length(relationships) + 1]] <- list(
              `Source Table` = table_name,
              `Target Table` = fk$table_name,
              `Foreign Key Column` = fk$foreign_keys,
              `Relationship Type` = fk$relationship_type %||% "parent",
              `Constraint Name` = fk$constraint_name %||% "",
              `Cardinality` = fk$cardinality %||% "",
              `Type` = fk$type %||% ""
            )
          }
        }
      }
    }
  }
  
  if (length(relationships) == 0) {
    return(tibble(
      `Source Table` = character(),
      `Target Table` = character(),
      `Foreign Key Column` = character(),
      `Relationship Type` = character(),
      `Constraint Name` = character()
    ))
  }
  
  # Convert to tibble and add additional relationship info
  relationships_df <- bind_rows(relationships) %>%
    mutate(
      `Join Type` = case_when(
        `Relationship Type` == "parent" ~ "Many-to-One",
        `Relationship Type` == "child" ~ "One-to-Many",
        TRUE ~ "Unknown"
      ),
      `Join Description` = paste0(`Source Table`, ".", `Foreign Key Column`, " → ", `Target Table`, ".", `Foreign Key Column`),
      `Cardinality Info` = paste0(`Type`, " (", `Cardinality`, ")")
    ) %>%
    arrange(`Source Table`, `Target Table`)
  
  return(relationships_df)
}

derive_default <- function() sample(c("NULL", "N/A", "Unknown", "0"), 1)

# =====================================================
# UI
# =====================================================
ui <- fluidPage(
  theme = bs_theme(version = 5, bootswatch = "flatly"),
  useShinyjs(),
  
  tags$head(tags$style(HTML("
    body { background-color: #f5f6f7; font-family: 'Helvetica Neue', sans-serif; }
    .login-container { 
      display: flex; 
      justify-content: center; 
      align-items: center; 
      min-height: 100vh; 
      padding: 20px;
    }
    .login-card { 
      background: #fff; 
      border-radius: 12px; 
      padding: 40px; 
      width: 100%; 
      max-width: 450px; 
      box-shadow: 0 4px 20px rgba(0,0,0,0.15);
    }
    .login-title { 
      color: #00356b; 
      text-align: center; 
      margin-bottom: 10px; 
      font-size: 28px; 
      font-weight: 600;
    }
    .login-subtitle {
      color: #6c757d;
      text-align: center;
      margin-bottom: 30px;
      font-size: 14px;
    }
    .error-message {
      color: #dc3545;
      background-color: #f8d7da;
      border: 1px solid #f5c6cb;
      border-radius: 6px;
      padding: 12px;
      margin-bottom: 20px;
      font-size: 14px;
    }
    .success-message {
      color: #155724;
      background-color: #d4edda;
      border: 1px solid #c3e6cb;
      border-radius: 6px;
      padding: 12px;
      margin-bottom: 20px;
      font-size: 14px;
    }
  "))),
  
  # Login Page
  conditionalPanel(
    condition = "output.showLogin == true",
    div(class = "login-container",
        div(class = "login-card",
            div(class = "login-title", "📊 SmartCare Metadata Explorer"),
            div(class = "login-subtitle", "Please enter your password to continue"),
            uiOutput("login_error"),
            passwordInput("password_input", "Password", width = "100%", placeholder = "Enter password"),
            br(),
            actionButton("login_button", "Login", class = "btn-primary btn-block", style = "width: 100%; padding: 10px; font-size: 16px;")
        )
    )
  ),
  
  # Main App (shown after login)
  conditionalPanel(
    condition = "output.showLogin == false",
    navbarPage(
      title = "📊 SmartCare Metadata Explorer",
      id = "mainNav",
  
  # ------------------- All Tables -------------------
  tabPanel(
    "All Tables",
    fluidPage(
      tags$head(tags$style(HTML("
        body { background-color: #f5f6f7; font-family: 'Helvetica Neue', sans-serif; }
        .datatable-card { background:#fff; border-radius:12px; padding:25px; width:95%; margin:20px auto; box-shadow:0 4px 10px rgba(0,0,0,0.1); }
        table.dataTable thead input { width:100%; padding:4px; box-sizing:border-box; font-size:13px; }
        table.dataTable tbody tr:hover { background-color:#eaf2ff !important; }
        .search-bar { width:60%; margin: 0 auto 10px auto; }
        .import-note { text-align:center; color:#6c757d; font-style:italic; margin-bottom: 16px; }
        .shiny-notification { border-radius: 8px; }
      "))),
      div(class = "search-bar",
          textInput("global_search", label = NULL,
                    placeholder = "🔍 Search across all columns (table, column, values)...", width = "100%")
      ),
      div(class = "import-note", paste("Date of Data Import:", IMPORT_DATE)),
      div(class = "datatable-card", DTOutput("results"))
    )
  ),
  
  # ------------------- Table Details -------------------
  tabPanel(
    "Table Details",
    fluidPage(
      tags$head(tags$style(HTML("
        .detail-card { background:#fff; border-radius:12px; padding:25px; width:85%; margin:25px auto;
                       box-shadow:0 4px 10px rgba(0,0,0,0.1); }
        h3 { color:#00356b; margin-top:0; }
        .subtle-line { color:#6c757d; font-style:italic; margin-top:-6px; margin-bottom:10px; }
        .section-title { font-weight:600; color:#00356b; margin-top:22px; }
        .info-box { background:#f8f9fa; padding:10px 15px; border-radius:8px; margin-top:8px; }
        .summary-grid { background:#f8f9fa; padding:15px; border-radius:8px; margin-bottom:18px; }
      "))),
      
      div(class = "detail-card",
          h3(textOutput("tbl_header")),
          div(class = "subtle-line", paste("Date of Data Import:", IMPORT_DATE)),
          tags$hr(),
          
          div(class = "section-title", "Table Description"),
          textAreaInput("tbl_desc", NULL, "", width = "100%", height = "70px"),
          
          div(class = "section-title", "Table Notes"),
          textAreaInput("tbl_notes", NULL, "", width = "100%", height = "70px"),
          
          div(style = "margin-top: 10px;",
              actionButton("edit_table", "Edit", class = "btn-primary"),
              actionButton("submit_table", "Submit", class = "btn-success", style = "display:none;")
          ),
          
          div(class = "summary-grid",
              fluidRow(
                column(4, div(class = "info-box", strong("# of Columns:"), br(), textOutput("tbl_cols"))),
                column(4, div(class = "info-box", strong("# of Rows:"), br(), textOutput("tbl_rows"))),
                column(4, div(class = "info-box", strong("Last Updated:"), br(), textOutput("tbl_updated")))
              )
          ),
          
          div(class = "section-title", "Column-Level Details"),
          DTOutput("tbl_columns_dt")
      )
    )
  ),
  
  # ------------------- Column Details -------------------
  tabPanel(
    "Column Details",
    fluidPage(
      tags$head(tags$style(HTML("
        .detail-card { background:#fff; border-radius:12px; padding:25px; width:80%; margin:25px auto;
                       box-shadow:0 4px 10px rgba(0,0,0,0.1); }
        h3 { color:#00356b; margin-top:0; }
        .subtle-line { color:#6c757d; font-style:italic; margin-top:-6px; margin-bottom:10px; }
        .section-title { font-weight:600; color:#00356b; margin-top:22px; }
        .info-box { background:#f8f9fa; padding:10px 15px; border-radius:8px; margin-top:8px; }
        .summary-grid { background:#f8f9fa; padding:15px; border-radius:8px; margin-bottom:18px; }
        .quick-jump { width:80%; margin: 0 auto 10px auto; }
      "))),
      
      div(class = "quick-jump",
          fluidRow(
            column(12, selectInput("jump_column", "🔎 Jump to a different column in the same table:", 
                                  choices = NULL, width = "100%"))
          )
      ),
      
      div(class = "detail-card",
          h3(textOutput("col_name")),
          div(class = "subtle-line", paste("Date of Data Import:", IMPORT_DATE)),
          uiOutput("col_context_text"),
          tags$hr(),
          
          div(class = "summary-grid",
              fluidRow(
                column(3, div(class = "info-box", strong("Data Type:"), br(), textOutput("col_type"))),
                column(3, div(class = "info-box", strong("% Filled:"), br(), textOutput("col_filled"))),
                column(3, div(class = "info-box", strong("# Unique Values:"), br(), textOutput("col_unique"))),
                column(3, div(class = "info-box", strong("Default Value:"), br(), textOutput("col_default")))
              ),
              fluidRow(
                column(12, div(class = "info-box", strong("Dropdown Values:"), br(), textOutput("col_values")))
              )
          ),
          
          div(class = "section-title", "Column Definition"),
          textAreaInput("col_def", NULL,
                        "", width = "100%", height = "70px"),
          
          div(class = "section-title", "Data Notes"),
          textAreaInput("col_notes", NULL,
                        "", width = "100%", height = "70px"),
          
          div(style = "margin-top: 10px;",
              actionButton("edit_column", "Edit", class = "btn-primary"),
              actionButton("submit_column", "Submit", class = "btn-success", style = "display:none;")
          ),
          
          div(class = "section-title", "Other Tables Where This Column Appears"),
          uiOutput("related_tables")
      )
    )
  ),
  
  # ------------------- Table Relationships -------------------
  tabPanel(
    "Table Relationships",
    fluidPage(
      tags$head(tags$style(HTML("
        body { background-color: #f5f6f7; font-family: 'Helvetica Neue', sans-serif; }
        .datatable-card { background:#fff; border-radius:12px; padding:25px; width:95%; margin:20px auto; box-shadow:0 4px 10px rgba(0,0,0,0.1); }
        table.dataTable thead input { width:100%; padding:4px; box-sizing:border-box; font-size:13px; }
        table.dataTable tbody tr:hover { background-color:#eaf2ff !important; }
        .search-bar { width:60%; margin: 0 auto 10px auto; }
        .import-note { text-align:center; color:#6c757d; font-style:italic; margin-bottom: 16px; }
        .relationship-summary { background:#f8f9fa; padding:15px; border-radius:8px; margin-bottom:18px; text-align:center; }
        .join-type-badge { display:inline-block; padding:2px 8px; border-radius:12px; font-size:11px; font-weight:500; }
        .join-type-many-to-one { background:#d4edda; color:#155724; }
        .join-type-one-to-many { background:#d1ecf1; color:#0c5460; }
        .join-type-unknown { background:#f8d7da; color:#721c24; }
      "))),
      
      div(class = "search-bar",
          textInput("relationship_search", label = NULL,
                    placeholder = "🔍 Search relationships (table names, columns, join types)...", width = "100%")
      ),
      
      div(class = "import-note", paste("Date of Data Import:", IMPORT_DATE)),
      
      div(class = "relationship-summary",
          fluidRow(
            column(4, div(strong("Total Relationships:"), br(), textOutput("total_relationships"))),
            column(4, div(strong("Tables with Foreign Keys:"), br(), textOutput("tables_with_fks"))),
            column(4, div(strong("Tables Referenced:"), br(), textOutput("referenced_tables")))
          )
      ),
      
      div(class = "datatable-card", DTOutput("relationships_table"))
    )
  )
    )
  )
)

# =====================================================
# SERVER
# =====================================================
server <- function(input, output, session) {
  
  # Authentication state
  authenticated <- reactiveVal(FALSE)
  data_source <- reactiveVal(NULL)  # Will be "internal" or "suppressed"
  metadata_internal <- reactiveVal(NULL)
  
  # Show/hide login page
  output$showLogin <- reactive(!authenticated())
  outputOptions(output, "showLogin", suspendWhenHidden = FALSE)
  
  # Login error message
  output$login_error <- renderUI({
    if (!is.null(input$login_button) && input$login_button > 0) {
      password <- isolate(input$password_input)
      if (password == "") {
        return(tags$div(class = "error-message", "Please enter a password."))
      } else if (password != "casrc_internal" && password != "casrc") {
        return(tags$div(class = "error-message", "Invalid password. Please try again."))
      }
    }
    return(NULL)
  })
  
  # Handle login
  observeEvent(input$login_button, {
    password <- input$password_input
    
    if (password == "casrc_internal") {
      # Show loading notification
      loading_id <- showNotification("Loading data... Please wait.", 
                                     type = "message", duration = NULL, id = "loading_data")
      
      # Load internal data source
      metadata_file <- file.path(data_dir, "metadata_internal_20250826.json")
      metadata_raw <- safe_read_json(metadata_file)
      
      if (is.null(metadata_raw)) {
        removeNotification(loading_id)
        showNotification("Error: Could not load metadata_internal_20250826.json", type = "error")
        return()
      }
      
      # Handle case where JSON might be wrapped in a list
      if (is.list(metadata_raw) && !is.data.frame(metadata_raw)) {
        # Try to extract data frame from list
        if (length(metadata_raw) > 0 && is.data.frame(metadata_raw[[1]])) {
          metadata_raw <- metadata_raw[[1]]
        } else if ("data" %in% names(metadata_raw) && is.data.frame(metadata_raw$data)) {
          metadata_raw <- metadata_raw$data
        } else {
          removeNotification(loading_id)
          showNotification("Error: metadata_internal_20250826.json has unexpected structure", type = "error")
          return()
        }
      }
      
      if (!is.data.frame(metadata_raw)) {
        removeNotification(loading_id)
        showNotification("Error: metadata_internal_20250826.json is not in the expected format", type = "error")
        return()
      }
      
      # Validate the loaded data
      if (nrow(metadata_raw) == 0) {
        removeNotification(loading_id)
        showNotification("Error: metadata_internal_20250826.json is empty", type = "error")
        return()
      }
      
      metadata_internal(as_tibble(metadata_raw))
      data_source("internal")
      authenticated(TRUE)
      
      # Remove loading notification
      removeNotification(loading_id)
      
      # Show success notification
      showNotification("Successfully loaded internal data source", type = "message", duration = 2)
      
    } else if (password == "casrc") {
      # Show loading notification
      loading_id <- showNotification("Loading data... Please wait.", 
                                     type = "message", duration = NULL, id = "loading_data")
      
      # Load suppressed data source
      metadata_file <- file.path(data_dir, "metadata_suppressed_20250826.json")
      metadata_raw <- safe_read_json(metadata_file)
      
      if (is.null(metadata_raw)) {
        removeNotification(loading_id)
        showNotification("Error: Could not load metadata_suppressed_20250826.json", type = "error")
        return()
      }
      
      # Handle case where JSON might be wrapped in a list
      if (is.list(metadata_raw) && !is.data.frame(metadata_raw)) {
        # Try to extract data frame from list
        if (length(metadata_raw) > 0 && is.data.frame(metadata_raw[[1]])) {
          metadata_raw <- metadata_raw[[1]]
        } else if ("data" %in% names(metadata_raw) && is.data.frame(metadata_raw$data)) {
          metadata_raw <- metadata_raw$data
        } else {
          removeNotification(loading_id)
          showNotification("Error: metadata_suppressed_20250826.json has unexpected structure", type = "error")
          return()
        }
      }
      
      if (!is.data.frame(metadata_raw)) {
        removeNotification(loading_id)
        showNotification("Error: metadata_suppressed_20250826.json is not in the expected format", type = "error")
        return()
      }
      
      # Validate the loaded data
      if (nrow(metadata_raw) == 0) {
        removeNotification(loading_id)
        showNotification("Error: metadata_suppressed_20250826.json is empty", type = "error")
        return()
      }
      
      metadata_internal(as_tibble(metadata_raw))
      data_source("suppressed")
      authenticated(TRUE)
      
      # Remove loading notification
      removeNotification(loading_id)
      
      # Show success notification
      showNotification("Successfully loaded suppressed data source", type = "message", duration = 2)
      
    } else {
      # Invalid password - error message will be shown by login_error output
      return()
    }
  })
  
  # Cache for processed data to avoid recalculation
  processed_data_cache <- reactiveVal(NULL)
  processed_data_cache_key <- reactiveVal(NULL)
  
  # Pre-compute lookups for better performance
  precompute_lookups <- function(md) {
    # Pre-compute data types from chatbot data
    data_types_lookup <- tibble(
      table_name = character(),
      field_name = character(),
      data_type = character()
    )
    
    if (!is.null(sc_chatbot_data) && "tables" %in% names(sc_chatbot_data)) {
      for (tbl in names(sc_chatbot_data$tables)) {
        tinfo <- sc_chatbot_data$tables[[tbl]]
        if (!is.null(tinfo$columns) && is.data.frame(tinfo$columns)) {
          cols_df <- tinfo$columns
          if (nrow(cols_df) > 0 && "name" %in% names(cols_df) && "data_type" %in% names(cols_df)) {
            lookup_rows <- tibble(
              table_name = tbl,
              field_name = cols_df$name,
              data_type = as.character(cols_df$data_type)
            )
            data_types_lookup <- bind_rows(data_types_lookup, lookup_rows)
          }
        }
      }
    }
    
    # Pre-compute key types
    key_types_lookup <- tibble(
      table_name = character(),
      field_name = character(),
      key_type = character()
    )
    
    if (!is.null(sc_chatbot_data) && "tables" %in% names(sc_chatbot_data)) {
      for (tbl in names(sc_chatbot_data$tables)) {
        tinfo <- sc_chatbot_data$tables[[tbl]]
        if (!is.null(tinfo$columns) && is.data.frame(tinfo$columns)) {
          cols_df <- tinfo$columns
          if (nrow(cols_df) > 0 && "name" %in% names(cols_df)) {
            # Primary keys
            if ("is_primary_key" %in% names(cols_df)) {
              pk_cols <- cols_df$name[cols_df$is_primary_key == TRUE]
              if (length(pk_cols) > 0) {
                key_types_lookup <- bind_rows(key_types_lookup, 
                  tibble(table_name = tbl, field_name = pk_cols, key_type = "Primary"))
              }
            }
            
            # Foreign keys
            if (!is.null(tinfo$foreign_keys)) {
              fk_cols <- character()
              if (is.data.frame(tinfo$foreign_keys) && "foreign_keys" %in% names(tinfo$foreign_keys)) {
                fk_cols <- unique(tinfo$foreign_keys$foreign_keys)
              } else if (is.list(tinfo$foreign_keys)) {
                for (fk in tinfo$foreign_keys) {
                  if (is.list(fk) && !is.null(fk$foreign_keys)) {
                    fk_cols <- c(fk_cols, fk$foreign_keys)
                  }
                }
              }
              if (length(fk_cols) > 0) {
                key_types_lookup <- bind_rows(key_types_lookup,
                  tibble(table_name = tbl, field_name = fk_cols, key_type = "Foreign"))
              }
            }
          }
        }
      }
    }
    
    # Add heuristic-based foreign keys (Id/_id suffix)
    all_fields <- md %>% select(table_name, field_name) %>% distinct()
    heuristic_fks <- all_fields %>%
      filter(str_detect(field_name, "(?i)(Id|_id)$")) %>%
      anti_join(key_types_lookup, by = c("table_name", "field_name")) %>%
      mutate(key_type = "Foreign")
    
    key_types_lookup <- bind_rows(key_types_lookup, heuristic_fks)
    
    # Add "None" for remaining fields
    all_with_keys <- key_types_lookup %>% select(table_name, field_name)
    none_keys <- all_fields %>%
      anti_join(all_with_keys, by = c("table_name", "field_name")) %>%
      mutate(key_type = "None")
    
    key_types_lookup <- bind_rows(key_types_lookup, none_keys)
    
    # Pre-compute global codes for GlobalCode type fields (optimized batch processing)
    global_codes_lookup <- tibble(
      table_name = character(),
      field_name = character(),
      global_codes = character()
    )
    
    # Find all GlobalCode fields - check both field_type and data_types_lookup
    global_code_fields <- md %>%
      filter(field_type == "type_GlobalCode") %>%
      select(table_name, field_name, sample_values)
    
    # Also check data_types_lookup if it has entries
    if (nrow(data_types_lookup) > 0) {
      chatbot_global_codes <- data_types_lookup %>%
        filter(data_type == "type_GlobalCode") %>%
        select(table_name, field_name)
      
      additional_fields <- md %>%
        inner_join(chatbot_global_codes, by = c("table_name", "field_name")) %>%
        anti_join(global_code_fields %>% select(table_name, field_name), by = c("table_name", "field_name")) %>%
        select(table_name, field_name, sample_values)
      
      global_code_fields <- bind_rows(global_code_fields, additional_fields)
    }
    
    # Batch process global codes lookup if we have fields and global codes data
    # Skip if there are too many fields (can be slow) - limit to reasonable number
    max_global_code_fields <- 1000  # Limit to prevent excessive processing time
    if (nrow(global_code_fields) > 0 && !is.null(sc_global_codes) && nrow(sc_global_codes) > 0) {
      # Limit the number of fields to process if too many
      fields_to_process <- nrow(global_code_fields)
      if (fields_to_process > max_global_code_fields) {
        global_code_fields <- global_code_fields %>%
          slice_head(n = max_global_code_fields)
        # Note: processing limited to first max_global_code_fields fields
      }
      
      # Pre-process global codes once for efficient lookups
      global_code_id_col <- if ("GlobalCodeId" %in% names(sc_global_codes)) "GlobalCodeId" else names(sc_global_codes)[1]
      
      # Create lookup structures for fast matching
      gc_id_lookup <- if (global_code_id_col %in% names(sc_global_codes)) {
        if (is.numeric(sc_global_codes[[global_code_id_col]])) {
          sc_global_codes %>% 
            select(id = !!sym(global_code_id_col), CodeName) %>%
            mutate(id_char = as.character(id))
        } else {
          sc_global_codes %>% 
            select(id = !!sym(global_code_id_col), CodeName) %>%
            mutate(id_char = as.character(id))
        }
      } else {
        tibble(id = character(), CodeName = character(), id_char = character())
      }
      
      gc_code_lookup <- if ("Code" %in% names(sc_global_codes)) {
        sc_global_codes %>% 
          select(code = Code, CodeName) %>%
          mutate(code_char = as.character(code))
      } else {
        tibble(code = character(), CodeName = character(), code_char = character())
      }
      
      gc_codename_lookup <- if ("CodeName" %in% names(sc_global_codes)) {
        sc_global_codes %>% 
          select(codename = CodeName)
      } else {
        tibble(codename = character())
      }
      
      gc_category_lookup <- if ("Category" %in% names(sc_global_codes)) {
        sc_global_codes %>% 
          select(category = Category, CodeName, id = !!sym(global_code_id_col))
      } else {
        tibble(category = character(), CodeName = character(), id = character())
      }
      
      # Process all fields in batch
      global_codes_results <- global_code_fields %>%
        mutate(
          parsed_values = map_chr(sample_values, parse_sample_values),
          field_values_list = map(parsed_values, function(pv) {
            if (pv == "") return(character())
            strsplit(pv, "; ")[[1]] %>% trimws()
          })
        ) %>%
        filter(map_int(field_values_list, length) > 0)
      
      if (nrow(global_codes_results) > 0) {
        # Optimize: Use vectorized operations where possible
        # Create a combined lookup for faster matching
        all_field_values <- unique(unlist(global_codes_results$field_values_list))
        
        # Pre-filter lookup tables once for all values
        gc_id_matches <- if (nrow(gc_id_lookup) > 0 && length(all_field_values) > 0) {
          numeric_vals <- suppressWarnings(as.numeric(all_field_values))
          numeric_vals <- numeric_vals[!is.na(numeric_vals)]
          if (length(numeric_vals) > 0) {
            gc_id_lookup %>%
              filter(id %in% numeric_vals | id_char %in% all_field_values)
          } else {
            gc_id_lookup %>%
              filter(id_char %in% all_field_values)
          }
        } else {
          gc_id_lookup %>% slice_head(n = 0)
        }
        
        gc_code_matches <- if (nrow(gc_code_lookup) > 0 && length(all_field_values) > 0) {
          gc_code_lookup %>%
            filter(code_char %in% all_field_values)
        } else {
          gc_code_lookup %>% slice_head(n = 0)
        }
        
        gc_codename_matches <- if (nrow(gc_codename_lookup) > 0 && length(all_field_values) > 0) {
          gc_codename_lookup %>%
            filter(codename %in% all_field_values)
        } else {
          gc_codename_lookup %>% slice_head(n = 0)
        }
        
        # Batch process matches using pre-filtered data
        global_codes_lookup_list <- map(seq_len(nrow(global_codes_results)), function(i) {
          row <- global_codes_results[i, ]
          field_vals <- row$field_values_list[[1]]
          
          if (length(field_vals) == 0) return(NULL)
          
          matching_results <- list()
          
          # Strategy 1: Match by GlobalCodeId (use pre-filtered matches)
          if (nrow(gc_id_matches) > 0) {
            numeric_vals <- suppressWarnings(as.numeric(field_vals))
            numeric_vals <- numeric_vals[!is.na(numeric_vals)]
            matches <- gc_id_matches %>%
              filter(id %in% numeric_vals | id_char %in% field_vals)
            if (nrow(matches) > 0) {
              matching_results[[length(matching_results) + 1]] <- matches
            }
          }
          
          # Strategy 2: Match by Code (use pre-filtered matches)
          if (nrow(gc_code_matches) > 0) {
            matches <- gc_code_matches %>%
              filter(code_char %in% field_vals)
            if (nrow(matches) > 0) {
              matching_results[[length(matching_results) + 1]] <- matches
            }
          }
          
          # Strategy 3: Match by CodeName (use pre-filtered matches)
          if (nrow(gc_codename_matches) > 0) {
            matches <- gc_codename_matches %>%
              filter(codename %in% field_vals)
            if (nrow(matches) > 0) {
              matching_results[[length(matching_results) + 1]] <- matches
            }
          }
          
          # Strategy 4: Match by Category (column name)
          if (nrow(gc_category_lookup) > 0) {
            matches <- gc_category_lookup %>%
              filter(category == row$field_name)
            if (nrow(matches) > 0) {
              matching_results[[length(matching_results) + 1]] <- matches
            }
          }
          
          # Combine results
          if (length(matching_results) > 0) {
            all_matches <- bind_rows(matching_results) %>%
              distinct()
            
            # Get CodeName and ID for formatting
            if ("CodeName" %in% names(all_matches)) {
              code_names <- all_matches$CodeName
            } else {
              code_names <- rep("", nrow(all_matches))
            }
            
            if (nrow(gc_id_lookup) > 0 && "id" %in% names(all_matches)) {
              code_ids <- as.character(all_matches$id)
            } else if (nrow(gc_id_lookup) > 0 && "id_char" %in% names(all_matches)) {
              code_ids <- all_matches$id_char
            } else {
              code_ids <- rep("", length(code_names))
            }
            
            # Format and limit to 20
            formatted <- paste0(code_ids, ": ", code_names)
            if (length(formatted) > 20) {
              formatted <- formatted[1:20]
            }
            
            result_str <- paste(formatted, collapse = "\n")
            
            if (result_str != "") {
              return(tibble(
                table_name = row$table_name,
                field_name = row$field_name,
                global_codes = result_str
              ))
            }
          }
          
          return(NULL)
        })
        
        # Combine all results
        valid_results <- compact(global_codes_lookup_list)
        if (length(valid_results) > 0) {
          global_codes_lookup <- bind_rows(valid_results)
        }
      }
    }
    
    return(list(
      data_types = data_types_lookup,
      key_types = key_types_lookup,
      global_codes = global_codes_lookup
    ))
  }
  
  # Load and prepare data after authentication
  processed_data <- reactive({
    req(authenticated())
    req(!is.null(metadata_internal()))
    
    # Make sure this reactive depends on metadata_internal
    md <- metadata_internal()
    
    # Check cache
    cache_key <- digest::digest(md)
    if (!is.null(processed_data_cache()) && 
        !is.null(processed_data_cache_key()) && 
        processed_data_cache_key() == cache_key) {
      return(processed_data_cache())
    }
    
    # Show loading notification
    loading_id <- showNotification("Processing data... This may take a moment.", 
                                   type = "message", duration = NULL, id = "processing_data")
    
    # Validate that metadata is loaded correctly
    if (is.null(md) || !is.data.frame(md) || nrow(md) == 0) {
      removeNotification(loading_id)
      showNotification("Error: Metadata is empty or invalid", type = "error")
      return(NULL)
    }
    
    tryCatch({
      # Pre-compute lookups once
      lookups <- precompute_lookups(md)
      
      # Create fields_meta from metadata_internal
      fields_meta <- as_tibble(md) %>%
        mutate(
          table_name = as.character(table_name),
          field_name = as.character(field_name),
          field_type = as.character(field_type),
          rows = as.numeric(rows_total),
          uniques = as.numeric(unique_n),
          blanks = as.numeric(missing_n),
          fill = ifelse(!is.na(pct_missing), 
                        1 - as.numeric(pct_missing) / 100,
                        ifelse(!is.na(filled_n) & !is.na(rows_total) & rows_total > 0,
                               as.numeric(filled_n) / as.numeric(rows_total),
                               0)),
          field_desc = notes %||% NA_character_
        ) %>%
        select(table_name, field_name, field_type, rows, uniques, blanks, fill, field_desc)
      
      # Create tables_meta by aggregating fields_meta
      tables_meta <- fields_meta %>%
        group_by(table_name) %>%
        summarise(
          `# of Table Cols` = n_distinct(field_name),
          `# of Table Rows` = max(rows, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        mutate(
          Table = table_name,
          `Table Description` = "",
          `Last Updated` = ""
        ) %>%
        select(Table, `Table Description`, `# of Table Cols`, `# of Table Rows`, `Last Updated`)
      
      # Parse sample_values to create dropdown values (vectorized)
      values_parsed <- as_tibble(md) %>%
        select(table_name, field_name, sample_values) %>%
        mutate(
          drop_values_parsed = map_chr(sample_values, parse_sample_values),
          drop_values = map_chr(drop_values_parsed, function(parsed) {
            if (parsed == "") return("")
            values_list <- strsplit(parsed, "; ")[[1]]
            if (length(values_list) > 10) {
              paste(values_list[1:10], collapse = "; ")
            } else {
              parsed
            }
          }),
          drop_values_full = drop_values_parsed
        ) %>%
        select(table_name, field_name, drop_values, drop_values_full)
      
      # Build unified dataset with vectorized operations
      test_app <- fields_meta %>%
        left_join(tables_meta %>% select(Table, `# of Table Cols`, `# of Table Rows`), by = c("table_name" = "Table")) %>%
        left_join(values_parsed, by = c("table_name", "field_name")) %>%
        mutate(
          Table = table_name,
          Column = field_name
        )
      
      # Add data types using lookup
      if (nrow(lookups$data_types) > 0) {
        test_app <- test_app %>%
          left_join(lookups$data_types, by = c("table_name", "field_name")) %>%
          mutate(
            `Variable Type` = ifelse(!is.na(data_type) & data_type != "", data_type, field_type)
          ) %>%
          select(-data_type)
      } else {
        test_app <- test_app %>%
          mutate(`Variable Type` = field_type)
      }
      
      # Add key types using lookup
      test_app <- test_app %>%
        left_join(lookups$key_types, by = c("table_name", "field_name")) %>%
        mutate(`Key Type` = ifelse(!is.na(key_type), key_type, "None")) %>%
        select(-key_type)
      
      # Add global codes using lookup
      if (nrow(lookups$global_codes) > 0) {
        test_app <- test_app %>%
          left_join(lookups$global_codes, by = c("table_name", "field_name")) %>%
          mutate(
            `Dropdown Values` = ifelse(!is.na(global_codes) & global_codes != "", global_codes, drop_values %||% ""),
            `Dropdown Values Full` = ifelse(!is.na(global_codes) & global_codes != "", global_codes, drop_values_full %||% "")
          ) %>%
          select(-global_codes)
      } else {
        test_app <- test_app %>%
          mutate(
            `Dropdown Values` = drop_values %||% "",
            `Dropdown Values Full` = drop_values_full %||% ""
          )
      }
      
      # Finalize columns
      test_app <- test_app %>%
        mutate(
          `% Filled` = round(as.numeric(fill) * 100, 1),
          `# Unique Values` = uniques
        ) %>%
        select(Table, Column, `Variable Type`, `Dropdown Values`, `Dropdown Values Full`, `% Filled`, `# Unique Values`, `Key Type`)
      
      # Create display version
      test_app_display <- test_app %>%
        select(Table, Column, `Variable Type`, `Dropdown Values`, `% Filled`, `# Unique Values`, `Key Type`)
      
      # Extract relationships (cached)
      table_relationships <- extract_table_relationships()
      if (nrow(table_relationships) == 0) {
        table_relationships <- tibble(
          `Source Table` = c("ConcurrentReviews", "ClientMedications", "DocumentVersions", "Staff", "Clients"),
          `Target Table` = c("Documents", "Medications", "Documents", "Programs", "Staff"),
          `Foreign Key Column` = c("PreScreenDocumentId", "MedicationId", "DocumentId", "ProgramId", "PrimaryStaffId"),
          `Relationship Type` = c("parent", "parent", "parent", "parent", "parent"),
          `Constraint Name` = c("Documents_ConcurrentReviews_FK", "Medications_ClientMedications_FK", "Documents_DocumentVersions_FK", "Programs_Staff_FK", "Staff_Clients_FK"),
          `Join Type` = c("Many-to-One", "Many-to-One", "Many-to-One", "Many-to-One", "Many-to-One"),
          `Join Description` = c("ConcurrentReviews.PreScreenDocumentId → Documents.PreScreenDocumentId", 
                                "ClientMedications.MedicationId → Medications.MedicationId",
                                "DocumentVersions.DocumentId → Documents.DocumentId",
                                "Staff.ProgramId → Programs.ProgramId",
                                "Clients.PrimaryStaffId → Staff.PrimaryStaffId")
        )
      }
      
      # Validate processed data
      if (nrow(test_app) == 0) {
        removeNotification(loading_id)
        showNotification("Error: No data processed from metadata", type = "error")
        return(NULL)
      }
      
      result <- list(
        test_app = test_app,
        test_app_display = test_app_display,
        tables_meta = tables_meta,
        table_relationships = table_relationships
      )
      
      # Cache the result
      processed_data_cache(result)
      processed_data_cache_key(cache_key)
      
      removeNotification(loading_id)
      showNotification("Data loaded successfully!", type = "message", duration = 2)
      
      return(result)
      
    }, error = function(e) {
      removeNotification(loading_id)
      showNotification(paste("Error processing data:", e$message), type = "error", duration = 5)
      cat("Error in processed_data:", e$message, "\n")
      return(NULL)
    })
  })
  
  # Reactive value to track current column
  current_column <- reactiveVal(NULL)
  
  # --- Global search (All Tables)
  filtered_data <- reactive({
    req(processed_data())
    data <- processed_data()$test_app_display
    q <- str_trim(input$global_search %||% "")
    if (q == "") return(data)
    data %>%
      filter(if_any(everything(), ~ str_detect(str_to_lower(as.character(.x)), str_to_lower(q))))
  })
  
  # --- All Tables render
  output$results <- renderDT({
    req(processed_data())
    data <- processed_data()
    if (is.null(data)) {
      return(datatable(data.frame(Message = "No data available"), rownames = FALSE))
    }
    
    data_display <- filtered_data()
    if (nrow(data_display) == 0) {
      return(datatable(data.frame(Message = "No data matches your search"), rownames = FALSE))
    }
    
    data_display <- data_display %>%
      mutate(
        Table = sprintf("<a href='#' onclick=\"Shiny.setInputValue('selected_table','%s',{priority:'event'})\">%s</a>", Table, Table),
        Column = sprintf("<a href='#' onclick=\"Shiny.setInputValue('selected_column','%s',{priority:'event'})\">%s</a>", Column, Column)
      )
    datatable(data_display, escape = FALSE, filter = "top", rownames = FALSE,
              class = "stripe hover row-border order-column compact",
              options = list(pageLength = 15, dom = 'tip', scrollX = TRUE))
  }, server = TRUE)
  
  # ---------------- Column Details ----------------
  observeEvent(input$selected_column, {
    current_column(input$selected_column)
  })
  
  # Handle dropdown column selection
  observeEvent(input$jump_column, {
    if (!is.null(input$jump_column) && input$jump_column != "") {
      current_column(input$jump_column)
    }
  })
  
  # Update column details when current_column changes
  observeEvent(current_column(), {
    req(processed_data())
    if (!is.null(current_column())) {
      data <- processed_data()
      selected <- data$test_app %>%
        filter(Column == current_column()) %>%
        left_join(data$tables_meta, by = "Table") %>%
        slice(1)
      
      updateTabsetPanel(session, "mainNav", selected = "Column Details")
      
      # Update dropdown choices with columns from the same table
      same_table_cols <- data$test_app %>%
        filter(Table == selected$Table) %>%
        pull(Column)
      
      updateSelectInput(session, "jump_column", 
                       choices = setNames(same_table_cols, same_table_cols),
                       selected = selected$Column)
      
      # Load saved annotations
      saved_def <- get_annotation("column", selected$Table, selected$Column, field_name = "definition")
      saved_notes <- get_annotation("column", selected$Table, selected$Column, field_name = "notes")
      
      # Pre-populate text areas with saved annotations
      updateTextAreaInput(session, "col_def", value = saved_def)
      updateTextAreaInput(session, "col_notes", value = saved_notes)
      
      # Reset button states
      shinyjs::disable("col_def")
      shinyjs::disable("col_notes")
      shinyjs::show("edit_column")
      shinyjs::hide("submit_column")
      
      output$col_name <- renderText({ selected$Column })
      output$col_type <- renderText({ selected$`Variable Type` })
      output$col_filled <- renderText({ paste0(selected$`% Filled`, "%") })
      output$col_unique <- renderText({ selected$`# Unique Values` })
      output$col_default <- renderText({ derive_default() })
      output$col_values <- renderText({ selected$`Dropdown Values Full` })
    
    output$col_context_text <- renderUI({
      this_tbl <- selected$Table
      tbl_info <- dplyr::filter(data$tables_meta, Table == this_tbl)
      tags$p(HTML(paste0(
        "<em>You are currently viewing <strong>", selected$Column,
        "</strong> in the ",
        "<a href='#' onclick=\"Shiny.setInputValue('selected_table','", this_tbl, "',{priority:'event'})\" ",
        "style='color:#0078D7;text-decoration:none;font-weight:500;'>",
        this_tbl, "</a> table.</em><br>",
        "This table contains ", tbl_info$`# of Table Cols`, " columns and ",
        format(tbl_info$`# of Table Rows`, big.mark = ","), " rows."
      )))
    })
    
    # Relationship section (same-named columns across tables)
    related <- data$test_app %>%
      filter(Column == selected$Column) %>%
      distinct(Table) %>%
      mutate(Role = ifelse(Table == selected$Table & selected$`Key Type` == "Primary", "(Primary)",
                           ifelse(Table != selected$Table, "(Foreign)", "")))
    
    output$related_tables <- renderUI({
      others <- related$Table[related$Table != selected$Table]
      if (length(others) == 0) return(tags$p("None"))
      tags$ul(lapply(others, function(tbl) {
        role <- related$Role[related$Table == tbl]
        tags$li(tags$a(paste0(tbl, " ", role), href = "#",
                       onclick = sprintf("Shiny.setInputValue('selected_table','%s',{priority:'event'})", tbl),
                       style = "color:#0078D7; text-decoration:none;"))
      }))
    })
    }
  })
  
  # Column Edit/Submit handlers
  observeEvent(input$edit_column, {
    shinyjs::enable("col_def")
    shinyjs::enable("col_notes")
    shinyjs::hide("edit_column")
    shinyjs::show("submit_column")
  })
  
  observeEvent(input$submit_column, {
    req(processed_data())
    # Show loading indicator
    showNotification("Saving column annotations...", type = "default", duration = NULL, id = "saving_col_msg")
    
    tryCatch({
      if (!is.null(current_column())) {
        data <- processed_data()
        selected <- data$test_app %>%
          filter(Column == current_column()) %>%
          slice(1)
        
        def_value <- input$col_def
        notes_value <- input$col_notes
        
        # Save annotations with error handling
        if (def_value != "") {
          update_annotation("column", selected$Table, selected$Column, field_name = "definition", value = def_value)
        }
        if (notes_value != "") {
          update_annotation("column", selected$Table, selected$Column, field_name = "notes", value = notes_value)
        }
        
        # Reset UI state
        shinyjs::disable("col_def")
        shinyjs::disable("col_notes")
        shinyjs::show("edit_column")
        shinyjs::hide("submit_column")
        
        # Remove loading message and show success
        removeNotification("saving_col_msg")
        showNotification("Column annotations saved successfully!", type = "default", duration = 3)
      }
      
    }, error = function(e) {
      # Remove loading message and show error
      removeNotification("saving_col_msg")
      showNotification(paste("Error saving column annotations:", e$message), type = "error", duration = 5)
      cat("Error in submit_column:", e$message, "\n")
    })
  })
  
  # ---------------- Table Details ----------------
  observeEvent(input$selected_table, {
    req(processed_data())
    data <- processed_data()
    tbl <- input$selected_table
    meta <- dplyr::filter(data$tables_meta, Table == tbl) %>% slice(1)
    updateTabsetPanel(session, "mainNav", selected = "Table Details")
    
    output$tbl_header <- renderText({ paste0("Table: ", meta$Table) })
    
    # Load saved annotations or use original description
    saved_desc <- get_annotation("table", tbl, field_name = "description")
    saved_notes <- get_annotation("table", tbl, field_name = "notes")
    
    updateTextAreaInput(session, "tbl_desc", value = if (saved_desc != "") saved_desc else (meta$`Table Description` %||% ""))
    updateTextAreaInput(session, "tbl_notes", value = saved_notes)
    
    # Reset button states
    shinyjs::disable("tbl_desc")
    shinyjs::disable("tbl_notes")
    shinyjs::show("edit_table")
    shinyjs::hide("submit_table")
    
    output$tbl_cols <- renderText({ meta$`# of Table Cols` })
    output$tbl_rows <- renderText({ format(meta$`# of Table Rows`, big.mark = ",") })
    output$tbl_updated <- renderText({ meta$`Last Updated` %||% "" })
    
    cols_expanded <- data$test_app_display %>%
      filter(Table == tbl) %>%
      select(`Column Name` = Column,
             `Data Type` = `Variable Type`,
             `% Filled`,
             `# Unique Values`,
             `Key Type`,
             `Dropdown Values`)
    
    cols_expanded$`Column Name` <- sprintf(
      "<a href='#' onclick=\"Shiny.setInputValue('selected_column','%s',{priority:'event'})\">%s</a>",
      cols_expanded$`Column Name`, cols_expanded$`Column Name`
    )
    
    output$tbl_columns_dt <- renderDT({
      datatable(cols_expanded, escape = FALSE, rownames = FALSE,
                filter = "top", class = "stripe hover row-border compact",
                options = list(pageLength = 12, dom = 'tip', scrollX = TRUE))
    }, server = TRUE)
  })
  
  # Table Edit/Submit handlers
  observeEvent(input$edit_table, {
    shinyjs::enable("tbl_desc")
    shinyjs::enable("tbl_notes")
    shinyjs::hide("edit_table")
    shinyjs::show("submit_table")
  })
  
  observeEvent(input$submit_table, {
    # Show loading indicator
    showNotification("Saving table annotations...", type = "default", duration = NULL, id = "saving_msg")
    
    tryCatch({
      tbl <- input$selected_table
      desc_value <- input$tbl_desc
      notes_value <- input$tbl_notes
      
      # Save annotations with error handling
      if (desc_value != "") {
        update_annotation("table", tbl, field_name = "description", value = desc_value)
      }
      if (notes_value != "") {
        update_annotation("table", tbl, field_name = "notes", value = notes_value)
      }
      
      # Reset UI state
      shinyjs::disable("tbl_desc")
      shinyjs::disable("tbl_notes")
      shinyjs::show("edit_table")
      shinyjs::hide("submit_table")
      
      # Remove loading message and show success
      removeNotification("saving_msg")
      showNotification("Table annotations saved successfully!", type = "default", duration = 3)
      
    }, error = function(e) {
      # Remove loading message and show error
      removeNotification("saving_msg")
      showNotification(paste("Error saving annotations:", e$message), type = "error", duration = 5)
      cat("Error in submit_table:", e$message, "\n")
    })
  })
  
  # ---------------- Table Relationships ----------------
  # Filter relationships based on search
  filtered_relationships <- reactive({
    req(processed_data())
    data <- processed_data()
    q <- str_trim(input$relationship_search %||% "")
    if (q == "") return(data$table_relationships)
    data$table_relationships %>%
      filter(if_any(everything(), ~ str_detect(str_to_lower(as.character(.x)), str_to_lower(q))))
  })
  
  # Render relationship summary statistics
  output$total_relationships <- renderText({
    req(processed_data())
    nrow(processed_data()$table_relationships)
  })
  
  output$tables_with_fks <- renderText({
    req(processed_data())
    length(unique(processed_data()$table_relationships$`Source Table`))
  })
  
  output$referenced_tables <- renderText({
    req(processed_data())
    length(unique(processed_data()$table_relationships$`Target Table`))
  })
  
  # Render relationships table
  output$relationships_table <- renderDT({
    req(processed_data())
    data_display <- filtered_relationships() %>%
      mutate(
        `Source Table` = sprintf("<a href='#' onclick=\"Shiny.setInputValue('selected_table','%s',{priority:'event'})\">%s</a>", 
                                `Source Table`, `Source Table`),
        `Target Table` = sprintf("<a href='#' onclick=\"Shiny.setInputValue('selected_table','%s',{priority:'event'})\">%s</a>", 
                                `Target Table`, `Target Table`),
        `Join Type` = sprintf("<span class='join-type-badge join-type-%s'>%s</span>", 
                             str_to_lower(str_replace_all(`Join Type`, "-", "-")), `Join Type`)
      ) %>%
      select(`Source Table`, `Target Table`, `Foreign Key Column`, `Join Type`, `Join Description`, `Cardinality Info`, `Constraint Name`)
    
    datatable(data_display, escape = FALSE, filter = "top", rownames = FALSE,
              class = "stripe hover row-border order-column compact",
              options = list(
                pageLength = 20, 
                dom = 'tip', 
                scrollX = TRUE,
                columnDefs = list(
                  list(width = "12%", targets = c(0, 1)),  # Source and Target tables
                  list(width = "10%", targets = 2),        # Foreign Key Column
                  list(width = "8%", targets = 3),         # Join Type
                  list(width = "30%", targets = 4),       # Join Description
                  list(width = "15%", targets = 5),        # Cardinality Info
                  list(width = "10%", targets = 6)         # Constraint Name
                )
              ))
  }, server = TRUE)
}

# =====================================================
# Run app
# =====================================================
shinyApp(ui, server)


