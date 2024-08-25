# Install jsonlite package if not already installed
if (!require(jsonlite)) {
  install.packages("jsonlite")
}

# Function to extract keys from the file
extract_keys_from_file <- function(file_path) {
  keys <- character()
  
  # Read file line by line
  con <- file(file_path, "r")
  while (TRUE) {
    line <- readLines(con, n = 1, warn = FALSE)
    if (length(line) == 0) break
    
    if (grepl("Extracted data keys:", line)) {
      # Extract keys from the line
      start <- regexpr("\\{", line)[1] + 1
      end <- regexpr("\\}", line)[1] - 1
      keys_str <- substr(line, start, end)
      
      # Split and clean keys
      keys <- unlist(strsplit(keys_str, ", "))
      keys <- gsub("'", "", keys)
    }
  }
  close(con)
  return(unique(keys))
}

# Function to reconstruct JSON structure from keys
reconstruct_json_from_keys <- function(keys) {
  json_obj <- list()
  
  for (key in keys) {
    parts <- unlist(strsplit(key, "__"))
    current_level <- json_obj
    
    for (i in seq_along(parts)) {
      if (i == length(parts)) {
        # Set the placeholder value "BORK"
        current_level[[parts[i]]] <- "BORK"
      } else {
        # Create nested lists as necessary
        if (is.null(current_level[[parts[i]]])) {
          current_level[[parts[i]]] <- list()
        }
        current_level <- current_level[[parts[i]]]
      }
    }
  }
  
  return(json_obj)
}

# Function to pretty print JSON using jsonlite
pretty_print_json <- function(json_obj) {
  cat(jsonlite::toJSON(json_obj, pretty = TRUE))
}

# Example usage:
file_path <- '/mnt/data/example.json'

# Extract keys from the file
extracted_keys <- extract_keys_from_file(file_path)

# Reconstruct the JSON
reconstructed_json <- reconstruct_json_from_keys(extracted_keys)

# Pretty print the JSON
pretty_print_json(reconstructed_json)
