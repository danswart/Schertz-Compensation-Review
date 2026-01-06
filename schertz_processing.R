# ==============================================================================
# SCHERTZ COMPENSATION DATA PIPELINE - PROCESSING FUNCTIONS
# ==============================================================================
#
# Core data processing functions for import, cleaning, and transformation.
# Works with auto-detection from schertz_config.R
#
# ==============================================================================

# ------------------------------------------------------------------------------
# UTILITY FUNCTIONS
# ------------------------------------------------------------------------------

#' Find column name matching a regex pattern
#' @param col_names Vector of column names
#' @param pattern Regex pattern to match
#' @return First matching column name, or NULL if no match
find_column <- function(col_names, pattern) {
  if (is.null(pattern)) return(NULL)
  matches <- col_names[stringr::str_detect(col_names, pattern)]
  if (length(matches) > 0) return(matches[1])
  return(NULL)
}


#' Safely extract column value, returning NA if column doesn't exist
#' @param df Data frame
#' @param col_name Column name (can be NULL)
#' @param as_char If TRUE, coerce to character (needed for bind_rows compatibility)
#' @return Column values or NA vector
safe_extract <- function(df, col_name, as_char = FALSE) {
  if (is.null(col_name) || !col_name %in% names(df)) {
    return(rep(NA_character_, nrow(df)))
  }
  val <- df[[col_name]]
  if (as_char) {
    return(as.character(val))
  }
  val
}


#' Parse currency strings to numeric
#' @param x Character vector with currency values
#' @return Numeric vector
parse_currency <- function(x) {
  if (is.numeric(x)) return(x)
  x |>
    stringr::str_remove_all("[$,]") |>
    stringr::str_trim() |>
    as.numeric()
}


#' Parse dates flexibly (handles multiple formats)
#' @param x Date values (character, numeric, or Date)
#' @return Date vector
parse_flex_date <- function(x) {
  if (inherits(x, "Date")) return(x)
  if (inherits(x, "POSIXct")) return(as.Date(x))
  
  # Handle Excel numeric dates
  if (is.numeric(x)) {
    return(as.Date(x, origin = "1899-12-30"))
  }
  
  # Character input - check if it's a numeric string (Excel date as character)
  if (is.character(x)) {
    # Try to detect if these are numeric Excel dates stored as character
    # Excel dates are typically 5-digit numbers (e.g., "45678")
    numeric_pattern <- "^\\d{5}$"
    if (all(is.na(x) | stringr::str_detect(x, numeric_pattern))) {
      numeric_dates <- suppressWarnings(as.numeric(x))
      if (!all(is.na(numeric_dates[!is.na(x)]))) {
        return(as.Date(numeric_dates, origin = "1899-12-30"))
      }
    }
  }
  
  # Try common date formats
  parsed <- lubridate::parse_date_time(
    x, 
    orders = c("mdy", "ymd", "dmy", "m/d/y", "y-m-d", "m/d/Y", "Y-m-d"),
    quiet = TRUE
  )
  as.Date(parsed)
}


#' Standardize names (automatic rules)
#' @param name_vec Vector of names
#' @return Standardized names
standardize_name <- function(name_vec) {
  name_vec |>
    # Remove invisible Unicode characters
    stringr::str_replace_all("[\u00A0\u200B\u200C\u200D\uFEFF]", " ") |>
    # Normalize whitespace
    stringr::str_squish() |>
    # Title case
    stringr::str_to_title() |>
    # Remove middle initials (", X" or ", X.")
    stringr::str_remove(",\\s+[A-Z]\\.?$") |>
    # Ensure proper comma spacing
    stringr::str_replace(",(?!\\s)", ", ") |>
    stringr::str_squish()
}


#' Apply manual name corrections from config
#' @param name_vec Vector of names
#' @param corrections Corrections tibble from config
#' @return Corrected names
apply_corrections <- function(name_vec, corrections) {
  result <- name_vec
  for (i in seq_len(nrow(corrections))) {
    result <- stringr::str_replace(
      result,
      corrections$pattern[i],
      corrections$replacement[i]
    )
  }
  result
}


#' Check if a row should be excluded (totals, subtotals, etc.)
#' @param values Vector of values from first column
#' @param patterns Exclusion patterns from config
#' @return Logical vector (TRUE = keep, FALSE = exclude)
is_data_row <- function(values, patterns = row_exclusion_patterns) {
  values_lower <- tolower(as.character(values))
  
  # Check each exclusion pattern
  exclude <- sapply(values_lower, function(v) {
    if (is.na(v) || v == "") return(TRUE)  # Exclude empty
    any(sapply(patterns, function(p) stringr::str_detect(v, p)))
  })
  
  !exclude
}


# ------------------------------------------------------------------------------
# IMPORT FUNCTIONS
# ------------------------------------------------------------------------------

#' Import and standardize a single Excel file
#' @param fiscal_year Fiscal year label (e.g., "FY25")
#' @param filename Excel filename
#' @param structure_type Structure type for column mapping
#' @param data_path Path to data directory
#' @return Standardized tibble
import_and_standardize <- function(fiscal_year, filename, structure_type, 
                                    data_path = paths$data_raw) {
  
  filepath <- file.path(data_path, filename)
  
  if (!file.exists(filepath)) {
    warning("File not found: ", filepath)
    return(NULL)
  }
  
  message("  Processing ", fiscal_year, ": ", filename)
  
 # Read raw data
  raw_data <- readxl::read_excel(filepath)
  col_names <- names(janitor::clean_names(raw_data))
  
  # Clean column names in the data frame
  df <- janitor::clean_names(raw_data)
  
  # Get mapping for this structure type
  mapping <- column_mappings[[structure_type]]
  
  if (is.null(mapping)) {
    stop("Unknown structure_type: ", structure_type)
  }
  
  # Find actual column names using patterns
  cols <- list()
  for (field in names(mapping)) {
    if (field == "name_source") {
      cols[[field]] <- mapping[[field]]
    } else {
      cols[[field]] <- find_column(col_names, mapping[[field]])
    }
  }
  
  # Build standardized data frame based on name structure
  if (cols$name_source == "combined") {
    # Early structure: single name column
    result <- dplyr::tibble(
      name_original = safe_extract(df, cols$name_col),
      last_name = NA_character_,
      first_name = NA_character_
    )
  } else {
    # Late/FY25+ structure: separate name columns
    last_vals <- safe_extract(df, cols$last_name)
    first_vals <- safe_extract(df, cols$first_name)
    result <- dplyr::tibble(
      name_original = paste0(last_vals, ", ", first_vals),
      last_name = last_vals,
      first_name = first_vals
    )
  }
  
  # Add common fields
  result <- result |>
    dplyr::mutate(
      # Core fields
      position_id = safe_extract(df, cols$position_id),
      department = safe_extract(df, cols$department),
      job_title = safe_extract(df, cols$job_title),
      employee_category = safe_extract(df, cols$employee_category),
      hire_date = safe_extract(df, cols$hire_date, as_char = TRUE),
      separation_date = safe_extract(df, cols$separation_date, as_char = TRUE),
      
      # Financial fields (extract as character for consistent bind_rows)
      annual_salary = safe_extract(df, cols$annual_salary, as_char = TRUE),
      leave_payout = safe_extract(df, cols$leave_payout, as_char = TRUE),
      regular_earnings = safe_extract(df, cols$regular_earnings, as_char = TRUE),
      overtime_earnings = safe_extract(df, cols$overtime_earnings, as_char = TRUE),
      additional_earnings = safe_extract(df, cols$additional_earnings, as_char = TRUE),
      deployment_earnings = safe_extract(df, cols$deployment_earnings, as_char = TRUE),
      arbitration = safe_extract(df, cols$arbitration, as_char = TRUE),
      total_earnings = safe_extract(df, cols$total_earnings, as_char = TRUE),
      benefits = safe_extract(df, cols$benefits, as_char = TRUE),
      total_compensation = safe_extract(df, cols$total_compensation, as_char = TRUE),
      
      # Metadata
      fiscal_year = fiscal_year,
      source_file = filename
    )
  
  # Filter out non-data rows (totals, subtotals, empty)
  name_col_for_filter <- if (cols$name_source == "combined") {
    result$name_original
  } else {
    result$last_name
  }
  
  result <- result |>
    dplyr::filter(is_data_row(name_col_for_filter))
  
  message("    -> ", nrow(result), " employee records")
  
  result
}


#' Process all files in the registry
#' @param registry File registry tibble
#' @param data_path Path to data directory
#' @return Combined tibble
process_all_files <- function(registry = file_registry, 
                               data_path = paths$data_raw) {
  
  message("\n=== STAGE 1: Import All Files ===\n")
  
  all_data <- purrr::pmap(
    list(
      fiscal_year = registry$fiscal_year,
      filename = registry$filename,
      structure_type = registry$structure_type
    ),
    import_and_standardize,
    data_path = data_path
  )
  
  # Remove NULLs (files not found)
  all_data <- purrr::compact(all_data)
  
  # Combine
  combined <- dplyr::bind_rows(all_data)
  
  message("\n  Combined ", nrow(combined), " rows from ", 
          length(all_data), " files")
  
  combined
}


# ------------------------------------------------------------------------------
# CLEANING FUNCTIONS
# ------------------------------------------------------------------------------

#' Clean combined data (Stage 2)
#' @param df Combined data from Stage 1
#' @return Cleaned tibble
clean_combined_data <- function(df) {
  
  message("\n=== STAGE 2: Clean and Transform ===\n")
  
  result <- df |>
    dplyr::mutate(
      # Standardize names
      name_std = standardize_name(name_original),
      
      # Parse currency fields
      annual_salary = parse_currency(annual_salary),
      leave_payout = parse_currency(leave_payout),
      regular_earnings = parse_currency(regular_earnings),
      overtime_earnings = parse_currency(overtime_earnings),
      additional_earnings = parse_currency(additional_earnings),
      deployment_earnings = parse_currency(deployment_earnings),
      arbitration = parse_currency(arbitration),
      total_earnings = parse_currency(total_earnings),
      benefits = parse_currency(benefits),
      total_compensation = parse_currency(total_compensation),
      
      # Parse dates
      hire_date = parse_flex_date(hire_date),
      separation_date = parse_flex_date(separation_date),
      
      # Clean department for key creation
      dept_std = department |>
        stringr::str_to_lower() |>
        stringr::str_squish(),
      
      # Make fiscal_year a factor with correct order
      fiscal_year = factor(fiscal_year, levels = sort(unique(fiscal_year)))
    )
  
  message("  Cleaned ", nrow(result), " rows")
  message("  Unique names (pre-correction): ", dplyr::n_distinct(result$name_std))
  
  result
}


#' Apply corrections and create employee keys (Stage 3)
#' @param df Cleaned data from Stage 2
#' @param corrections Name corrections from config
#' @return Tibble with corrections and keys
apply_corrections_and_keys <- function(df, corrections = name_corrections) {
  
  message("\n=== STAGE 3: Apply Corrections and Create Keys ===\n")
  
  result <- df |>
    dplyr::mutate(
      # Apply manual corrections
      name_std = apply_corrections(name_std, corrections),
      
      # Create composite employee key
      employee_key = paste(name_std, dept_std, sep = "|") |>
        tolower() |>
        stringr::str_squish()
    )
  
  message("  Unique names (post-correction): ", dplyr::n_distinct(result$name_std))
  message("  Unique employee keys: ", dplyr::n_distinct(result$employee_key))
  
  result
}


# ------------------------------------------------------------------------------
# PIVOT FUNCTIONS (for backward compatibility with existing analysis)
# ------------------------------------------------------------------------------

#' Pivot earnings to long format
#' @param df Wide format data
#' @return Long format with earnings_type and amount columns
pivot_to_long <- function(df) {
  
  message("\n=== STAGE 4: Pivot to Long Format ===\n")
  
  # Only pivot the three core earnings columns for compatibility
  result <- df |>
    tidyr::pivot_longer(
      cols = c(regular_earnings, overtime_earnings, additional_earnings),
      names_to = "earnings_type",
      values_to = "amount"
    ) |>
    dplyr::mutate(
      earnings_type = earnings_type |>
        stringr::str_remove("_earnings$") |>
        stringr::str_to_title()
    )
  
  message("  Pivoted to ", nrow(result), " rows")
  
  result
}


# ------------------------------------------------------------------------------
# MASTER PIPELINE FUNCTION
# ------------------------------------------------------------------------------

#' Run the full pipeline
#' @param registry File registry (default: file_registry from config)
#' @param corrections Name corrections (default: name_corrections from config)
#' @param data_path Path to raw data
#' @param output_format "wide" (default) or "long" 
#' @return List with combined, cleaned, and final data frames
run_pipeline <- function(registry = file_registry,
                          corrections = name_corrections,
                          data_path = paths$data_raw,
                          output_format = "wide") {
  
  message("\n", paste(rep("=", 60), collapse = ""))
  message("SCHERTZ COMPENSATION DATA PIPELINE")
  message(paste(rep("=", 60), collapse = ""))
  message("Processing ", nrow(registry), " fiscal years: ", 
          paste(registry$fiscal_year, collapse = ", "))
  
  # Stage 1: Import
  combined <- process_all_files(registry, data_path)
  
  # Stage 2: Clean
  cleaned <- clean_combined_data(combined)
  
  # Stage 3: Corrections and Keys
  with_keys <- apply_corrections_and_keys(cleaned, corrections)
  
  # Stage 4: Pivot (optional)
  if (output_format == "long") {
    final <- pivot_to_long(with_keys)
  } else {
    final <- with_keys
  }
  
  message("\n", paste(rep("=", 60), collapse = ""))
  message("PIPELINE COMPLETE")
  message(paste(rep("=", 60), collapse = ""))
  message("  Final dataset: ", nrow(final), " rows")
  message("  Unique employees: ", dplyr::n_distinct(final$employee_key))
  message("  Fiscal years: ", paste(levels(final$fiscal_year), collapse = ", "))
  message(paste(rep("=", 60), collapse = ""))
  
  # Return all stages for inspection
  list(
    combined = combined,
    cleaned = cleaned,
    with_keys = with_keys,
    final = final
  )
}


# ------------------------------------------------------------------------------
# CONVENIENCE FUNCTION FOR ADDING NEW YEARS
# ------------------------------------------------------------------------------

#' One-step function to add and process a new fiscal year
#' @param filename Excel filename in data_raw/
#' @param run_full_pipeline If TRUE, runs entire pipeline after adding
#' @return Pipeline results if run_full_pipeline=TRUE, else updated registry
add_and_process <- function(filename, run_full_pipeline = TRUE) {
  
  # Add to registry (auto-detects FY and structure)
  add_fiscal_year(filename)
  
  if (run_full_pipeline) {
    # Run full pipeline
    return(run_pipeline())
  }
  
  invisible(file_registry)
}
