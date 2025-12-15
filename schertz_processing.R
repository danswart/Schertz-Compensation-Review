# Schertz Compensation Data Pipeline - Processing Functions
# ==========================================================
# Core data cleaning and transformation functions.
# These are called by the main workflow.
#
# Usage:
#   source("schertz_config.R")
#   source("schertz_processing.R")

library(readxl)
library(dplyr)
library(tidyr)
library(stringr)
library(janitor)

# ------------------------------------------------------------------------------
# TEXT CLEANING FUNCTIONS
# ------------------------------------------------------------------------------

#' Fix non-breaking spaces and other invisible Unicode spaces
#'
#' @param x Character vector
#' @return Cleaned character vector
fix_spaces <- function(x) {
  x |>
    # Non-breaking space (common in Excel exports)
    stringr::str_replace_all("\u00A0", " ") |>
    # Other Unicode spaces
    stringr::str_replace_all("\u2007", " ") |>
    stringr::str_replace_all("\u202F", " ") |>
    # Clean up multiple spaces
    stringr::str_squish()
}


#' Normalize Unicode hyphens and dashes to regular hyphen
#'
#' @param x Character vector
#' @return  Cleaned character vector
fix_hyphens <- function(x) {
  stringr::str_replace_all(
    x,
    "[\u2010\u2011\u2012\u2013\u2014\u2015\u2212]",
    "-"
  )
}


#' Standardize names for matching across years
#'
#' @param x Character vector of names
#' @return  Standardized names
standardize_name <- function(x) {
  x |>
    fix_spaces() |>
    fix_hyphens() |>
    # Remove middle initials (single letter + optional period at end)
    stringr::str_remove("\\s+[A-Z]\\.?$") |>
    # Remove trailing periods
    stringr::str_remove("\\.$") |>
    # Standardize Jr/Sr case
    stringr::str_replace("\\bjr\\b", "Jr") |>
    stringr::str_replace("\\bsr\\b", "Sr") |>
    stringr::str_replace("\\bJr,", "Jr.,") |>
    stringr::str_squish()
}


#' Apply manual name corrections from configuration
#'
#' @param names_vector Character vector of names
#' @param corrections  Tribble with pattern and replacement columns
#' @return Corrected names
apply_name_corrections <- function(
  names_vector,
  corrections = name_corrections
) {
  result <- names_vector

  for (i in base::seq_len(base::nrow(corrections))) {
    result <- stringr::str_replace(
      result,
      corrections$pattern[i],
      corrections$replacement[i]
    )
  }

  result
}


# ------------------------------------------------------------------------------
# DATA TYPE CONVERSION FUNCTIONS
# ------------------------------------------------------------------------------

#' Parse currency strings to numeric
#'
#' @param x Character vector of currency values
#' @return Numeric vector
parse_currency <- function(x) {
  x |>
    stringr::str_replace("^N/A$", NA_character_) |>
    stringr::str_remove_all("\\$") |>
    stringr::str_remove_all(",") |>
    stringr::str_remove_all("\\s") |>
    stringr::str_replace("^-$", "0") |>
    stringr::str_replace("^$", NA_character_) |>
    base::as.numeric()
}


#' Parse dates - handles both Excel serial numbers and text formats
#'
#' @param x Character vector of date values
#' @return Date vector
parse_flex_date <- function(x) {
  serial_dates <- base::suppressWarnings(base::as.numeric(x))
  is_serial <- !base::is.na(serial_dates) &
    serial_dates > 30000 &
    serial_dates < 50000

  result <- base::as.Date(base::rep(NA, base::length(x)))

  # Excel serial numbers (origin is 1899-12-30)
  result[is_serial] <- base::as.Date(
    serial_dates[is_serial],
    origin = "1899-12-30"
  )

  # Text dates in m/d/Y format
  text_dates <- base::suppressWarnings(
    base::as.Date(x[!is_serial], format = "%m/%d/%Y")
  )
  result[!is_serial] <- text_dates

  result
}


# ------------------------------------------------------------------------------
# FILE IMPORT AND STANDARDIZATION
# ------------------------------------------------------------------------------

#' Import and standardize a single Excel file
#'
#' @param fiscal_year Fiscal year label (e.g., "FY20")
#' @param filename Name of Excel file
#' @param structure_type Structure type from config (determines column mapping)
#' @param data_path Path to data directory
#' @return Standardized data frame
import_and_standardize <- function(
  fiscal_year,
  filename,
  structure_type,
  data_path = paths$data_raw
) {
  filepath <- base::file.path(data_path, filename)

  if (!base::file.exists(filepath)) {
    warning("File not found, skipping: ", filepath)
    return(NULL)
  }

  base::message("  Importing: ", fiscal_year, " (", filename, ")")

  # Import as text
  df <- readxl::read_excel(filepath, col_types = "text") |>
    janitor::clean_names()

  # Fix invisible characters across all text columns FIRST
  df <- df |>
    dplyr::mutate(dplyr::across(dplyr::where(is.character), fix_spaces))

  # Apply structure-specific standardization
  if (structure_type == "early") {
    result <- standardize_early_structure(df, fiscal_year)
  } else if (structure_type == "late") {
    result <- standardize_late_structure(df, fiscal_year)
  } else {
    stop("Unknown structure_type: ", structure_type)
  }

  result
}


#' Standardize early structure files (FY20-22 format)
#'
#' @param df Raw data frame
#' @param fiscal_year Fiscal year label
#' @return Standardized data frame
standardize_early_structure <- function(df, fiscal_year) {
  # Find the department column (could be "department" or "home_department")
  dept_col <- base::intersect(
    base::names(df),
    c("department", "home_department")
  )[1]

  df |>
    dplyr::transmute(
      name_original = name,
      department = .data[[dept_col]],
      job_title = job_title,
      hire_date = hire_date,
      separation_date = termination_date,
      regular_earnings = regular_earnings,
      overtime_earnings = overtime_earnings,
      additional_earnings = additional_earnings1,
      fiscal_year = fiscal_year
    )
}


#' Standardize late structure files (FY23-24 format)
#'
#' @param df Raw data frame
#' @param fiscal_year Fiscal year label
#' @return Standardized data frame
standardize_late_structure <- function(df, fiscal_year) {
  # Find earnings columns by pattern matching
  regular_col <- base::names(df)[stringr::str_detect(
    base::names(df),
    "regular_earnings|regular$"
  )][1]
  overtime_col <- base::names(df)[stringr::str_detect(
    base::names(df),
    "overtime_earnings|overtime$"
  )][1]
  additional_col <- base::names(df)[stringr::str_detect(
    base::names(df),
    "^fy\\d+_additional$|additional_earnings"
  )][1]

  df |>
    dplyr::transmute(
      name_original = base::paste0(last_name, ", ", first_name),
      department = home_department,
      job_title = job_title,
      hire_date = hire_date,
      separation_date = separation_date,
      regular_earnings = .data[[regular_col]],
      overtime_earnings = .data[[overtime_col]],
      additional_earnings = .data[[additional_col]],
      fiscal_year = fiscal_year
    )
}


# ------------------------------------------------------------------------------
# MAIN PROCESSING PIPELINE STAGES
# ------------------------------------------------------------------------------

#' Stage 1: Import and combine all files
#'
#' @param registry File registry from config
#' @param data_path Path to data directory
#' @return Combined data frame (text, before cleaning)
stage_1_import <- function(
  registry = file_registry,
  data_path = paths$data_raw
) {
  base::message("\n=== STAGE 1: Import and Combine Files ===\n")

  # Process each file
  all_data <- purrr::pmap(
    base::list(
      fiscal_year = registry$fiscal_year,
      filename = registry$filename,
      structure_type = registry$structure_type
    ),
    import_and_standardize,
    data_path = data_path
  )

  # Remove any NULLs (files that weren't found)
  all_data <- purrr::compact(all_data)

  # Combine
  combined <- dplyr::bind_rows(all_data)

  base::message(
    "\n  Combined ",
    base::nrow(combined),
    " rows from ",
    base::length(all_data),
    " files"
  )

  combined
}


#' Stage 2: Clean and transform (before corrections)
#'
#' @param df Combined data frame from Stage 1
#' @return Cleaned data frame with name_std (before manual corrections)
stage_2_clean <- function(df) {
  base::message("\n=== STAGE 2: Clean and Transform ===\n")

  result <- df |>
    dplyr::mutate(
      # Standardize names (automatic rules only)
      name_std = standardize_name(name_original),

      # Parse currency fields
      regular_earnings = parse_currency(regular_earnings),
      overtime_earnings = parse_currency(overtime_earnings),
      additional_earnings = parse_currency(additional_earnings),

      # Parse dates
      hire_date = parse_flex_date(hire_date),
      separation_date = parse_flex_date(separation_date),

      # Clean department for key creation
      dept_std = department |>
        stringr::str_to_lower() |>
        stringr::str_squish()
    )

  base::message("  Parsed ", base::nrow(result), " rows")
  base::message(
    "  Unique standardized names: ",
    dplyr::n_distinct(result$name_std)
  )

  result
}


#' Stage 3: Apply manual corrections and create keys
#'
#' @param df Cleaned data frame from Stage 2
#' @param corrections Name corrections from config
#' @return Data frame with corrections applied and employee_key created
stage_3_corrections_and_keys <- function(df, corrections = name_corrections) {
  base::message("\n=== STAGE 3: Apply Corrections and Create Keys ===\n")

  result <- df |>
    # Apply manual name corrections
    dplyr::mutate(
      name_std = apply_name_corrections(name_std, corrections)
    ) |>
    # Create composite employee key
    dplyr::mutate(
      employee_key = base::paste(
        stringr::str_to_lower(name_std),
        dept_std,
        hire_date,
        sep = "|"
      )
    )

  base::message("  Applied ", base::nrow(corrections), " correction rules")
  base::message(
    "  Created ",
    dplyr::n_distinct(result$employee_key),
    " unique employee keys"
  )

  result
}


#' Stage 4: Pivot to long format
#'
#' @param df Data frame from Stage 3
#' @return Long format data frame
stage_4_pivot_long <- function(df) {
  base::message("\n=== STAGE 4: Pivot to Long Format ===\n")

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

  base::message("  Pivoted to ", base::nrow(result), " rows")

  result
}


#' Stage 5: Prepare final dataset
#'
#' @param df Long format data frame from Stage 4
#' @return Final dataset with selected columns
stage_5_finalize <- function(df) {
  base::message("\n=== STAGE 5: Finalize Dataset ===\n")

  result <- df |>
    dplyr::select(
      employee_key,
      name = name_std,
      name_original,
      department,
      job_title,
      hire_date,
      separation_date,
      fiscal_year,
      earnings_type,
      amount
    ) |>
    dplyr::arrange(name, fiscal_year, earnings_type)

  base::message("  Final dataset: ", base::nrow(result), " rows")
  base::message("  Unique employees: ", dplyr::n_distinct(result$employee_key))
  base::message(
    "  Fiscal years: ",
    base::paste(base::sort(base::unique(result$fiscal_year)), collapse = ", ")
  )

  result
}


# ------------------------------------------------------------------------------
# CONVENIENCE FUNCTION: RUN ALL STAGES
# ------------------------------------------------------------------------------

#' Run the full pipeline with intermediate outputs for reconciliation
#'
#' @param registry File registry
#' @param corrections Name corrections
#' @param data_path Path to raw data
#' @return List with combined, cleaned, and final data frames
run_pipeline_with_intermediates <- function(
  registry = file_registry,
  corrections = name_corrections,
  data_path = paths$data_raw
) {
  base::message("\n")
  base::message(base::strrep("=", 70))
  base::message("  SCHERTZ COMPENSATION DATA PIPELINE")
  base::message("  Started: ", base::Sys.time())
  base::message(base::strrep("=", 70))

  # Stage 1: Import
  combined_df <- stage_1_import(registry, data_path)

  # Stage 2: Clean
  cleaned_df <- stage_2_clean(combined_df)

  # Stage 3: Corrections and keys
  keyed_df <- stage_3_corrections_and_keys(cleaned_df, corrections)

  # Stage 4: Pivot
  long_df <- stage_4_pivot_long(keyed_df)

  # Stage 5: Finalize
  final_df <- stage_5_finalize(long_df)

  base::message("\n")
  base::message(base::strrep("=", 70))
  base::message("  PIPELINE COMPLETE")
  base::message("  Finished: ", base::Sys.time())
  base::message(base::strrep("=", 70))
  base::message("\n")

  # Return all stages for reconciliation
  base::list(
    combined = combined_df,
    cleaned = keyed_df, # Has name_std and employee_key
    final = final_df
  )
}
