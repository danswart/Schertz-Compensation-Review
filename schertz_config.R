# ==============================================================================
# SCHERTZ COMPENSATION DATA PIPELINE - CONFIGURATION
# ==============================================================================
# 
# This config supports two modes:
#   1. EXPLICIT: Files listed in file_registry with specific structure types
#   2. AUTO-DETECT: New files auto-analyzed and registered via add_fiscal_year()
#
# To add a new fiscal year:
#   1. Place Excel file in data_raw/
#   2. Run: add_fiscal_year("YourNewFile.xlsx")
#   3. Review the output, then run the pipeline
#
# The auto-detection handles:
#   - Detecting fiscal year from filename or column names
#   - Mapping columns by flexible regex patterns
#   - Filtering out total/subtotal rows
#   - Flagging any columns it couldn't find
#
# ==============================================================================

# ------------------------------------------------------------------------------
# PATHS
# ------------------------------------------------------------------------------

paths <- list(

data_raw = here::here("data_raw"),
  data_clean = here::here("data_clean"),
  reports = here::here("reports")
)

# ------------------------------------------------------------------------------
# FILE REGISTRY
# ------------------------------------------------------------------------------
# Known files with their fiscal year and structure type
# structure_type: "early" (FY20-22), "late" (FY23-24), "fy25plus" (FY25+)

file_registry <- dplyr::tribble(
  ~fiscal_year, ~filename,                                  ~structure_type,
  "FY20",       "FY20 Earnings.xlsx",                       "early",
  "FY21",       "FY21 Earnings.xlsx",                       "early",
  "FY22",       "FY22 Earnings.xlsx",                       "early",
 "FY23",       "FY23Employee_Comp_FY23.xlsx",              "late",
  "FY24",       "FY24Employee_Comp_FY24_All.xlsx",          "late",
  "FY25",       "Employee_Compensation_Report_FY25.xlsx",   "fy25plus"
)

# ------------------------------------------------------------------------------
# COLUMN MAPPINGS
# ------------------------------------------------------------------------------
# Maps source columns to standardized names using regex patterns
# Patterns are matched against clean_names() output (lowercase, underscores)
#
# IMPORTANT: Patterns are tried in order; first match wins
# Use "|" for alternatives, "\\d+" for any digits

column_mappings <- list(
  
 # ============================================================================
  # EARLY STRUCTURE (FY20-FY22)
  # - Single "name" column (combined first/last)
  # - No benefits columns
  # - "termination_date" instead of "separation_date"
  # ============================================================================
  early = list(
    name_source         = "combined",
    name_col            = "^name$",
    department          = "^department$|^home_department$",
    job_title           = "^job_title$",
    hire_date           = "^hire_date$",
    separation_date     = "^termination_date$|^separation_date$",
    regular_earnings    = "^regular_earnings$",
    overtime_earnings   = "^overtime_earnings$",
    additional_earnings = "^additional_earnings",
    # These don't exist in early files - will be NA
    deployment_earnings = NULL,
    benefits            = NULL,
    total_compensation  = NULL,
    annual_salary       = NULL,
    leave_payout        = NULL
  ),
  
  # ============================================================================
  # LATE STRUCTURE (FY23-FY24)
  # - Separate first_name/last_name columns
  # - Has home_department (not department)
  # - FYxx_ prefix on some earnings columns
  # ============================================================================
  late = list(
    name_source         = "separate",
    last_name           = "^last_name$",
    first_name          = "^first_name$",
    department          = "^home_department$|^department$",
    job_title           = "^job_title$",
    hire_date           = "^hire_date$",
    separation_date     = "^separation_date$|^termination_date$",
    regular_earnings    = "fy\\d+_regular_earnings|^regular_earnings$",
    overtime_earnings   = "fy\\d+_overtime_earnings|^overtime_earnings$",
    additional_earnings = "^fy\\d+_additional$|^fy\\d+_additional_earnings",
    # These may or may not exist
    deployment_earnings = "fy\\d+_deployment",
    benefits            = "fy\\d+_additional_benefits|^benefits$",
    total_compensation  = "fy\\d+_total_compensation|^total_compensation$",
    annual_salary       = "fy\\d+_annual_salary|^annual_salary$",
    leave_payout        = "fy\\d+_leave_payout|^leave_payout$"
  ),
  
  # ============================================================================
  # FY25+ STRUCTURE
  # - Separate first_name/last_name columns  
  # - Uses "department" (not home_department)
  # - Full column names with fy25_ prefix
  # - Has position_id, employee_category, deployment, benefits, etc.
  # ============================================================================
  fy25plus = list(
    name_source         = "separate",
    last_name           = "^last_name$",
    first_name          = "^first_name$",
    position_id         = "^position_id$",
    department          = "^department$|^home_department$",
    job_title           = "^job_title$",
    employee_category   = "^employee_category$",
    hire_date           = "^hire_date$",
    separation_date     = "^separation_date$|^termination_date$",
    annual_salary       = "fy\\d+_annual_salary|^annual_salary$",
    leave_payout        = "fy\\d+_leave_payout|^leave_payout$",
    regular_earnings    = "fy\\d+_regular_earnings|^regular_earnings$",
    overtime_earnings   = "fy\\d+_overtime_earnings|^overtime_earnings$",
    additional_earnings = "fy\\d+_additional_earnings|^fy\\d+_additional$",
    deployment_earnings = "fy\\d+_deployment_earnings|^deployment",
    arbitration         = "arbitration|settlements",
    total_earnings      = "^total_earnings|fy\\d+_total_earnings",
    benefits            = "fy\\d+_additional_benefits|^additional_benefits",
    total_compensation  = "fy\\d+_total_compensation|^total_compensation$"
  )
)

# ------------------------------------------------------------------------------
# ROW FILTERS
# ------------------------------------------------------------------------------
# Patterns to identify and remove non-data rows (totals, subtotals, headers)
# Applied to the first column (typically last_name or name)

row_exclusion_patterns <- c(
  "^total",
  "^subtotal", 
  "^grand total",
  "^sum$",
  "^click",
  "^page \\d+",
  "^$"
)

# ------------------------------------------------------------------------------
# MANUAL NAME CORRECTIONS
# ------------------------------------------------------------------------------
# Applied AFTER automatic standardization
# Format: pattern (regex) -> replacement
# Add new corrections as you discover name variations across years

name_corrections <- dplyr::tribble(
  ~pattern,                    ~replacement,              ~note,
  # Example corrections - add your actual ones here
 "Busch Jr,",                 "Busch Jr.,",              "Standardize Jr suffix",
  "Lowery Jr\\.,",             "Lowery Jr.,",             "Standardize Jr suffix",
  "Fowler IV,",                "Fowler IV,",              "Keep IV suffix",
  "Ramirez Jr\\.,",            "Ramirez Jr.,",            "Standardize Jr suffix",
  "Maldonado Jr,",             "Maldonado Jr.,",          "Standardize Jr suffix",
  "Guerrero Jr,",              "Guerrero Jr.,",           "Standardize Jr suffix",
  "Rosas Jr,",                 "Rosas Jr.,",              "Standardize Jr suffix"
  # Add more as discovered during profiling
)

# ------------------------------------------------------------------------------
# VALIDATION SETTINGS
# ------------------------------------------------------------------------------
# Departments to use for validation totals (should exist in all years)

validation_departments <- c(
  "Police Department",
  "Fire Department",
  "EMS"
)

# Key positions that should appear in every fiscal year
key_positions <- c(
  "City Manager",
  "Police Chief",
  "Fire Chief",
  "EMS Chief"
)

# ------------------------------------------------------------------------------
# AUTO-DETECTION FUNCTIONS
# ------------------------------------------------------------------------------

#' Detect fiscal year from filename or column names
#' @param filename The Excel filename
#' @param col_names Column names from the file (after clean_names)
#' @return Character string like "FY25" or NA if not detected
detect_fiscal_year <- function(filename, col_names = NULL) {
  
 # Try filename first (e.g., "FY25_something.xlsx" or "Employee_Comp_FY25.xlsx")
  fy_match <- stringr::str_extract(filename, "(?i)fy\\d{2}")
  if (!is.na(fy_match)) {
    return(toupper(fy_match))
  }
  
  # Try column names (e.g., "fy25_regular_earnings")
  if (!is.null(col_names)) {
    fy_cols <- stringr::str_extract(col_names, "(?i)^fy\\d{2}")
    fy_cols <- fy_cols[!is.na(fy_cols)]
    if (length(fy_cols) > 0) {
      return(toupper(fy_cols[1]))
    }
  }
  
  # Try year in filename (e.g., "2025_Compensation.xlsx")
  year_match <- stringr::str_extract(filename, "20\\d{2}")
  if (!is.na(year_match)) {
    # Convert 2025 -> FY25
    return(paste0("FY", substr(year_match, 3, 4)))
  }
  
  return(NA_character_)
}


#' Detect structure type from column names
#' @param col_names Column names from the file (after clean_names)
#' @return Character: "early", "late", or "fy25plus"
detect_structure_type <- function(col_names) {
  
  has_separate_names <- all(c("last_name", "first_name") %in% col_names)
  has_combined_name <- "name" %in% col_names
  has_position_id <- "position_id" %in% col_names
  has_home_dept <- "home_department" %in% col_names
  has_dept <- "department" %in% col_names
  has_benefits <- any(stringr::str_detect(col_names, "benefit"))
  
  if (has_combined_name && !has_separate_names) {
    return("early")
  }
  
  if (has_separate_names && has_position_id) {
    return("fy25plus")
  }
  
  if (has_separate_names && has_home_dept) {
    return("late")
  }
  
  if (has_separate_names) {
    # Default newer files to fy25plus structure
    return("fy25plus")
  }
  
  # Fallback
  return("late")
}


#' Add a new fiscal year file to the registry
#' @param filename Name of Excel file in data_raw/
#' @param fiscal_year Optional override for fiscal year (auto-detected if NULL
#' @param structure_type Optional override for structure type (auto-detected if NULL)
#' @return Invisibly returns the updated registry; prints diagnostic info
add_fiscal_year <- function(filename, 
                            fiscal_year = NULL, 
                            structure_type = NULL) {
  
  filepath <- file.path(paths$data_raw, filename)
  
  if (!file.exists(filepath)) {
    stop("File not found: ", filepath)
  }
  
  # Read and clean column names
  raw_data <- readxl::read_excel(filepath)
  col_names <- names(janitor::clean_names(raw_data))
  
  # Auto-detect fiscal year if not provided
  if (is.null(fiscal_year)) {
    fiscal_year <- detect_fiscal_year(filename, col_names)
    if (is.na(fiscal_year)) {
      stop("Could not detect fiscal year from filename or columns. ",
           "Please provide fiscal_year parameter (e.g., 'FY26')")
    }
  }
  
  # Auto-detect structure if not provided
  if (is.null(structure_type)) {
    structure_type <- detect_structure_type(col_names)
  }
  
  # Check if already in registry
  if (fiscal_year %in% file_registry$fiscal_year) {
    message("\n", fiscal_year, " already exists in registry. Updating...")
    file_registry <<- file_registry |>
      dplyr::filter(fiscal_year != !!fiscal_year)
  }
  
  # Add to registry
  new_row <- dplyr::tibble(
    fiscal_year = fiscal_year,
    filename = filename,
    structure_type = structure_type
  )
  
  file_registry <<- dplyr::bind_rows(file_registry, new_row) |>
    dplyr::arrange(fiscal_year)
  
  # Print diagnostic info
  message("\n", paste(rep("=", 60), collapse = ""))
  message("ADDED TO FILE REGISTRY")
  message(paste(rep("=", 60), collapse = ""))
  message("  Fiscal Year:    ", fiscal_year)
  message("  Filename:       ", filename)
  message("  Structure Type: ", structure_type)
  message("  Total Rows:     ", nrow(raw_data))
  message("  Columns Found:  ", length(col_names))
  
  # Show column mapping results
  mapping <- column_mappings[[structure_type]]
  message("\n  Column Mapping Results:")
  
  for (field in names(mapping)) {
    if (is.null(mapping[[field]])) next
    if (field == "name_source") next
    
    pattern <- mapping[[field]]
    matches <- col_names[stringr::str_detect(col_names, pattern)]
    
    if (length(matches) > 0) {
      message("    ✓ ", field, " -> ", matches[1])
    } else {
      message("    ✗ ", field, " (no match for pattern: ", pattern, ")")
    }
  }
  
  # Check for unmapped columns (potential new data)
  all_patterns <- unlist(mapping[!sapply(mapping, is.null)])
  all_patterns <- all_patterns[all_patterns != "combined" & all_patterns != "separate"]
  
  unmapped <- col_names[!sapply(col_names, function(cn) {
    any(sapply(all_patterns, function(p) stringr::str_detect(cn, p)))
  })]
  
  if (length(unmapped) > 0) {
    message("\n  Unmapped Columns (review if important):")
    for (col in unmapped) {
      message("    ? ", col)
    }
  }
  
  message("\n", paste(rep("=", 60), collapse = ""))
  message("Next: Run the pipeline to process all files")
  message(paste(rep("=", 60), collapse = ""))
  
  invisible(file_registry)
}


#' Quick preview of a new file without adding it
#' @param filename Name of Excel file in data_raw/
#' @return Prints diagnostic info
preview_file <- function(filename) {
  
  filepath <- file.path(paths$data_raw, filename)
  
  if (!file.exists(filepath)) {
    stop("File not found: ", filepath)
  }
  
  raw_data <- readxl::read_excel(filepath)
  col_names <- names(janitor::clean_names(raw_data))
  
  message("\n", paste(rep("=", 60), collapse = ""))
  message("FILE PREVIEW: ", filename)
  message(paste(rep("=", 60), collapse = ""))
  message("\nDetected Fiscal Year: ", detect_fiscal_year(filename, col_names))
  message("Detected Structure:   ", detect_structure_type(col_names))
  message("Total Rows:           ", nrow(raw_data))
  message("\nColumn Names (after clean_names):")
  
  for (i in seq_along(col_names)) {
    message("  ", sprintf("%2d", i), ". ", col_names[i])
  }
  
  # Show sample of first column to check for total rows
  first_col <- raw_data[[1]]
  message("\nFirst column sample (checking for total rows):")
  message("  First 3:  ", paste(head(first_col, 3), collapse = ", "))
  message("  Last 3:   ", paste(tail(first_col, 3), collapse = ", "))
  
  message("\n", paste(rep("=", 60), collapse = ""))
}
