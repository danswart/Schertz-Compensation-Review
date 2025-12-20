# Schertz Compensation Data Pipeline - Cross-Year Consistency Module
# ====================================================================
# Detects data quality issues in SOURCE FILES by comparing categorical
# values (departments, job titles, etc.) across fiscal years.
#
# Key insight: If a department exists in FY22 and FY24 but NOT FY23,
# that's almost certainly a source data error, not a real reorganization.
#
# This module is GENERALIZED - it works with any column name, so you
# can reuse it for other projects with different data structures.
#
# Usage:
#   source("schertz_consistency.R")
#   
#   # Check departments across years
#   dept_check <- check_cross_year_consistency(
#     df = final_df,
#     value_col = "department",
#     year_col = "fiscal_year"
#   )
#   
#   # Check job titles across years
#   title_check <- check_cross_year_consistency(
#     df = final_df,
#     value_col = "job_title",
#     year_col = "fiscal_year"
#   )

library(dplyr)
library(tidyr)
library(stringr)
library(purrr)

# ==============================================================================
# CORE CONSISTENCY FUNCTIONS
# ==============================================================================

#' Build a presence matrix showing which values appear in which years
#'
#' @param df Data frame to analyze
#' @param value_col Name of the column containing values to check (e.g., "department")
#' @param year_col Name of the column containing fiscal years
#' @return A tibble with one row per unique value and columns for each year (TRUE/FALSE)
#'
#' @examples
#' build_presence_matrix(final_df, "department", "fiscal_year")
build_presence_matrix <- function(df, value_col, year_col) {
  # Get all unique values and years
  all_values <- base::sort(base::unique(df[[value_col]]))
  all_years <- base::sort(base::unique(df[[year_col]]))
  
  # For each value, check which years it appears in
  presence <- purrr::map_dfr(all_values, function(val) {
    years_present <- df |>
      dplyr::filter(.data[[value_col]] == val) |>
      dplyr::distinct(.data[[year_col]]) |>
      dplyr::pull(.data[[year_col]])
    
    # Build a row with TRUE/FALSE for each year
    row <- dplyr::tibble(value = val)
    for (yr in all_years) {
      row[[yr]] <- yr %in% years_present
    }
    row
  })
  
  # Rename the 'value' column to match the input
  names(presence)[1] <- value_col
  
  presence
}


#' Detect "gaps" - values present before and after a year, but missing in that year
#'
#' This is the key check: if a department exists in FY22 and FY24 but NOT FY23,
#' that's almost certainly a data error.
#'
#' @param presence_matrix Output from build_presence_matrix()
#' @param value_col Name of the value column
#' @return A tibble of gaps found, with columns: value, gap_year, year_before, year_after
detect_gaps <- function(presence_matrix, value_col) {
  # Get year columns (all columns except the value column)
  year_cols <- base::setdiff(names(presence_matrix), value_col)
  
  # Sort years to ensure chronological order
  year_cols <- base::sort(year_cols)
  
  gaps <- dplyr::tibble(
    value = character(),
    gap_year = character(),
    year_before = character(),
    year_after = character()
  )
  
  # For each value, look for gaps
 for (i in base::seq_len(base::nrow(presence_matrix))) {
    val <- presence_matrix[[value_col]][i]
    
    # Get the presence pattern as a logical vector
    pattern <- base::as.logical(presence_matrix[i, year_cols])
    
    # Look for FALSE values that have TRUE on both sides
    for (j in 2:(base::length(pattern) - 1)) {
      if (!pattern[j] && pattern[j-1] && pattern[j+1]) {
        # Found a gap!
        gaps <- dplyr::bind_rows(gaps, dplyr::tibble(
          value = val,
          gap_year = year_cols[j],
          year_before = year_cols[j-1],
          year_after = year_cols[j+1]
        ))
      }
    }
    
    # Also check for multi-year gaps (e.g., present in FY20 and FY24, missing FY21-23)
    # Find first and last TRUE
    first_true <- base::which(pattern)[1]
    last_true <- utils::tail(base::which(pattern), 1)
    
    if (!base::is.na(first_true) && !base::is.na(last_true) && last_true > first_true) {
      # Check for any FALSE values between first and last TRUE
      for (j in (first_true + 1):(last_true - 1)) {
        if (!pattern[j]) {
          # Check if we already recorded this gap (avoid duplicates)
          existing <- gaps |>
            dplyr::filter(value == val, gap_year == year_cols[j])
          
          if (base::nrow(existing) == 0) {
            # Find the actual year before with TRUE (might not be j-1)
            before_idx <- base::max(base::which(pattern[1:(j-1)]))
            after_idx <- base::min(base::which(pattern[(j+1):base::length(pattern)])) + j
            
            gaps <- dplyr::bind_rows(gaps, dplyr::tibble(
              value = val,
              gap_year = year_cols[j],
              year_before = year_cols[before_idx],
              year_after = year_cols[after_idx]
            ))
          }
        }
      }
    }
  }
  
  # Rename value column
  names(gaps)[1] <- value_col
  
  gaps |>
    dplyr::distinct() |>
    dplyr::arrange(.data[[value_col]], gap_year)
}


#' Compare values between consecutive years
#'
#' Shows what appeared, disappeared, or stayed the same between each pair of years.
#'
#' @param presence_matrix Output from build_presence_matrix()
#' @param value_col Name of the value column
#' @return A tibble with year transitions and what changed
compare_consecutive_years <- function(presence_matrix, value_col) {
  year_cols <- base::sort(base::setdiff(names(presence_matrix), value_col))
  
  comparisons <- dplyr::tibble(
    from_year = character(),
    to_year = character(),
    change_type = character(),
    values = character()
  )
  
  for (i in 1:(base::length(year_cols) - 1)) {
    year_from <- year_cols[i]
    year_to <- year_cols[i + 1]
    
    # Values in year_from
    in_from <- presence_matrix[[value_col]][presence_matrix[[year_from]] == TRUE]
    # Values in year_to
    in_to <- presence_matrix[[value_col]][presence_matrix[[year_to]] == TRUE]
    
    # What disappeared?
    disappeared <- base::setdiff(in_from, in_to)
    if (base::length(disappeared) > 0) {
      comparisons <- dplyr::bind_rows(comparisons, dplyr::tibble(
        from_year = year_from,
        to_year = year_to,
        change_type = "DISAPPEARED",
        values = base::paste(disappeared, collapse = "; ")
      ))
    }
    
    # What appeared?
    appeared <- base::setdiff(in_to, in_from)
    if (base::length(appeared) > 0) {
      comparisons <- dplyr::bind_rows(comparisons, dplyr::tibble(
        from_year = year_from,
        to_year = year_to,
        change_type = "APPEARED",
        values = base::paste(appeared, collapse = "; ")
      ))
    }
    
    # What stayed?
    stayed <- base::intersect(in_from, in_to)
    comparisons <- dplyr::bind_rows(comparisons, dplyr::tibble(
      from_year = year_from,
      to_year = year_to,
      change_type = "CONTINUED",
      values = base::paste0(base::length(stayed), " values continued")
    ))
  }
  
  comparisons
}


#' Get summary statistics for cross-year presence
#'
#' @param presence_matrix Output from build_presence_matrix()
#' @param value_col Name of the value column
#' @return A tibble with summary stats
summarize_presence <- function(presence_matrix, value_col) {
  year_cols <- base::setdiff(names(presence_matrix), value_col)
  n_years <- base::length(year_cols)
  
  presence_matrix |>
    dplyr::mutate(
      years_present = base::rowSums(dplyr::across(dplyr::all_of(year_cols))),
      coverage = years_present / n_years,
      first_year = purrr::pmap_chr(
        dplyr::across(dplyr::all_of(year_cols)),
        function(...) {
          vals <- c(...)
          yr <- year_cols[base::which(vals)[1]]
          base::ifelse(base::is.na(yr), NA_character_, yr)
        }
      ),
      last_year = purrr::pmap_chr(
        dplyr::across(dplyr::all_of(year_cols)),
        function(...) {
          vals <- c(...)
          yr <- utils::tail(year_cols[base::which(vals)], 1)
          base::ifelse(base::length(yr) == 0, NA_character_, yr)
        }
      )
    ) |>
    dplyr::select(
      dplyr::all_of(value_col),
      years_present,
      coverage,
      first_year,
      last_year,
      dplyr::everything()
    ) |>
    dplyr::arrange(dplyr::desc(years_present), .data[[value_col]])
}


# ==============================================================================
# MAIN CONSISTENCY CHECK FUNCTION
# ==============================================================================

#' Run comprehensive cross-year consistency check on a column
#'
#' This is the main function you'll use. It runs all checks and returns
#' structured results.
#'
#' @param df Data frame to analyze
#' @param value_col Name of the column to check (e.g., "department", "job_title")
#' @param year_col Name of the fiscal year column
#' @param label Human-readable label for reports (e.g., "Departments", "Job Titles")
#' @return A list with all check results
#'
#' @examples
#' dept_check <- check_cross_year_consistency(final_df, "department", "fiscal_year", "Departments")
check_cross_year_consistency <- function(df, 
                                          value_col, 
                                          year_col, 
                                          label = NULL) {
  if (base::is.null(label)) {
    label <- value_col
  }
  
  base::message("\n=== Cross-Year Consistency Check: ", label, " ===\n")
  
  # Build presence matrix
  presence <- build_presence_matrix(df, value_col, year_col)
  base::message("Found ", base::nrow(presence), " unique ", tolower(label))
  
  # Detect gaps (the critical check)
  gaps <- detect_gaps(presence, value_col)
  n_gaps <- base::nrow(gaps)
  
  if (n_gaps > 0) {
    base::message("CRITICAL: Found ", n_gaps, " gap(s) - values missing from intermediate years!")
    for (i in base::seq_len(base::nrow(gaps))) {
      base::message("  - '", gaps[[value_col]][i], "' missing from ", gaps$gap_year[i],
                    " (present in ", gaps$year_before[i], " and ", gaps$year_after[i], ")")
    }
  } else {
    base::message("No gaps detected (good)")
  }
  
  # Compare consecutive years
  comparisons <- compare_consecutive_years(presence, value_col)
  
  # Show disappearances
  disappearances <- comparisons |> 
    dplyr::filter(change_type == "DISAPPEARED")
  
  if (base::nrow(disappearances) > 0) {
    base::message("\nDisappearances between years:")
    for (i in base::seq_len(base::nrow(disappearances))) {
      base::message("  ", disappearances$from_year[i], " → ", disappearances$to_year[i], 
                    ": ", disappearances$values[i])
    }
  }
  
  # Get summary
  summary_stats <- summarize_presence(presence, value_col)
  
  # Determine pass/fail
  # Gaps are CRITICAL failures - these are almost always source data errors
  has_critical_gaps <- n_gaps > 0
  
  base::list(
    column_checked = value_col,
    label = label,
    pass = !has_critical_gaps,
    has_gaps = has_critical_gaps,
    n_gaps = n_gaps,
    gaps = gaps,
    presence_matrix = presence,
    comparisons = comparisons,
    summary = summary_stats
  )
}


# ==============================================================================
# EMPLOYEE-LEVEL CONSISTENCY CHECK
# ==============================================================================

#' Check for employees with suspicious gaps in their tenure
#'
#' If an employee appears in FY20, FY21, FY22, and FY24 but NOT FY23,
#' that's suspicious - especially if they have the same job title throughout.
#'
#' @param df Data frame with employee data
#' @param key_col Employee identifier column (e.g., "employee_key")
#' @param year_col Fiscal year column
#' @param name_col Name column for display
#' @return List with gap analysis results
check_employee_gaps <- function(df,
                                 key_col = "employee_key",
                                 year_col = "fiscal_year",
                                 name_col = "name") {
  
  base::message("\n=== Employee Gap Check ===\n")
  
  # Get unique employees per year
  employee_years <- df |>
    dplyr::distinct(.data[[key_col]], .data[[year_col]], .data[[name_col]])
  
  # Build presence matrix for employees
  presence <- build_presence_matrix(employee_years, key_col, year_col)
  
  # Detect gaps
  gaps <- detect_gaps(presence, key_col)
  
  if (base::nrow(gaps) > 0) {
    # Join with names for readability
    gaps_with_names <- gaps |>
      dplyr::left_join(
        df |> dplyr::distinct(.data[[key_col]], .data[[name_col]]),
        by = key_col
      ) |>
      dplyr::select(dplyr::all_of(name_col), dplyr::everything())
    
    base::message("Found ", base::nrow(gaps), " employee(s) with gap years")
    
    base::list(
      pass = FALSE,
      n_gaps = base::nrow(gaps),
      gaps = gaps_with_names,
      presence_matrix = presence
    )
  } else {
    base::message("No employee gaps detected")
    
    base::list(
      pass = TRUE,
      n_gaps = 0,
      gaps = gaps,
      presence_matrix = presence
    )
  }
}


# ==============================================================================
# MULTI-COLUMN CONSISTENCY REPORT
# ==============================================================================

#' Run consistency checks on multiple columns and produce summary
#'
#' @param df Data frame to analyze
#' @param columns_to_check Named vector of columns to check, e.g., 
#'   c("Departments" = "department", "Job Titles" = "job_title")
#' @param year_col Fiscal year column name
#' @return List with all check results and overall status
run_consistency_checks <- function(df,
                                    columns_to_check,
                                    year_col = "fiscal_year") {
  
  base::message("\n")
  base::message(base::strrep("=", 70))
  base::message("  CROSS-YEAR CONSISTENCY VALIDATION")
  base::message("  Checking for source data omissions and anomalies")
  base::message(base::strrep("=", 70))
  
  results <- base::list()
  all_pass <- TRUE
  total_gaps <- 0
  
  for (label in names(columns_to_check)) {
    col <- columns_to_check[[label]]
    check <- check_cross_year_consistency(df, col, year_col, label)
    results[[col]] <- check
    
    if (!check$pass) {
      all_pass <- FALSE
    }
    total_gaps <- total_gaps + check$n_gaps
  }
  
  base::message("\n")
  base::message(base::strrep("=", 70))
  if (all_pass) {
    base::message("  CONSISTENCY CHECK: PASS")
  } else {
    base::message("  CONSISTENCY CHECK: FAIL - ", total_gaps, " gap(s) found")
  }
  base::message(base::strrep("=", 70))
  base::message("\n")
  
  base::list(
    overall_pass = all_pass,
    total_gaps = total_gaps,
    checks = results
  )
}


# ==============================================================================
# REPORT GENERATION
# ==============================================================================

#' Generate HTML report for consistency checks
#'
#' @param consistency_results Output from run_consistency_checks()
#' @param output_path Directory to save report
#' @param filename Base filename (without extension)
#' @return Path to generated report
generate_consistency_report <- function(consistency_results,
                                         output_path,
                                         filename = "consistency_report") {
  
  # Create output directory if needed
  if (!base::dir.exists(output_path)) {
    base::dir.create(output_path, recursive = TRUE)
  }
  
  date_str <- base::format(base::Sys.Date(), "%Y-%m-%d")
  filepath <- base::file.path(output_path, base::paste0(filename, "_", date_str, ".html"))
  
  # Build HTML
  html <- build_consistency_html(consistency_results)
  
  base::writeLines(html, filepath)
  base::message("Consistency report saved to: ", filepath)
  
  filepath
}


#' Build HTML content for consistency report
#'
#' @param results Consistency check results
#' @return HTML string
build_consistency_html <- function(results) {
  
  pass_style <- "color: green; font-weight: bold;"
  fail_style <- "color: red; font-weight: bold;"
  
  overall_status <- base::ifelse(
    results$overall_pass,
    base::paste0('<span style="', pass_style, '">PASS</span>'),
    base::paste0('<span style="', fail_style, '">FAIL - ', results$total_gaps, ' gaps found</span>')
  )
  
  # Helper for HTML tables
  df_to_html <- function(df, max_rows = 50) {
    if (base::is.null(df) || base::nrow(df) == 0) {
      return("<p><em>No data</em></p>")
    }
    
    df <- utils::head(df, max_rows)
    
    header <- base::paste0(
      "<tr>",
      base::paste0("<th>", base::names(df), "</th>", collapse = ""),
      "</tr>"
    )
    
    rows <- base::apply(df, 1, function(row) {
      base::paste0(
        "<tr>",
        base::paste0("<td>", base::as.character(row), "</td>", collapse = ""),
        "</tr>"
      )
    })
    
    base::paste0(
      '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; margin: 10px 0;">',
      header,
      base::paste(rows, collapse = ""),
      "</table>"
    )
  }
  
  # Build sections for each checked column
  sections <- ""
  for (col in names(results$checks)) {
    check <- results$checks[[col]]
    
    status <- base::ifelse(
      check$pass,
      base::paste0('<span style="', pass_style, '">PASS</span>'),
      base::paste0('<span style="', fail_style, '">FAIL - ', check$n_gaps, ' gaps</span>')
    )
    
    sections <- base::paste0(sections, '
<h2>', check$label, ': ', status, '</h2>
')
    
    if (check$n_gaps > 0) {
      sections <- base::paste0(sections, '
<h3 style="color: red;">CRITICAL: Gaps Detected</h3>
<p>These values are present before AND after the gap year, but missing from that year. 
This is almost always a source data error.</p>
', df_to_html(check$gaps), '
')
    }
    
    # Add presence matrix (transposed for readability if small)
    sections <- base::paste0(sections, '
<h3>Presence Matrix</h3>
<p>Shows which ', tolower(check$label), ' appear in which years (TRUE = present).</p>
', df_to_html(check$presence_matrix), '
')
    
    # Add comparisons
    sections <- base::paste0(sections, '
<h3>Year-over-Year Changes</h3>
', df_to_html(check$comparisons), '
')
  }
  
  # Assemble full HTML
  html <- base::paste0('
<!DOCTYPE html>
<html>
<head>
  <title>Cross-Year Consistency Report</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 40px; max-width: 1200px; }
    h1 { color: #333; }
    h2 { color: #555; border-bottom: 1px solid #ccc; padding-bottom: 5px; margin-top: 30px; }
    h3 { color: #666; }
    table { margin: 10px 0; font-size: 14px; }
    th { background-color: #f0f0f0; text-align: left; }
    td, th { padding: 6px 10px; border: 1px solid #ddd; }
    .summary-box { background-color: #f9f9f9; padding: 15px; margin: 20px 0; border-radius: 5px; }
  </style>
</head>
<body>

<h1>Cross-Year Consistency Report</h1>
<p><strong>Generated:</strong> ', base::format(base::Sys.time(), "%Y-%m-%d %H:%M:%S"), '</p>

<div class="summary-box">
  <h2>Overall Status: ', overall_status, '</h2>
  <p>This report checks whether categorical values (departments, job titles, etc.) 
  are consistent across fiscal years. <strong>Gaps</strong> - where a value appears 
  before and after a year but is missing from that year - are flagged as critical errors.</p>
</div>

', sections, '

<hr>
<p><em>End of Consistency Report</em></p>

</body>
</html>
')
  
  html
}
