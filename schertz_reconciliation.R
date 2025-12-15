# Schertz Compensation Data Pipeline - Reconciliation Module
# ===========================================================
# Functions for audit-ready validation and reconciliation.
# Produces evidence that data is complete and accurate.
#
# Four Checkpoints:
#   A: Source Reconciliation - row counts and totals match Excel files
#   B: Name Variation Scan - catch spelling inconsistencies
#   C: Key Validation - composite keys are unique and valid
#   D: Completeness Verification - key entities present, totals match published
#
# Usage:
#   source("schertz_config.R")
#   source("schertz_reconciliation.R")
#
#   # Run full reconciliation
#   report <- run_reconciliation(raw_data_list, combined_df, final_df)

library(readxl)
library(dplyr)
library(tidyr)
library(stringr)
library(janitor)
library(knitr)

# ==============================================================================
# CHECKPOINT A: SOURCE RECONCILIATION
# ==============================================================================
# Verify that imported data matches source Excel files exactly

#' Calculate totals from raw Excel file (before any processing)
#'
#' @param filepath Path to Excel file
#' @return List with row_count and earnings_total
get_source_totals <- function(filepath) {
  df <- readxl::read_excel(filepath, col_types = "text") |>
    janitor::clean_names()

  # Find earnings columns
  earnings_cols <- names(df)[stringr::str_detect(
    names(df),
    "regular_earnings|overtime_earnings|additional_earnings|^fy\\d+_regular|^fy\\d+_overtime|^fy\\d+_additional$"
  )]

  # Parse and sum earnings
  parse_currency_simple <- function(x) {
    x |>
      stringr::str_replace_all("\\$|,|\\s", "") |>
      stringr::str_replace("^-$", "0") |>
      stringr::str_replace("^N/A$", NA_character_) |>
      base::as.numeric()
  }

  earnings_total <- 0
  for (col in earnings_cols) {
    col_total <- base::sum(parse_currency_simple(df[[col]]), na.rm = TRUE)
    earnings_total <- earnings_total + col_total
  }

  base::list(
    row_count = base::nrow(df),
    earnings_total = earnings_total,
    earnings_columns = earnings_cols
  )
}


#' Run Checkpoint A: Source Reconciliation
#'
#' @param combined_df Combined data frame (after import, before other processing)
#' @param registry File registry from config
#' @param data_path Path to source files
#' @return List with reconciliation results
checkpoint_a_source_reconciliation <- function(
  combined_df,
  registry = file_registry,
  data_path = paths$data_raw
) {
  base::message("\n=== CHECKPOINT A: Source Reconciliation ===\n")

  results <- purrr::pmap_dfr(
    base::list(
      fiscal_year = registry$fiscal_year,
      filename = registry$filename
    ),
    function(fiscal_year, filename) {
      filepath <- base::file.path(data_path, filename)

      # Source file totals
      source <- get_source_totals(filepath)

      # Imported data totals for this FY
      imported <- combined_df |>
        dplyr::filter(fiscal_year == !!fiscal_year)

      imported_rows <- base::nrow(imported)

      # Sum earnings from imported data (still as text at this point)
      parse_currency_simple <- function(x) {
        x |>
          stringr::str_replace_all("\\$|,|\\s", "") |>
          stringr::str_replace("^-$", "0") |>
          stringr::str_replace("^N/A$", NA_character_) |>
          stringr::str_replace_all("\u00A0", "") |>
          base::as.numeric()
      }

      imported_total <- base::sum(
        parse_currency_simple(imported$regular_earnings),
        parse_currency_simple(imported$overtime_earnings),
        parse_currency_simple(imported$additional_earnings),
        na.rm = TRUE
      )

      # Compare
      rows_match <- source$row_count == imported_rows
      # Allow small floating point variance
      totals_match <- base::abs(source$earnings_total - imported_total) < 1

      dplyr::tibble(
        fiscal_year = fiscal_year,
        filename = filename,
        source_rows = source$row_count,
        imported_rows = imported_rows,
        rows_match = rows_match,
        source_total = source$earnings_total,
        imported_total = imported_total,
        total_diff = imported_total - source$earnings_total,
        totals_match = totals_match
      )
    }
  )

  # Summary
  all_rows_match <- base::all(results$rows_match)
  all_totals_match <- base::all(results$totals_match)
  overall_pass <- all_rows_match & all_totals_match

  if (overall_pass) {
    base::message("PASS: All source files reconcile correctly.")
  } else {
    base::message("FAIL: Reconciliation discrepancies found!")
    if (!all_rows_match) {
      failed <- results |> dplyr::filter(!rows_match)
      base::message(
        "  Row count mismatches: ",
        base::paste(failed$fiscal_year, collapse = ", ")
      )
    }
    if (!all_totals_match) {
      failed <- results |> dplyr::filter(!totals_match)
      base::message(
        "  Total mismatches: ",
        base::paste(failed$fiscal_year, collapse = ", ")
      )
    }
  }

  base::list(
    checkpoint = "A",
    name = "Source Reconciliation",
    pass = overall_pass,
    details = results,
    summary = dplyr::tibble(
      check = c("Row counts match", "Totals match", "Overall"),
      result = c(
        base::ifelse(all_rows_match, "PASS", "FAIL"),
        base::ifelse(all_totals_match, "PASS", "FAIL"),
        base::ifelse(overall_pass, "PASS", "FAIL")
      )
    )
  )
}


# ==============================================================================
# CHECKPOINT B: NAME VARIATION SCAN
# ==============================================================================
# Detect spelling inconsistencies that could cause silent omissions

#' Find names that differ only by case
#'
#' @param names_vector Character vector of names
#' @return Tibble of case variations
find_case_variations <- function(names_vector) {
  dplyr::tibble(name = base::unique(names_vector)) |>
    dplyr::mutate(name_lower = stringr::str_to_lower(name)) |>
    dplyr::group_by(name_lower) |>
    dplyr::filter(dplyr::n() > 1) |>
    dplyr::arrange(name_lower) |>
    dplyr::ungroup()
}


#' Find similar names by edit distance
#'
#' @param names_vector Character vector of names
#' @param max_distance Maximum edit distance
#' @return Tibble of similar name pairs
find_similar_names <- function(names_vector, max_distance = 2) {
  unique_names <- base::unique(stats::na.omit(names_vector))

  if (base::length(unique_names) < 2) {
    return(dplyr::tibble(
      name1 = character(),
      name2 = character(),
      distance = integer()
    ))
  }

  similar_pairs <- base::data.frame(
    name1 = character(),
    name2 = character(),
    distance = integer(),
    stringsAsFactors = FALSE
  )

  for (i in base::seq_along(unique_names)) {
    for (j in base::seq_along(unique_names)) {
      if (j > i) {
        d <- utils::adist(unique_names[i], unique_names[j])
        if (d >= 1 && d <= max_distance) {
          similar_pairs <- base::rbind(
            similar_pairs,
            base::data.frame(
              name1 = unique_names[i],
              name2 = unique_names[j],
              distance = base::as.integer(d)
            )
          )
        }
      }
    }
  }

  dplyr::as_tibble(similar_pairs) |>
    dplyr::arrange(distance, name1)
}


#' Run Checkpoint B: Name Variation Scan
#'
#' @param df Data frame with name_std column
#' @param max_distance Maximum edit distance for similarity
#' @param corrections Current corrections from config (to show what's already handled)
#' @return List with scan results
checkpoint_b_name_variations <- function(
  df,
  max_distance = validation_thresholds$name_edit_distance_max,
  corrections = name_corrections
) {
  base::message("\n=== CHECKPOINT B: Name Variation Scan ===\n")

  # Get standardized names
  names_to_check <- base::unique(df$name_std)
  base::message(
    "Scanning ",
    base::length(names_to_check),
    " unique standardized names..."
  )

  # Find case variations
  case_vars <- find_case_variations(names_to_check)
  n_case_vars <- base::nrow(case_vars)

  # Find similar names
  similar <- find_similar_names(names_to_check, max_distance)
  n_similar <- base::nrow(similar)

  # Check which similar pairs might already be handled by corrections
  if (n_similar > 0 & base::nrow(corrections) > 0) {
    similar <- similar |>
      dplyr::mutate(
        possibly_corrected = purrr::map2_lgl(name1, name2, function(n1, n2) {
          base::any(purrr::map_lgl(
            corrections$pattern,
            ~ {
              stringr::str_detect(n1, .x) | stringr::str_detect(n2, .x)
            }
          ))
        })
      )
  } else if (n_similar > 0) {
    similar <- similar |>
      dplyr::mutate(possibly_corrected = FALSE)
  }

  # Summary
  if (n_case_vars == 0) {
    base::message("No case variations found.")
  } else {
    base::message("Found ", n_case_vars, " case variation(s).")
  }

  if (n_similar == 0) {
    base::message(
      "No similar name pairs found (edit distance 1-",
      max_distance,
      ")."
    )
  } else {
    n_uncorrected <- base::sum(!similar$possibly_corrected)
    base::message("Found ", n_similar, " similar name pair(s).")
    if (n_uncorrected > 0) {
      base::message(
        "  ",
        n_uncorrected,
        " may need review (not covered by existing corrections)."
      )
    }
  }

  # This checkpoint is informational - always "PASS" but with warnings
  has_issues <- (n_case_vars > 0) |
    (n_similar > 0 & base::any(!similar$possibly_corrected))

  base::list(
    checkpoint = "B",
    name = "Name Variation Scan",
    pass = TRUE, # Informational - doesn't fail pipeline
    has_warnings = has_issues,
    case_variations = case_vars,
    similar_names = similar,
    summary = dplyr::tibble(
      check = c(
        "Case variations",
        "Similar names (edit dist 1-2)",
        "Possibly needing review"
      ),
      count = c(
        n_case_vars,
        n_similar,
        base::ifelse(n_similar > 0, base::sum(!similar$possibly_corrected), 0)
      )
    )
  )
}


# ==============================================================================
# CHECKPOINT C: KEY VALIDATION
# ==============================================================================
# Verify composite keys are unique and valid

#' Run Checkpoint C: Key Validation
#'
#' @param df Data frame with employee_key and fiscal_year columns
#' @return List with validation results
checkpoint_c_key_validation <- function(df) {
  base::message("\n=== CHECKPOINT C: Key Validation ===\n")

  # Check for duplicate keys within same fiscal year
  dup_within_year <- df |>
    dplyr::group_by(fiscal_year, employee_key) |>
    dplyr::filter(dplyr::n() > 1) |>
    dplyr::ungroup() |>
    dplyr::distinct(fiscal_year, employee_key, name_std, department, job_title)

  n_dups <- base::nrow(dup_within_year)

  # Check for NA keys
  na_keys <- df |>
    dplyr::filter(base::is.na(employee_key) | employee_key == "NA|NA|NA")
  n_na_keys <- base::nrow(na_keys)

  # Key coverage stats
  key_coverage <- df |>
    dplyr::distinct(employee_key, fiscal_year) |>
    dplyr::group_by(employee_key) |>
    dplyr::summarise(
      n_years = dplyr::n(),
      years = base::paste(base::sort(fiscal_year), collapse = ", "),
      .groups = "drop"
    )

  coverage_summary <- key_coverage |>
    dplyr::count(n_years, name = "n_employees") |>
    dplyr::mutate(description = base::paste("Present in", n_years, "year(s)"))

  # Summary
  keys_valid <- (n_dups == 0) & (n_na_keys == 0)

  if (n_dups == 0) {
    base::message("No duplicate keys within fiscal years.")
  } else {
    base::message(
      "WARNING: Found ",
      n_dups,
      " duplicate key instances within fiscal years!"
    )
  }

  if (n_na_keys == 0) {
    base::message("No NA or invalid keys.")
  } else {
    base::message("WARNING: Found ", n_na_keys, " NA/invalid keys!")
  }

  base::message("Unique employee keys: ", dplyr::n_distinct(df$employee_key))

  base::list(
    checkpoint = "C",
    name = "Key Validation",
    pass = keys_valid,
    duplicates_within_year = dup_within_year,
    na_keys = na_keys,
    coverage_summary = coverage_summary,
    n_unique_keys = dplyr::n_distinct(df$employee_key),
    summary = dplyr::tibble(
      check = c("No duplicate keys within FY", "No NA/invalid keys", "Overall"),
      result = c(
        base::ifelse(n_dups == 0, "PASS", "FAIL"),
        base::ifelse(n_na_keys == 0, "PASS", "FAIL"),
        base::ifelse(keys_valid, "PASS", "FAIL")
      )
    )
  )
}


# ==============================================================================
# CHECKPOINT D: COMPLETENESS VERIFICATION
# ==============================================================================
# Verify key entities present and totals match published figures

#' Check if key entities are present in data
#'
#' @param df Final data frame
#' @param entities Key entities from config
#' @return Tibble with presence check results
check_key_entities <- function(df, entities = key_entities) {
  if (base::nrow(entities) == 0) {
    return(dplyr::tibble(
      name_pattern = character(),
      job_title_pattern = character(),
      expected_years = character(),
      found = logical(),
      years_found = character(),
      missing_years = character()
    ))
  }

  purrr::pmap_dfr(
    base::list(
      name_pattern = entities$name_pattern,
      job_title_pattern = entities$job_title_pattern,
      expected_years = entities$expected_years,
      note = entities$note
    ),
    function(name_pattern, job_title_pattern, expected_years, note) {
      # Parse expected years
      expected <- stringr::str_split(expected_years, ",\\s*")[[1]]

      # Find matching records
      matches <- df |>
        dplyr::filter(
          stringr::str_detect(
            name,
            stringr::regex(name_pattern, ignore_case = TRUE)
          ),
          stringr::str_detect(
            job_title,
            stringr::regex(job_title_pattern, ignore_case = TRUE)
          )
        ) |>
        dplyr::distinct(fiscal_year) |>
        dplyr::pull(fiscal_year)

      # Check coverage
      found_all <- base::all(expected %in% matches)
      missing <- base::setdiff(expected, matches)

      dplyr::tibble(
        name_pattern = name_pattern,
        job_title_pattern = job_title_pattern,
        note = note,
        expected_years = expected_years,
        found = found_all,
        years_found = base::paste(base::sort(matches), collapse = ", "),
        missing_years = base::ifelse(
          base::length(missing) > 0,
          base::paste(missing, collapse = ", "),
          ""
        )
      )
    }
  )
}


#' Compare totals to external published figures
#'
#' @param df Final data frame
#' @param external External totals from config
#' @param tolerance Acceptable variance as decimal
#' @return Tibble with comparison results
compare_external_totals <- function(
  df,
  external = external_totals,
  tolerance = validation_thresholds$external_total_tolerance
) {
  if (base::nrow(external) == 0) {
    return(dplyr::tibble(
      fiscal_year = character(),
      calculated_total = numeric(),
      published_total = numeric(),
      difference = numeric(),
      pct_diff = numeric(),
      within_tolerance = logical()
    ))
  }

  # Calculate totals from our data
  calculated <- df |>
    dplyr::group_by(fiscal_year) |>
    dplyr::summarise(
      calculated_total = base::sum(amount, na.rm = TRUE),
      .groups = "drop"
    )

  # Join with external
  comparison <- external |>
    dplyr::left_join(calculated, by = "fiscal_year") |>
    dplyr::mutate(
      difference = calculated_total - published_total,
      pct_diff = difference / published_total,
      within_tolerance = base::abs(pct_diff) <= tolerance
    )

  comparison
}


#' Run Checkpoint D: Completeness Verification
#'
#' @param df Final data frame (long format)
#' @param entities Key entities from config
#' @param external External totals from config
#' @return List with verification results
checkpoint_d_completeness <- function(
  df,
  entities = key_entities,
  external = external_totals
) {
  base::message("\n=== CHECKPOINT D: Completeness Verification ===\n")

  # Check key entities
  entity_check <- check_key_entities(df, entities)

  if (base::nrow(entity_check) > 0) {
    all_entities_found <- base::all(entity_check$found)
    n_missing <- base::sum(!entity_check$found)

    if (all_entities_found) {
      base::message(
        "All ",
        base::nrow(entity_check),
        " key entities found in expected years."
      )
    } else {
      base::message(
        "WARNING: ",
        n_missing,
        " key entity/year combination(s) missing!"
      )
      missing <- entity_check |> dplyr::filter(!found)
      for (i in base::seq_len(base::nrow(missing))) {
        base::message(
          "  - ",
          missing$name_pattern[i],
          " (",
          missing$job_title_pattern[i],
          ") missing from: ",
          missing$missing_years[i]
        )
      }
    }
  } else {
    all_entities_found <- TRUE
    base::message("No key entities defined in config.")
  }

  # Compare to external totals
  external_check <- compare_external_totals(df, external)

  if (base::nrow(external_check) > 0) {
    all_within_tolerance <- base::all(
      external_check$within_tolerance,
      na.rm = TRUE
    )

    if (all_within_tolerance) {
      base::message("All totals within tolerance of published figures.")
    } else {
      base::message("WARNING: Some totals differ from published figures!")
      outside <- external_check |> dplyr::filter(!within_tolerance)
      for (i in base::seq_len(base::nrow(outside))) {
        base::message(
          "  - ",
          outside$fiscal_year[i],
          ": ",
          scales::percent(outside$pct_diff[i], accuracy = 0.1),
          " difference"
        )
      }
    }
  } else {
    all_within_tolerance <- TRUE
    base::message("No external totals defined for comparison.")
  }

  # Calculate our own totals for reference
  our_totals <- df |>
    dplyr::group_by(fiscal_year) |>
    dplyr::summarise(
      n_employees = dplyr::n_distinct(employee_key),
      total_compensation = base::sum(amount, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::arrange(fiscal_year)

  overall_pass <- all_entities_found & all_within_tolerance

  base::list(
    checkpoint = "D",
    name = "Completeness Verification",
    pass = overall_pass,
    entity_check = entity_check,
    external_comparison = external_check,
    our_totals = our_totals,
    summary = dplyr::tibble(
      check = c("Key entities present", "Totals match published", "Overall"),
      result = c(
        base::ifelse(all_entities_found, "PASS", "FAIL"),
        base::ifelse(all_within_tolerance, "PASS", "PASS (no external data)"),
        base::ifelse(overall_pass, "PASS", "FAIL")
      )
    )
  )
}


# ==============================================================================
# MASTER RECONCILIATION FUNCTION
# ==============================================================================

#' Run all reconciliation checkpoints
#'
#' @param combined_df Combined data after import (before cleaning)
#' @param cleaned_df Cleaned data with name_std and employee_key
#' @param final_df Final long-format data
#' @param registry File registry from config
#' @param corrections Name corrections from config
#' @param entities Key entities from config
#' @param external External totals from config
#' @return List with all checkpoint results
run_reconciliation <- function(
  combined_df,
  cleaned_df,
  final_df,
  registry = file_registry,
  corrections = name_corrections,
  entities = key_entities,
  external = external_totals
) {
  base::message("\n")
  base::message(base::strrep("=", 70))
  base::message("  RECONCILIATION REPORT")
  base::message("  Generated: ", base::Sys.time())
  base::message(base::strrep("=", 70))

  # Run all checkpoints
  checkpoint_a <- checkpoint_a_source_reconciliation(combined_df, registry)
  checkpoint_b <- checkpoint_b_name_variations(
    cleaned_df,
    corrections = corrections
  )
  checkpoint_c <- checkpoint_c_key_validation(cleaned_df)
  checkpoint_d <- checkpoint_d_completeness(final_df, entities, external)

  # Overall status
  overall_pass <- checkpoint_a$pass & checkpoint_c$pass & checkpoint_d$pass

  base::message("\n")
  base::message(base::strrep("=", 70))
  base::message(
    "  OVERALL STATUS: ",
    base::ifelse(overall_pass, "PASS", "FAIL")
  )
  if (checkpoint_b$has_warnings) {
    base::message("  (Note: Name variations detected - review recommended)")
  }
  base::message(base::strrep("=", 70))
  base::message("\n")

  base::list(
    timestamp = base::Sys.time(),
    overall_pass = overall_pass,
    checkpoint_a = checkpoint_a,
    checkpoint_b = checkpoint_b,
    checkpoint_c = checkpoint_c,
    checkpoint_d = checkpoint_d
  )
}


# ==============================================================================
# REPORT GENERATION
# ==============================================================================

#' Generate HTML reconciliation report
#'
#' @param reconciliation_results Output from run_reconciliation()
#' @param output_path Directory to save report
#' @param test_mode Include "TEST" in filename
#' @return Path to generated report
generate_reconciliation_report <- function(
  reconciliation_results,
  output_path = paths$reports,
  test_mode = report_settings$test_mode
) {
  # Create output directory if needed
  if (!base::dir.exists(output_path)) {
    base::dir.create(output_path, recursive = TRUE)
  }

  # Generate filename
  date_str <- base::format(base::Sys.Date(), report_settings$date_format)
  test_suffix <- base::ifelse(test_mode, "_TEST", "")
  filename <- base::paste0(
    "reconciliation",
    test_suffix,
    "_",
    date_str,
    ".html"
  )
  filepath <- base::file.path(output_path, filename)

  # Build HTML content
  html_content <- build_reconciliation_html(reconciliation_results)

  # Write file
  base::writeLines(html_content, filepath)

  base::message("Reconciliation report saved to: ", filepath)
  filepath
}


#' Build HTML content for reconciliation report
#'
#' @param results Reconciliation results
#' @return HTML string
build_reconciliation_html <- function(results) {
  # Status styling
  pass_style <- "color: green; font-weight: bold;"
  fail_style <- "color: red; font-weight: bold;"
  warn_style <- "color: orange; font-weight: bold;"

  overall_status <- base::ifelse(
    results$overall_pass,
    base::paste0('<span style="', pass_style, '">PASS</span>'),
    base::paste0('<span style="', fail_style, '">FAIL</span>')
  )

  # Helper to make status badge
  status_badge <- function(pass) {
    if (pass) {
      base::paste0('<span style="', pass_style, '">PASS</span>')
    } else {
      base::paste0('<span style="', fail_style, '">FAIL</span>')
    }
  }

  # Helper to make HTML table
  df_to_html_table <- function(df) {
    if (base::nrow(df) == 0) {
      return("<p><em>No data</em></p>")
    }

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
      '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">',
      header,
      base::paste(rows, collapse = ""),
      "</table>"
    )
  }

  # Build HTML
  html <- base::paste0(
    '
<!DOCTYPE html>
<html>
<head>
  <title>Reconciliation Report</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 40px; }
    h1 { color: #333; }
    h2 { color: #555; border-bottom: 1px solid #ccc; padding-bottom: 5px; }
    h3 { color: #666; }
    table { margin: 10px 0; }
    th { background-color: #f0f0f0; text-align: left; }
    td, th { padding: 8px; border: 1px solid #ddd; }
    .summary-box { background-color: #f9f9f9; padding: 15px; margin: 20px 0; border-radius: 5px; }
    .pass { color: green; font-weight: bold; }
    .fail { color: red; font-weight: bold; }
    .warn { color: orange; font-weight: bold; }
  </style>
</head>
<body>

<h1>Schertz Compensation Data - Reconciliation Report</h1>
<p><strong>Generated:</strong> ',
    base::format(results$timestamp, "%Y-%m-%d %H:%M:%S"),
    '</p>

<div class="summary-box">
  <h2>Overall Status: ',
    overall_status,
    '</h2>
  <ul>
    <li>Checkpoint A (Source Reconciliation): ',
    status_badge(results$checkpoint_a$pass),
    '</li>
    <li>Checkpoint B (Name Variations): ',
    status_badge(results$checkpoint_b$pass),
    base::ifelse(
      results$checkpoint_b$has_warnings,
      ' <span class="warn">(warnings)</span>',
      ''
    ),
    '</li>
    <li>Checkpoint C (Key Validation): ',
    status_badge(results$checkpoint_c$pass),
    '</li>
    <li>Checkpoint D (Completeness): ',
    status_badge(results$checkpoint_d$pass),
    '</li>
  </ul>
</div>

<h2>Checkpoint A: Source Reconciliation</h2>
<p>Verifies that row counts and totals match source Excel files.</p>
',
    df_to_html_table(
      results$checkpoint_a$details |>
        dplyr::mutate(
          source_total = scales::dollar(source_total, accuracy = 1),
          imported_total = scales::dollar(imported_total, accuracy = 1),
          total_diff = scales::dollar(total_diff, accuracy = 1)
        ) |>
        dplyr::select(
          fiscal_year,
          filename,
          source_rows,
          imported_rows,
          rows_match,
          source_total,
          imported_total,
          totals_match
        )
    ),
    '

<h2>Checkpoint B: Name Variations</h2>
<p>Detects spelling inconsistencies that could cause silent omissions.</p>

<h3>Summary</h3>
',
    df_to_html_table(results$checkpoint_b$summary),
    '
'
  )

  # Add similar names if present
  if (base::nrow(results$checkpoint_b$similar_names) > 0) {
    n_to_show <- base::min(
      base::nrow(results$checkpoint_b$similar_names),
      report_settings$max_similar_names_display
    )

    html <- base::paste0(
      html,
      '
<h3>Similar Name Pairs (showing first ',
      n_to_show,
      ')</h3>
',
      df_to_html_table(
        results$checkpoint_b$similar_names |>
          dplyr::slice_head(n = n_to_show)
      ),
      '
'
    )
  }

  html <- base::paste0(
    html,
    '
<h2>Checkpoint C: Key Validation</h2>
<p>Verifies composite keys are unique and valid.</p>

<h3>Summary</h3>
',
    df_to_html_table(results$checkpoint_c$summary),
    '

<h3>Employee Coverage Across Years</h3>
',
    df_to_html_table(results$checkpoint_c$coverage_summary),
    '
'
  )

  # Add duplicates if present
  if (base::nrow(results$checkpoint_c$duplicates_within_year) > 0) {
    html <- base::paste0(
      html,
      '
<h3 class="fail">Duplicate Keys Within Fiscal Year</h3>
',
      df_to_html_table(results$checkpoint_c$duplicates_within_year),
      '
'
    )
  }

  html <- base::paste0(
    html,
    '
<h2>Checkpoint D: Completeness Verification</h2>
<p>Verifies key entities are present and totals match published figures.</p>

<h3>Key Entities Check</h3>
'
  )

  if (base::nrow(results$checkpoint_d$entity_check) > 0) {
    html <- base::paste0(
      html,
      df_to_html_table(results$checkpoint_d$entity_check)
    )
  } else {
    html <- base::paste0(
      html,
      '<p><em>No key entities defined in configuration.</em></p>'
    )
  }

  html <- base::paste0(
    html,
    '
<h3>External Totals Comparison</h3>
'
  )

  if (base::nrow(results$checkpoint_d$external_comparison) > 0) {
    html <- base::paste0(
      html,
      df_to_html_table(
        results$checkpoint_d$external_comparison |>
          dplyr::mutate(
            calculated_total = scales::dollar(calculated_total, accuracy = 1),
            published_total = scales::dollar(published_total, accuracy = 1),
            difference = scales::dollar(difference, accuracy = 1),
            pct_diff = scales::percent(pct_diff, accuracy = 0.1)
          )
      )
    )
  } else {
    html <- base::paste0(
      html,
      '<p><em>No external totals defined for comparison.</em></p>'
    )
  }

  html <- base::paste0(
    html,
    '
<h3>Calculated Totals by Fiscal Year</h3>
',
    df_to_html_table(
      results$checkpoint_d$our_totals |>
        dplyr::mutate(
          total_compensation = scales::dollar(total_compensation, accuracy = 1)
        )
    ),
    '

<hr>
<p><em>End of Reconciliation Report</em></p>

</body>
</html>
'
  )

  html
}
