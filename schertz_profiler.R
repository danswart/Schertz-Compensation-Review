# Schertz Compensation Data Pipeline - Profiling Functions
# =========================================================
# These functions examine Excel files to surface potential issues
# BEFORE incorporating them into the main dataset.
#
# Usage:
#   source("schertz_config.R")
#   source("schertz_profiler.R")
#   
#   # Profile a single new file
#   profile <- profile_new_file("FY25_Earnings.xlsx")

library(readxl)
library(dplyr)
library(tidyr)
library(stringr)
library(janitor)
library(purrr)

# ------------------------------------------------------------------------------
# CORE PROFILING FUNCTIONS
# ------------------------------------------------------------------------------

#' Profile all text columns for potential issues
#' 
#' @param df A data frame to profile
#' @return A tibble with one row per text column
profile_text_columns <- function(df) {
  text_cols <- names(df)[base::sapply(df, is.character)]
  
  purrr::map_dfr(text_cols, function(col) {
    x <- df[[col]]
    
    dplyr::tibble(
      column = col,
      n_values = base::length(x),
      n_unique = dplyr::n_distinct(x, na.rm = TRUE),
      n_na = base::sum(base::is.na(x)),
      pct_na = base::round(base::mean(base::is.na(x)) * 100, 1),
      has_nbsp = base::any(stringr::str_detect(x, "\u00A0"), na.rm = TRUE),
      has_unicode_hyphen = base::any(stringr::str_detect(x, "[\u2010-\u2015\u2212]"), na.rm = TRUE),
      has_leading_trailing_space = base::any(x != stringr::str_squish(x), na.rm = TRUE),
      min_nchar = base::min(base::nchar(x), na.rm = TRUE),
      max_nchar = base::max(base::nchar(x), na.rm = TRUE),
      sample_values = base::paste(utils::head(base::unique(stats::na.omit(x)), 5), collapse = " | ")
    )
  })
}


#' Check for invisible character issues in a specific column
#' 
#' @param df A data frame
#' @param col_name Name of column to check
#' @return A tibble showing unique values with their byte representations
check_invisible_chars <- function(df, col_name) {
  df |>
    dplyr::distinct(.data[[col_name]]) |>
    dplyr::mutate(
      nchar = base::nchar(.data[[col_name]]),
      bytes = base::sapply(.data[[col_name]], function(x) {
        if (base::is.na(x)) return(NA_character_)
        base::paste(base::charToRaw(x), collapse = " ")
      }),
      has_nbsp = stringr::str_detect(bytes, "c2 a0"),
      has_unicode_hyphen = stringr::str_detect(bytes, "e2 80 9[0-5]")
    ) |>
    dplyr::filter(has_nbsp | has_unicode_hyphen)
}


#' Find values that look identical but have different bytes
#' 
#' @param df A data frame
#' @param col_name Name of column to check
#' @return A tibble of visually identical but byte-different values
find_visual_duplicates <- function(df, col_name) {
  df |>
    dplyr::distinct(.data[[col_name]]) |>
    dplyr::mutate(
      normalized = .data[[col_name]] |>
        stringr::str_replace_all("\u00A0", " ") |>
        stringr::str_replace_all("[\u2010-\u2015\u2212]", "-") |>
        stringr::str_squish()
    ) |>
    dplyr::group_by(normalized) |>
    dplyr::filter(dplyr::n() > 1) |>
    dplyr::arrange(normalized) |>
    dplyr::ungroup()
}


#' Profile names for similar entries (potential duplicates)
#' 
#' @param names_vector A character vector of names
#' @param max_distance Maximum edit distance to consider
#' @return A tibble of similar name pairs
find_similar_names_profile <- function(names_vector, max_distance = 2) {
  unique_names <- base::unique(stats::na.omit(names_vector))
  
  if (base::length(unique_names) < 2) {
    return(dplyr::tibble(name1 = character(), name2 = character(), distance = integer()))
  }
  
  if (base::length(unique_names) > 500) {
    base::message("  Checking ", base::length(unique_names), 
                  " unique names - this may take a moment...")
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
          similar_pairs <- base::rbind(similar_pairs, base::data.frame(
            name1 = unique_names[i],
            name2 = unique_names[j],
            distance = base::as.integer(d)
          ))
        }
      }
    }
  }
  
  dplyr::as_tibble(similar_pairs) |>
    dplyr::arrange(distance, name1)
}


# ------------------------------------------------------------------------------
# FILE-LEVEL PROFILING
# ------------------------------------------------------------------------------

#' Profile an Excel file for potential issues
#' 
#' @param filename Name of file (in data_raw directory)
#' @param data_path Path to data_raw directory
#' @return A list containing profile results
profile_excel_file <- function(filename, data_path = paths$data_raw) {
  filepath <- base::file.path(data_path, filename)
  
  if (!base::file.exists(filepath)) {
    stop("File not found: ", filepath)
  }
  
  base::message("Profiling: ", filename)
  base::message(base::strrep("-", 50))
  
  # Import
  df <- readxl::read_excel(filepath, col_types = "text") |>
    janitor::clean_names()
  
  # Basic info
  basic_info <- dplyr::tibble(
    filename = filename,
    n_rows = base::nrow(df),
    n_cols = base::ncol(df),
    column_names = base::paste(base::names(df), collapse = ", ")
  )
  
  base::message("Rows: ", basic_info$n_rows)
  base::message("Columns: ", basic_info$n_cols)
  base::message("Column names: ", basic_info$column_names)
  base::message("")
  
  # Text column profiles
  text_profile <- profile_text_columns(df)
  
  # Check for invisible character issues
  invisible_char_issues <- purrr::map_dfr(
    base::names(df)[base::sapply(df, is.character)],
    ~ check_invisible_chars(df, .x) |>
      dplyr::mutate(column = .x, .before = 1)
  )
  
  if (base::nrow(invisible_char_issues) > 0) {
    base::message("WARNING: Found invisible character issues in ", 
                  dplyr::n_distinct(invisible_char_issues$column), " column(s)")
  }
  
  # Check for visual duplicates
  visual_duplicates <- purrr::map_dfr(
    base::names(df)[base::sapply(df, is.character)],
    ~ find_visual_duplicates(df, .x) |>
      dplyr::mutate(column = .x, .before = 1)
  )
  
  if (base::nrow(visual_duplicates) > 0) {
    base::message("WARNING: Found ", base::nrow(visual_duplicates), 
                  " visually identical but byte-different values")
  }
  
  # Find name column and check for similar names
  name_col <- base::intersect(
    base::names(df), 
    c("name", "last_name", "employee_name")
  )[1]
  
  similar_names <- NULL
  if (!base::is.na(name_col)) {
    similar_names <- find_similar_names_profile(df[[name_col]])
    if (base::nrow(similar_names) > 0) {
      base::message("Found ", base::nrow(similar_names), 
                    " similar name pairs (potential duplicates)")
    }
  }
  
  base::message("")
  base::message("Profile complete.")
  
  base::list(
    filename = filename,
    basic_info = basic_info,
    text_profile = text_profile,
    invisible_char_issues = invisible_char_issues,
    visual_duplicates = visual_duplicates,
    similar_names = similar_names,
    raw_data = df
  )
}


#' Print a profile summary
#' 
#' @param profile Output from profile_excel_file()
print_profile_summary <- function(profile) {
  base::cat("\n")
  base::cat("=== PROFILE SUMMARY: ", profile$filename, " ===\n")
  base::cat(base::strrep("=", 60), "\n\n")
  
  base::cat("BASIC INFO:\n")
  base::cat("  Rows: ", profile$basic_info$n_rows, "\n")
  base::cat("  Columns: ", profile$basic_info$n_cols, "\n\n")
  
  # Columns with issues
  issues <- profile$text_profile |>
    dplyr::filter(has_nbsp | has_unicode_hyphen | has_leading_trailing_space)
  
  if (base::nrow(issues) > 0) {
    base::cat("COLUMNS WITH CHARACTER ISSUES:\n")
    for (i in base::seq_len(base::nrow(issues))) {
      row <- issues[i, ]
      flags <- c()
      if (row$has_nbsp) flags <- c(flags, "non-breaking spaces")
      if (row$has_unicode_hyphen) flags <- c(flags, "unicode hyphens")
      if (row$has_leading_trailing_space) flags <- c(flags, "leading/trailing spaces")
      base::cat("  ", row$column, ": ", base::paste(flags, collapse = ", "), "\n")
    }
    base::cat("\n")
  } else {
    base::cat("NO CHARACTER ISSUES DETECTED\n\n")
  }
  
  # Visual duplicates
  if (base::nrow(profile$visual_duplicates) > 0) {
    base::cat("VISUAL DUPLICATES (same appearance, different bytes):\n")
    base::print(profile$visual_duplicates, n = 20)
    base::cat("\n")
  }
  
  # Similar names
  if (!base::is.null(profile$similar_names) && base::nrow(profile$similar_names) > 0) {
    base::cat("SIMILAR NAMES (potential duplicates, edit distance <= 2):\n")
    base::print(profile$similar_names, n = 30)
    base::cat("\n")
  }
  
  # High NA columns
  high_na <- profile$text_profile |>
    dplyr::filter(pct_na > 50)
  
  if (base::nrow(high_na) > 0) {
    base::cat("COLUMNS WITH >50% MISSING:\n")
    for (i in base::seq_len(base::nrow(high_na))) {
      base::cat("  ", high_na$column[i], ": ", high_na$pct_na[i], "% NA\n")
    }
    base::cat("\n")
  }
  
  base::cat(base::strrep("=", 60), "\n")
}


# ------------------------------------------------------------------------------
# COMPARISON FUNCTIONS
# ------------------------------------------------------------------------------

#' Compare a new file's structure to existing files
#' 
#' @param new_filename Name of new file to compare
#' @param data_path Path to data_raw directory
#' @param registry File registry from config
#' @return Comparison report
compare_structure <- function(new_filename, 
                               data_path = paths$data_raw,
                               registry = file_registry) {
  # Load new file
  new_df <- readxl::read_excel(
    base::file.path(data_path, new_filename), 
    col_types = "text"
  ) |>
    janitor::clean_names()
  
  new_cols <- base::names(new_df)
  
  # Load existing files and get their columns
  existing_structures <- purrr::map(registry$filename, function(fn) {
    fp <- base::file.path(data_path, fn)
    if (base::file.exists(fp)) {
      df <- readxl::read_excel(fp, col_types = "text") |>
        janitor::clean_names()
      base::names(df)
    } else {
      NULL
    }
  })
  base::names(existing_structures) <- registry$fiscal_year
  existing_structures <- purrr::compact(existing_structures)
  
  # Find closest match
  similarities <- purrr::map_dbl(existing_structures, function(cols) {
    base::length(base::intersect(new_cols, cols)) / 
      base::length(base::union(new_cols, cols))
  })
  
  best_match <- base::names(similarities)[base::which.max(similarities)]
  best_match_cols <- existing_structures[[best_match]]
  
  base::cat("=== STRUCTURE COMPARISON ===\n\n")
  base::cat("New file: ", new_filename, "\n")
  base::cat("Best match: ", best_match, " (", 
            base::round(base::max(similarities) * 100), "% similar)\n\n")
  
  base::cat("Columns in new file but not in ", best_match, ":\n")
  new_only <- base::setdiff(new_cols, best_match_cols)
  if (base::length(new_only) > 0) {
    base::cat("  ", base::paste(new_only, collapse = ", "), "\n")
  } else {
    base::cat("  (none)\n")
  }
  
  base::cat("\nColumns in ", best_match, " but not in new file:\n")
  missing <- base::setdiff(best_match_cols, new_cols)
  if (base::length(missing) > 0) {
    base::cat("  ", base::paste(missing, collapse = ", "), "\n")
  } else {
    base::cat("  (none)\n")
  }
  
  base::cat("\nSuggested structure_type: ")
  if (base::max(similarities) > 0.8) {
    suggested_type <- registry$structure_type[registry$fiscal_year == best_match]
    base::cat("'", suggested_type, "' (same as ", best_match, ")\n")
  } else {
    base::cat("NEW TYPE NEEDED - structure differs significantly\n")
  }
  
  base::invisible(base::list(
    new_columns = new_cols,
    best_match = best_match,
    similarity = base::max(similarities),
    columns_only_in_new = new_only,
    columns_missing_from_new = missing
  ))
}


#' Full profiling workflow for a new file
#' 
#' @param filename Name of new Excel file
#' @param data_path Path to data directory
#' @param registry File registry from config
profile_new_file <- function(filename, 
                              data_path = paths$data_raw,
                              registry = file_registry) {
  base::cat("\n")
  base::cat(base::strrep("=", 70), "\n")
  base::cat("  PROFILING NEW FILE: ", filename, "\n")
  base::cat(base::strrep("=", 70), "\n\n")
  
  # Structure comparison
  comparison <- compare_structure(filename, data_path, registry)
  base::cat("\n")
  
  # Full profile
  profile <- profile_excel_file(filename, data_path)
  print_profile_summary(profile)
  
  base::invisible(base::list(
    comparison = comparison,
    profile = profile
  ))
}
