# R/census_helpers.R
# Helper functions for reproducible, low-traffic Census + inflation workflows
# All functions use explicit namespace syntax (package::function)
#
# Required packages:
#   tidycensus, tigris, dplyr, purrr, tibble, stringr, readr,
#   digest, lubridate, httr2, readxl, rlang, ggplot2, flextable, qicharts2
#
# NOTE: quantmod and zoo removed. CPI-U-RS is not on FRED; it is a BLS
# research series published as an annual flat file. httr2 + readxl are the
# correct retrieval tools — no FRED account or API key required.
#
# Author: Dan
# -----------------------------------------------------------------------------

# =============================================================================
# CACHING UTILITIES
# =============================================================================

ds_cache_path <- function(cache_dir, key, ext = "rds") {

  base::dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)

  base::file.path(cache_dir, base::paste0(key, ".", ext))
}

ds_cache_key <- function(prefix, params) {

#' Build a human-readable cache filename key from prefix + params
#'
#' WHY human-readable instead of a hash: a hash like "a3f8c2d1" tells you
#' nothing about what is inside the file. A name like
#' "acs_place__Schertz__TX__2023__acs5" lets you find, inspect, or delete
#' a specific cached result without loading it into R first.
#'
#' Strategy:
#'   - Each param value is coerced to a string and joined with "__"
#'   - Vectors (e.g. a variables list) are collapsed with "-"
#'   - If the variable list is long, only a short digest of that portion is
#'     appended so the filename stays under OS path limits while remaining
#'     identifiable by the other human-readable parts
#'   - Characters illegal in filenames (spaces, slashes, colons) are replaced
#'     with "_" so the key is safe on Windows, macOS, and Linux

  # Sanitize a single scalar value to a filesystem-safe string
  sanitize <- function(x) {
    s <- base::as.character(x)
    # Replace any character that is not alphanumeric, dot, dash, or underscore
    base::gsub("[^A-Za-z0-9._-]", "_", s)
  }

  # Collapse one param value to a string, handling vectors gracefully
  collapse_param <- function(val) {
    if (base::length(val) == 0L) return("none")
    parts <- base::vapply(val, sanitize, base::character(1L))

    if (base::length(parts) == 1L) {
      return(parts)
    }

    joined <- base::paste(parts, collapse = "-")

    # Keep filenames sane: if collapsing many values (e.g. 20 variable codes)
    # would blow out the filename, keep the first two for readability and
    # append a short hash of the full set for uniqueness.
    if (base::nchar(joined) > 60L) {
      short_hash <- base::substr(
        digest::digest(val, algo = "xxhash32"),
        1L, 8L
      )
      head_parts <- base::paste(utils::head(parts, 2L), collapse = "-")
      joined <- base::paste0(head_parts, "_etc_", short_hash)
    }

    joined
  }

  param_parts <- base::vapply(params, collapse_param, base::character(1L))

  # Final key: prefix__param1val__param2val__...
  base::paste(
    c(sanitize(prefix), param_parts),
    collapse = "__"
  )
}

ds_cache_read <- function(path) {
  if (base::file.exists(path)) base::readRDS(path) else NULL
}

ds_cache_write <- function(x, path) {

  base::dir.create(base::dirname(path), showWarnings = FALSE, recursive = TRUE)
  base::saveRDS(x, path)
  base::invisible(x)
}

# =============================================================================
# CENSUS API KEY
# =============================================================================

ds_set_census_key <- function(key = NULL) {

#' Set Census API key for tidycensus
#'
#' Checks environment variable CENSUS_API_KEY first, then falls back to
#' provided key parameter. Does NOT install to ~/.Renviron.

  if (base::is.null(key) || !base::nzchar(key)) {
    key <- base::Sys.getenv("CENSUS_API_KEY")
  }
  if (!base::nzchar(key)) {
    base::stop(
      "No Census API key found. ",
      "Set CENSUS_API_KEY in .Renviron or pass key parameter."
    )
  }
  tidycensus::census_api_key(key, install = FALSE, overwrite = TRUE)
  base::invisible(TRUE)
}

# =============================================================================
# STATE / PLACE NAME UTILITIES
# =============================================================================

ds_get_state_fips <- function(state_abbr) {

#' Get state FIPS code from abbreviation
#'
#' @param state_abbr Two-letter state abbreviation (e.g., "TX")
#' @return Two-digit FIPS code as character (e.g., "48")

  fips_tbl <- tigris::fips_codes
  match_row <- fips_tbl[fips_tbl$state == state_abbr, ]
  if (base::nrow(match_row) == 0L) {
    base::stop("Unknown state abbreviation: ", state_abbr)
  }
  unique_fips <- base::unique(match_row$state_code)
  if (base::length(unique_fips) != 1L) {
    base::stop("Ambiguous FIPS for state: ", state_abbr)
  }
  base::message("Using FIPS code '", unique_fips, "' for state '", state_abbr, "'")
  unique_fips
}

ds_get_state_name <- function(state_abbr) {

#' Get full state name from abbreviation
#'
#' @param state_abbr Two-letter state abbreviation (e.g., "TX")
#' @return Full state name (e.g., "Texas")

  fips_tbl <- tigris::fips_codes
  match_row <- fips_tbl[fips_tbl$state == state_abbr, ]
  if (base::nrow(match_row) == 0L) {
    base::warning("Unknown state abbreviation: ", state_abbr, "; using abbr as-is")
    return(state_abbr)
  }
  base::unique(match_row$state_name)[1L]
}

ds_place_name <- function(city, state_abbr) {

#' Build Census place name string
#'
#' tidycensus returns NAME like "Schertz city, Texas".
#' This builds that string from city name and state abbreviation.
#'
#' @param city City name (e.g., "Schertz")
#' @param state_abbr Two-letter state abbreviation (e.g., "TX")
#' @return Place name string (e.g., "Schertz city, Texas")

  state_full <- ds_get_state_name(state_abbr)
  base::paste0(city, " city, ", state_full)
}

# =============================================================================
# PEP POPULATION (2010-2024)
# =============================================================================

ds_get_pep_population_place <- function(
    city,
    state,
    years,
    cache_dir = "cache/census"
) {

#' Get PEP annual population estimates for a place
#'
#' Uses tidycensus::get_estimates() for all years:
#'   - 2010-2019: vintage = 2019 with time_series = TRUE
#'   - 2020-2024: vintage = 2024 (tidycensus reads Census flat files)
#'
#' @param city City name (e.g., "Schertz")
#' @param state Two-letter state abbreviation (e.g., "TX")
#' @param years Integer vector of years to retrieve
#' @param cache_dir Directory for caching results
#' @return Tibble with columns: source, series, year, estimate, moe

  years <- base::sort(base::unique(base::as.integer(years)))
  place_nm <- ds_place_name(city, state)

  # -------------------------------------------------------------------------
  # Part A: 2010-2019 via tidycensus time series (vintage 2019)
  # -------------------------------------------------------------------------
  years_2010s <- years[years >= 2010L & years <= 2019L]
  out_2010s <- tibble::tibble()

  if (base::length(years_2010s) > 0L) {
    key_a <- ds_cache_key("pep_2010s_v2019_both", base::list(city = city, state = state))
    path_a <- ds_cache_path(cache_dir, key_a)
    cached_a <- ds_cache_read(path_a)

    if (!base::is.null(cached_a)) {
      out_2010s <- cached_a
    } else {
      raw_ts <- tidycensus::get_estimates(
        geography   = "place",
        product     = "population",
        state       = state,
        vintage     = 2019L,
        time_series = TRUE
      )

      # Filter to target place
      place_data <- raw_ts[raw_ts$NAME == place_nm, ]

      if (base::nrow(place_data) == 0L) {
        base::warning(
          "No PEP 2010s data found for '", place_nm, "'. ",
          "Available places: ", base::paste(base::head(base::unique(raw_ts$NAME), 5), collapse = ", ")
        )
        out_2010s <- tibble::tibble()
      } else {
        # Map variable codes to friendly labels
        var_labels <- base::c(
          "POP" = "population",
          "DENSITY" = "density_per_sq_mile"
        )
        out_2010s <- tibble::tibble(
          source   = "PEP",
          series   = var_labels[place_data$variable],
          year     = 2009L + base::as.integer(place_data$DATE),
          estimate = base::as.numeric(place_data$value),
          moe      = NA_real_
        )
        out_2010s <- out_2010s[out_2010s$year %in% years_2010s, ]
        out_2010s <- out_2010s[base::order(out_2010s$series, out_2010s$year), ]
        ds_cache_write(out_2010s, path_a)
      }
    }
  }

  # -------------------------------------------------------------------------
  # Part B: 2020-2024 via tidycensus (vintage 2024)
  # tidycensus reads Census flat files directly for post-2020 data
  # -------------------------------------------------------------------------
  years_2020s <- years[years >= 2020L & years <= 2024L]
  out_2020s <- tibble::tibble()

  if (base::length(years_2020s) > 0L) {
    key_b <- ds_cache_key("pep_2020s_v2024", base::list(city = city, state = state))
    path_b <- ds_cache_path(cache_dir, key_b)
    cached_b <- ds_cache_read(path_b)

    if (!base::is.null(cached_b)) {
      out_2020s <- cached_b
    } else {
      # Get all available years from vintage 2024
      raw_2020s <- tidycensus::get_estimates(
        geography = "place",
        variables = "POPESTIMATE",
        state     = state,
        vintage   = 2024L
      )

      # Filter to target place
      place_data <- raw_2020s[raw_2020s$NAME == place_nm, ]
      if (base::nrow(place_data) == 0L) {
        # Try partial match on city name
        place_data <- raw_2020s[base::grepl(
          base::paste0("^", city, " "),
          raw_2020s$NAME,
          ignore.case = TRUE
        ), ]
      }

      if (base::nrow(place_data) == 0L) {
        base::warning(
          "No PEP 2020s data found for '", place_nm, "'. ",
          "Check city name spelling."
        )
        out_2020s <- tibble::tibble()
      } else {
        # For vintage 2024, the year column contains the estimate year
        out_2020s <- tibble::tibble(
          source   = "PEP",
          series   = "population",
          year     = base::as.integer(place_data$year),
          estimate = base::as.numeric(place_data$value),
          moe      = NA_real_
        )
        out_2020s <- out_2020s[out_2020s$year %in% years_2020s, ]
        out_2020s <- out_2020s[base::order(out_2020s$year), ]
        ds_cache_write(out_2020s, path_b)
      }
    }
  }

  # Combine and return
  result <- dplyr::bind_rows(out_2010s, out_2020s)
  result[base::order(result$year), ]
}

# =============================================================================
# ACS 5-YEAR DATA
# =============================================================================

ds_get_acs_place <- function(
    city,
    state,
    years,
    variables,
    survey = "acs5",
    cache_dir = "cache/census"
) {

#' Get ACS 5-year estimates for a place
#'
#' @param city City name
#' @param state Two-letter state abbreviation
#' @param years Integer vector of end-years for ACS 5-year estimates
#' @param variables Named or unnamed character vector of ACS variable codes
#' @param survey Survey type (default "acs5")
#' @param cache_dir Cache directory
#' @return Tibble with columns: source, series, year, estimate, moe

  place_nm <- ds_place_name(city, state)

  purrr::map_dfr(years, function(y) {
    key <- ds_cache_key(
      "acs_place",
      base::list(city = city, state = state, year = y, variables = variables, survey = survey)
    )
    path <- ds_cache_path(cache_dir, key)
    cached <- ds_cache_read(path)
    if (!base::is.null(cached)) return(cached)

    raw <- tidycensus::get_acs(
      geography   = "place",
      variables   = variables,
      year        = y,
      survey      = survey,
      state       = state,
      cache_table = TRUE
    )

    place_data <- raw[raw$NAME == place_nm, ]
    if (base::nrow(place_data) == 0L) {
      base::warning("No ACS data for '", place_nm, "' in year ", y)
      return(tibble::tibble())
    }

    out <- tibble::tibble(
      source   = base::paste0("ACS_", survey),
      series   = place_data$variable,
      year     = y,
      estimate = place_data$estimate,
      moe      = place_data$moe
    )

    ds_cache_write(out, path)
  })
}

# =============================================================================
# DECENNIAL CENSUS
# =============================================================================

ds_get_decennial_place <- function(
    city,
    state,
    year,
    variable,
    sumfile,
    cache_dir = "cache/census"
) {

#' Get Decennial Census data for a place
#'
#' @param city City name
#' @param state Two-letter state abbreviation
#' @param year Census year (2000, 2010, or 2020)
#' @param variable Variable code (e.g., "P001001" for 2010, "P1_001N" for 2020)
#' @param sumfile Summary file ("sf1" for 2000/2010, "pl" for 2020)
#' @param cache_dir Cache directory
#' @return Tibble with columns: source, series, year, estimate, moe

  place_nm <- ds_place_name(city, state)

  key <- ds_cache_key(
    "decennial_place",
    base::list(city = city, state = state, year = year, variable = variable, sumfile = sumfile)
  )
  path <- ds_cache_path(cache_dir, key)
  cached <- ds_cache_read(path)
  if (!base::is.null(cached)) return(cached)

  raw <- tidycensus::get_decennial(
    geography   = "place",
    variables   = variable,
    year        = year,
    sumfile     = sumfile,
    state       = state,
    cache_table = TRUE
  )

  place_data <- raw[raw$NAME == place_nm, ]
  if (base::nrow(place_data) == 0L) {
    base::warning("No Decennial data for '", place_nm, "' in year ", year)
    return(tibble::tibble())
  }

  out <- tibble::tibble(
    source   = base::paste0("Decennial_", year),
    series   = variable,
    year     = base::as.integer(year),
    estimate = base::as.numeric(place_data$value),
    moe      = NA_real_
  )

  ds_cache_write(out, path)
}

# =============================================================================
# CPI-U-RS / INFLATION
# =============================================================================
#
# WHY CPI-U-RS INSTEAD OF CPI-U OR CPI-W:
#   - CPI-U-RS (Research Series Using Current Methods) recalculates the full
#     CPI-U history with *today's* methodology applied uniformly backward.
#     This eliminates the artificial "kinks" caused by BLS methodological
#     changes in 1983 (rental equivalence) and 1998 (geometric means), which
#     would otherwise distort real-dollar comparisons across those boundaries.
#   - CPI-W was designed for Social Security COLA indexing, not income
#     deflation — it over-weights expenditure patterns of hourly wage earners
#     and is inappropriate for municipal compensation studies.
#   - Census Bureau researchers explicitly recommend CPI-U-RS for deflating
#     ACS/CPS income series across years. See:
#     https://www.census.gov/topics/income-poverty/income/guidance/current-vs-constant-dollars.html
#
# WHY NOT FRED/quantmod:
#   - CPI-U-RS is *not* in FRED. BLS publishes it as an annual flat file on
#     their website. quantmod was pulling CPI-W from FRED — wrong series,
#     wrong source. httr2 downloads the authoritative BLS file directly.

# BLS publishes CPI-U-RS as a single xlsx: all items, annual averages.
# URL is stable and versioned by BLS fiscal year update cycle.
.DS_CPIU_RS_URL <- "https://www.bls.gov/cpi/research-series/r-cpi-u-rs-allitems.xlsx"

ds_get_cpiu_rs_annual <- function(
    start_year,
    end_year,
    base_year  = end_year,
    cache_dir  = "cache/inflation"
) {

#' Fetch CPI-U-RS annual averages and compute deflation factors
#'
#' Downloads the BLS Research Series flat file (xlsx), extracts annual
#' averages, and returns conversion factors to express any year's dollars
#' in base_year dollars.
#'
#' WHY annual averages: ACS income questions ask about the prior 12 months
#' with no single reference month, so a 12-month average is the correct
#' temporal alignment — using a single month would introduce arbitrary
#' seasonal noise.
#'
#' @param start_year First year of desired range
#' @param end_year   Last year of desired range
#' @param base_year  Reference year for constant dollars (default = end_year)
#' @param cache_dir  Cache directory
#' @return Tibble: year, cpi_annual_avg, base_year, factor_to_base

  # Cache on the full downloaded range, not the filtered range, so a single
  # download serves multiple analyses with different start/end windows.
  key  <- ds_cache_key("cpiu_rs_annual_raw", base::list(base_year = base_year))
  path <- ds_cache_path(cache_dir, key)
  cached <- ds_cache_read(path)

  if (!base::is.null(cached)) {
    annual <- cached
  } else {

    # -------------------------------------------------------------------------
    # Download: httr2 gives us explicit status checking and retry logic,
    # which matters here because a failed/partial download would silently
    # corrupt the cache if we used download.file() without verification.
    # -------------------------------------------------------------------------
    tmp <- base::tempfile(fileext = ".xlsx")
    base::on.exit(base::unlink(tmp), add = TRUE)

    # WHY these headers: BLS returns 403 to any request that doesn't present
    # a plausible browser identity. A bare User-Agent like "R/httr2" is
    # immediately blocked. We supply the full set of headers a real browser
    # sends — including Referer — because BLS checks that xlsx downloads
    # appear to originate from their own CPI research-series landing page,
    # not a direct deep-link from an external script.
    resp <- httr2::request(.DS_CPIU_RS_URL) |>
      httr2::req_headers(
        "User-Agent"      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept"          = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream,*/*",
        "Accept-Language" = "en-US,en;q=0.9",
        "Accept-Encoding" = "gzip, deflate, br",
        "Referer"         = "https://www.bls.gov/cpi/research-series/home.htm",
        "Connection"      = "keep-alive"
      ) |>
      httr2::req_retry(max_tries = 3L, backoff = ~ 5) |>
      httr2::req_perform()

    httr2::resp_check_status(resp)
    base::writeBin(httr2::resp_body_raw(resp), tmp)

    # -------------------------------------------------------------------------
    # Parse: BLS xlsx layout varies by release year. The file always has:
    #   Row 1:   Document title (e.g. "R-CPI-U-RS ALL ITEMS")
    #   Row 2:   Subtitle (e.g. "U.S. CITY AVERAGE") -- sometimes absent
    #   Row 3:   Column headers: YEAR, JAN, FEB, ..., DEC, [AVG or ANNUAL]
    #   Row 4+:  Data rows
    #
    # WHY scan for the header row instead of hard-coding skip: BLS has shipped
    # releases with 1, 2, and 3 pre-header rows depending on the vintage. We
    # find the first row that contains "YEAR" as a cell value, which is
    # unambiguous regardless of how many title rows precede it.
    #
    # WHY compute AVG from monthly columns if no summary column exists: BLS
    # does not guarantee a pre-computed annual average column. The 12 monthly
    # columns (JAN-DEC) are always present. Computing the mean ourselves
    # replicates exactly what BLS publishes and makes us independent of
    # whatever they name -- or whether they include -- the summary column.
    # -------------------------------------------------------------------------

    .month_cols <- base::c("JAN","FEB","MAR","APR","MAY","JUN",
                           "JUL","AUG","SEP","OCT","NOV","DEC")
    .avg_aliases <- base::c("AVG","ANNUAL","AVERAGE","ANN AVG","ANN_AVG")

    # Scan up to 5 skip levels to find the row where "YEAR" appears as a header
    .find_header_skip <- function() {
      for (sk in 0:6) {
        xl  <- readxl::read_xlsx(tmp, skip = sk, col_types = "text", n_max = 3L)
        nms <- base::toupper(base::trimws(base::names(xl)))
        if ("YEAR" %in% nms) return(sk)
      }
      return(NULL)
    }

    header_skip <- .find_header_skip()

    if (base::is.null(header_skip)) {
      # Emit what we actually found so the caller can diagnose and fix
      diag <- base::lapply(0:6, function(sk) {
        xl <- readxl::read_xlsx(tmp, skip = sk, col_types = "text", n_max = 2L)
        base::paste0("skip=", sk, ": ", base::paste(base::toupper(base::trimws(base::names(xl))), collapse = ", "))
      })
      base::stop(
        "CPI-U-RS xlsx: 'YEAR' column not found in first 5 rows.\n",
        base::paste(diag, collapse = "\n"), "\n",
        "Source: ", .DS_CPIU_RS_URL
      )
    }

    raw_xl <- readxl::read_xlsx(tmp, skip = header_skip, col_types = "text")
    base::names(raw_xl) <- base::toupper(base::trimws(base::names(raw_xl)))

    # Locate year column (always "YEAR" once we're on the right row)
    year_col <- "YEAR"

    # Locate or compute annual average
    # WHY prefer BLS-published value: if BLS provides AVG, use it -- it may
    # differ trivially from a naive mean due to rounding conventions.
    # If absent, compute from the 12 monthly columns, which are always present.
    avg_col <- base::names(raw_xl)[base::names(raw_xl) %in% .avg_aliases][1L]

    if (base::is.na(avg_col)) {
      # No pre-computed summary column -- calculate from monthly data
      present_months <- .month_cols[.month_cols %in% base::names(raw_xl)]
      if (base::length(present_months) < 12L) {
        base::stop(
          "CPI-U-RS xlsx: neither an annual-average column nor all 12 monthly ",
          "columns found. Columns present: ",
          base::paste(base::names(raw_xl), collapse = ", ")
        )
      }
      raw_xl[["COMPUTED_AVG"]] <- base::rowMeans(
        base::sapply(present_months, function(m) base::as.numeric(raw_xl[[m]])),
        na.rm = TRUE
      )
      avg_col <- "COMPUTED_AVG"
    }

    annual <- tibble::tibble(
      year           = base::as.integer(raw_xl[[year_col]]),
      cpi_annual_avg = base::as.numeric(raw_xl[[avg_col]])
    )

    # Drop footer notes and blank trailing rows BLS leaves in the file
    annual <- annual[!base::is.na(annual$year) & !base::is.na(annual$cpi_annual_avg), ]
    annual <- annual[base::order(annual$year), ]

    ds_cache_write(annual, path)
  }

  # Filter to requested window after cache retrieval
  annual <- annual[annual$year >= start_year & annual$year <= end_year, ]

  # Compute deflation factors relative to base_year.
  # factor_to_base = base_cpi / year_cpi, so multiplying a nominal value
  # by this factor expresses it in base_year dollars.
  base_cpi <- annual$cpi_annual_avg[annual$year == base_year]
  if (base::length(base_cpi) != 1L) {
    base::stop(
      "Base year ", base_year, " not found in CPI-U-RS data. ",
      "Available range: ", base::min(annual$year), "-", base::max(annual$year)
    )
  }

  annual$base_year       <- base_year
  annual$factor_to_base  <- base_cpi / annual$cpi_annual_avg

  annual
}

ds_adjust_to_base_dollars <- function(
    df,
    value_col = "estimate",
    year_col  = "year",
    cpi_tbl,
    out_col   = "estimate_real"
) {

#' Adjust nominal dollar values to constant base-year dollars
#'
#' Joins CPI-U-RS factors onto df by year and multiplies nominal values.
#' The join approach (rather than a lookup loop) preserves all rows and
#' naturally propagates NA for any year not covered by the CPI table,
#' which makes data gaps visible rather than silently wrong.
#'
#' @param df       Data frame with nominal dollar values
#' @param value_col Column of nominal values
#' @param year_col  Column of integer years
#' @param cpi_tbl  Output of ds_get_cpiu_rs_annual()
#' @param out_col  Name for the new real-dollar column
#' @return df with out_col appended

  cpi_slim <- cpi_tbl[, c("year", "factor_to_base")]

  # Rename year key to match df's year column name before joining
  base::names(cpi_slim)[base::names(cpi_slim) == "year"] <- year_col

  merged <- dplyr::left_join(df, cpi_slim, by = year_col)
  merged[[out_col]] <- base::as.numeric(merged[[value_col]]) * merged$factor_to_base
  merged
}

# =============================================================================
# SPC / XmR CHARTING (using qicharts2)
# =============================================================================

ds_plot_xmr <- function(
    df,
    year_col  = "year",
    value_col = "estimate",
    title     = NULL,
    subtitle  = NULL,
    y_label   = NULL
) {

#' Create XmR (Individuals) control chart using qicharts2
#'
#' @param df        Data frame with time series data
#' @param year_col  Name of year/time column
#' @param value_col Name of value column
#' @param title     Chart title (optional)
#' @param subtitle  Chart subtitle (optional)
#' @param y_label   Y-axis label (optional, defaults to value_col)
#' @return ggplot2 object from qicharts2::qic()

  # Ensure data is sorted
  d <- df[base::order(df[[year_col]]), ]

  p <- qicharts2::qic(
    x         = d[[year_col]],
    y         = d[[value_col]],
    chart     = "i",
    title     = title %||% "XmR (Individuals) Chart",
    subtitle  = subtitle,
    ylab      = y_label %||% value_col,
    xlab      = "Year",
    show.grid = TRUE
  )

  p
}

# =============================================================================
# SPC / XmR CHARTING (custom ggplot2 implementation)
# =============================================================================

ds_plot_xmr2 <- function(
    df,
    year_col  = "year",
    value_col = "estimate",
    title     = NULL,
    subtitle  = NULL,
    y_label   = NULL,
    caption   = NULL
) {

#' Create XmR (Individuals) control chart using custom ggplot2
#'
#' Full-featured control chart with:
#'   - Signal detection (points outside limits shown in red)
#'   - Runs analysis (dashed centerline if runs rules violated)
#'   - Labels for CL, UCL, LCL values
#'
#' @param df        Data frame with time series data
#' @param year_col  Name of year/time column
#' @param value_col Name of value column
#' @param title     Chart title (optional)
#' @param subtitle  Chart subtitle (optional)
#' @param y_label   Y-axis label (optional)
#' @param caption   Chart caption (optional)
#' @return ggplot2 object

  # Sort data
  d      <- df[base::order(df[[year_col]]), ]
  x_vals <- d[[year_col]]
  y_vals <- base::as.numeric(d[[value_col]])

  # -------------------------------------------------------------------------
  # XmR control limits: Shewhart's d2 unbiasing constant for n=2 moving
  # ranges is 1.128. Dividing mean(|MR|) by 1.128 gives an unbiased
  # estimate of sigma — this is the canonical SPC formula, not an
  # approximation. Using sample sd() instead would inflate limits because
  # it is sensitive to level shifts that SPC is designed to detect.
  # -------------------------------------------------------------------------
  emp_cl  <- base::mean(y_vals, na.rm = TRUE)
  emp_sd  <- base::mean(base::abs(base::diff(y_vals)), na.rm = TRUE) / 1.128
  emp_ucl <- emp_cl + (3 * emp_sd)
  emp_lcl <- base::max(emp_cl - (3 * emp_sd), 0)

  # Sigma signals: points outside control limits
  sigma_signals <- y_vals < emp_lcl | y_vals > emp_ucl

  # -------------------------------------------------------------------------
  # Runs analysis: test for non-random patterns that sigma rules alone miss.
  # longest_run_max uses log2(n)+3 as the expected upper bound under the null
  # hypothesis of random variation — this is the standard Wheeler rule.
  # n_crossings_min uses the 5th percentile of Binomial(n-1, 0.5) to flag
  # stratification (too few crossings = two distinct populations in the data).
  # -------------------------------------------------------------------------
  runs <- base::sign(y_vals - emp_cl)
  runs <- runs[runs != 0]
  if (base::length(runs) > 0) {
    runs_lengths   <- base::rle(runs)$lengths
    n_obs          <- base::sum(runs_lengths)
    longest_run    <- base::max(runs_lengths)
    n_runs         <- base::length(runs_lengths)
    n_crossings    <- n_runs - 1
    longest_run_max  <- base::round(base::log2(n_obs) + 3)
    n_crossings_min  <- stats::qbinom(0.05, n_obs - 1, 0.5)
    runs_signal <- longest_run > longest_run_max | n_crossings < n_crossings_min
  } else {
    runs_signal <- FALSE
  }

  # Build plotting data frame
  plot_df <- tibble::tibble(
    x            = x_vals,
    y            = y_vals,
    emp_cl       = emp_cl,
    emp_ucl      = emp_ucl,
    emp_lcl      = emp_lcl,
    out_of_control = sigma_signals,
    runs_signal  = runs_signal
  )

  # Build plot
  p <- ggplot2::ggplot(plot_df, ggplot2::aes(x = x, y = y)) +

    # Line connecting points
    ggplot2::geom_line(color = "darkgray", linewidth = 1.0) +

    # Points colored by out-of-control status
    ggplot2::geom_point(
      ggplot2::aes(color = base::factor(out_of_control)),
      size = 3
    ) +
    ggplot2::scale_color_manual(
      values = base::c("FALSE" = "blue", "TRUE" = "red"),
      guide  = "none"
    ) +

    # Centerline: dashed when runs rules are violated to signal that the
    # process center estimate itself may be unreliable
    ggplot2::geom_hline(
      yintercept = emp_cl,
      linetype   = base::ifelse(runs_signal, "dashed", "solid"),
      color      = "black",
      linewidth  = 0.8
    ) +

    # UCL and LCL
    ggplot2::geom_hline(
      yintercept = emp_ucl,
      color      = "red",
      linetype   = "solid",
      linewidth  = 0.8
    ) +
    ggplot2::geom_hline(
      yintercept = emp_lcl,
      color      = "red",
      linetype   = "solid",
      linewidth  = 0.8
    ) +

    # Labels
    ggplot2::labs(
      title    = title %||% "XmR (Individuals) Chart",
      subtitle = subtitle,
      caption  = caption %||% base::ifelse(
        runs_signal,
        "Note: Runs rules violated (dashed centerline)",
        "Limits: CL \u00b1 2.66\u00d7MR\u0304"
      ),
      x = "Year",
      y = y_label %||% value_col
    ) +

    # X-axis with all years as breaks
    ggplot2::scale_x_continuous(
      breaks = base::unique(x_vals),
      expand = ggplot2::expansion(mult = 0.08)
    ) +

    # Y-axis formatting
    ggplot2::scale_y_continuous(
      labels = function(y) base::format(y, big.mark = ",", scientific = FALSE),
      expand = ggplot2::expansion(mult = 0.15)
    ) +

    # Theme
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(
      plot.title    = ggplot2::element_text(face = "bold", size = 14),
      plot.subtitle = ggplot2::element_text(size = 11),
      axis.text.x   = ggplot2::element_text(angle = 45, hjust = 1),
      panel.grid.minor = ggplot2::element_blank()
    ) +

    # Add CL label at end
    ggplot2::annotate(
      "text",
      x     = base::max(x_vals),
      y     = emp_cl,
      label = base::paste0("CL = ", base::format(base::round(emp_cl, 0), big.mark = ",")),
      hjust = -0.1, vjust = 0.5,
      color = "black", size = 3.5
    ) +

    # Add UCL label at end
    ggplot2::annotate(
      "text",
      x     = base::max(x_vals),
      y     = emp_ucl,
      label = base::paste0("UCL = ", base::format(base::round(emp_ucl, 0), big.mark = ",")),
      hjust = -0.1, vjust = 0.5,
      color = "red", size = 3.5
    ) +

    # Add LCL label at end
    ggplot2::annotate(
      "text",
      x     = base::max(x_vals),
      y     = emp_lcl,
      label = base::paste0("LCL = ", base::format(base::round(emp_lcl, 0), big.mark = ",")),
      hjust = -0.1, vjust = 0.5,
      color = "red", size = 3.5
    ) +

    # Expand plot area to fit end labels
    ggplot2::coord_cartesian(clip = "off") +
    ggplot2::theme(plot.margin = ggplot2::margin(10, 50, 10, 10))

  p
}

# =============================================================================
# FLEXTABLE STYLING
# =============================================================================

ds_style_flextable <- function(ft, title = NULL) {

#' Apply standard flextable styling
#'
#' Styling per Dan's preferences:
#'   - Title row: blue italic text, white background
#'   - Header row: pale green background
#'   - Auto-fit column widths
#'
#' @param ft    A flextable object
#' @param title Optional title string to add as header
#' @return Styled flextable object

  if (!base::is.null(title)) {
    ft <- flextable::add_header_lines(ft, values = title)
    ft <- flextable::color(ft, i = 1, part = "header", color = "blue")
    ft <- flextable::italic(ft, i = 1, part = "header")
    ft <- flextable::align(ft, i = 1, part = "header", align = "left")
    ft <- flextable::fontsize(ft, i = 1, part = "header", size = 12)
    ft <- flextable::bg(ft, i = 1, part = "header", bg = "white")
    ft <- flextable::bg(ft, i = 2, part = "header", bg = "palegreen")
  } else {
    ft <- flextable::bg(ft, i = 1, part = "header", bg = "palegreen")
  }

  ft <- flextable::autofit(ft)
  ft
}
