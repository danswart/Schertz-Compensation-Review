# R/census_helpers.R
# Helper functions for reproducible, low-traffic Census + inflation workflows
# Requires: tidycensus, dplyr, purrr, readr, tibble, stringr, digest, lubridate, quantmod, ggplot2
# Optional: qicharts2, gt

ds_cache_path <- function(cache_dir, key, ext = "rds") {
  base::dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
  base::file.path(cache_dir, base::paste0(key, ".", ext))
}

ds_cache_key <- function(prefix, params) {
  # Stable cache key for any list of parameters
  digest::digest(
    base::list(prefix = prefix, params = params),
    algo = "xxhash64"
  )
}

ds_cache_read <- function(path) {
  if (base::file.exists(path)) base::readRDS(path) else NULL
}

ds_cache_write <- function(x, path) {
  base::dir.create(base::dirname(path), showWarnings = FALSE, recursive = TRUE)
  base::saveRDS(x, path)
  x
}

ds_set_census_key <- function(key = NULL) {
  # Prefer env var; allow passing a key param for convenience
  if (base::is.null(key) || !base::nzchar(key)) {
    key <- base::Sys.getenv("CENSUS_API_KEY")
  }
  if (!base::nzchar(key)) {
    base::stop(
      "No Census API key found. Set Sys.setenv(CENSUS_API_KEY='...') or pass params$census_api_key in the Quarto doc."
    )
  }
  # Do NOT install to ~/.Renviron automatically; keep project portable.
  tidycensus::census_api_key(key, install = FALSE, overwrite = TRUE)
  invisible(TRUE)
}

ds_place_name <- function(city, state) {
  # tidycensus returns NAME like "Schertz city, Texas"
  # We'll match the exact city + " city, " + state full name via a lookup table.
  st <- tigris::fips_codes %>%
    dplyr::distinct(state, state_name) %>%
    dplyr::filter(state == state) %>%
    dplyr::slice(1) %>%
    dplyr::pull(state_name)

  # If lookup fails (rare), fall back to the abbreviation in a permissive match
  if (base::length(st) == 0) {
    st <- state
  }
  base::paste0(city, " city, ", st)
}


ds_get_pep_population_place <- function(
  city,
  state,
  years,
  cache_dir = "cache/census"
) {
  years <- sort(unique(as.integer(years)))

  # State FIPS needed for the Census table file name (e.g., TX = 48)
  st_fips <- tigris::fips_codes |>
    dplyr::distinct(state, state_code) |>
    dplyr::filter(.data$state == state) |>
    dplyr::slice(1) |>
    dplyr::pull(.data$state_code)

  if (length(st_fips) != 1) {
    stop("Could not determine state FIPS for ", state)
  }

  # ---------------------------
  # Part A: 2010–2019 via tidycensus API time series (place supported)
  # ---------------------------
  years_2010s <- years[years <= 2019]
  out_2010s <- tibble::tibble()

  if (length(years_2010s) > 0) {
    keyA <- ds_cache_key(
      "pep_place_2010s_timeseries_v2019",
      list(city = city, state = state)
    )
    pathA <- ds_cache_path(cache_dir, keyA)
    cachedA <- ds_cache_read(pathA)

    if (!is.null(cachedA)) {
      out_2010s <- cachedA
    } else {
      ts_dat <- tidycensus::get_estimates(
        geography = "place",
        product = "population",
        state = state,
        vintage = 2019,
        time_series = TRUE
      ) |>
        dplyr::filter(.data$NAME == ds_place_name(city, state)) |>
        # For this endpoint, DATE indexes 2010..2019; convert to year
        dplyr::mutate(year = 2009L + as.integer(.data$DATE)) |>
        dplyr::filter(.data$year %in% years_2010s) |>
        dplyr::transmute(
          source = "PEP",
          series = "population",
          year = .data$year,
          estimate = .data$value,
          moe = NA_real_
        ) |>
        dplyr::arrange(.data$year)

      out_2010s <- ds_cache_write(ts_dat, pathA)
    }
  }

  # ---------------------------
  # Part B: 2020–2024 via Census website XLSX (place supported)
  # ---------------------------
  years_2020s <- years[years >= 2020]
  out_2020s <- tibble::tibble()

  if (length(years_2020s) > 0) {
    # Use the most recent completed vintage file (Vintage 2024)
    # Texas file: SUB-IP-EST2024-POP-48.xlsx
    xlsx_url <- sprintf(
      "https://www2.census.gov/programs-surveys/popest/tables/2020-2024/cities/totals/SUB-IP-EST2024-POP-%02d.xlsx",
      as.integer(st_fips)
    )

    keyB <- ds_cache_key(
      "pep_place_2020s_xlsx_v2024",
      list(state = state, fips = st_fips)
    )
    pathB <- ds_cache_path(cache_dir, keyB)

    cachedB <- ds_cache_read(pathB)
    if (!is.null(cachedB)) {
      wide <- cachedB
    } else {
      tmp <- base::file.path(tempdir(), basename(xlsx_url))
      utils::download.file(xlsx_url, tmp, mode = "wb", quiet = TRUE)

      # Census XLSX files have 3-4 metadata rows before headers
      # Skip them and rename the geographic column to NAME for consistency
      wide <- readxl::read_xlsx(tmp, skip = 3) |>
        dplyr::rename_with(toupper) |>
        dplyr::rename_with(
          ~ dplyr::if_else(stringr::str_detect(.x, "GEOGRAPHIC"), "NAME", .x)
        )

      wide <- ds_cache_write(wide, pathB)
    }

    # Find Schertz row
    # In these files, the place name column is usually "NAME"
    sch <- wide |>
      dplyr::filter(.data$NAME == ds_place_name(city, state))

    if (nrow(sch) != 1) {
      stop(
        "Could not uniquely match place name in XLSX. Found ",
        nrow(sch),
        " rows for: ",
        ds_place_name(city, state)
      )
    }

    # Columns are typically POPESTIMATE2020..POPESTIMATE2024 (or similar)
    # We gather any columns containing 2020-2024 estimates
    out_2020s <- sch |>
      tidyr::pivot_longer(
        cols = dplyr::matches("POPESTIMATE20(20|21|22|23|24)$"),
        names_to = "col",
        values_to = "estimate"
      ) |>
      dplyr::mutate(
        year = as.integer(stringr::str_extract(.data$col, "\\d{4}"))
      ) |>
      dplyr::filter(.data$year %in% years_2020s) |>
      dplyr::transmute(
        source = "PEP",
        series = "population",
        year = .data$year,
        estimate = as.numeric(.data$estimate),
        moe = NA_real_
      ) |>
      dplyr::arrange(.data$year)
  }

  dplyr::bind_rows(out_2010s, out_2020s) |>
    dplyr::arrange(.data$year)
}


ds_get_acs_place <- function(
  city,
  state,
  years,
  variables,
  survey = "acs5",
  cache_dir = "cache/census"
) {
  # Returns long format with estimate + moe per variable
  purrr::map_dfr(years, function(y) {
    key <- ds_cache_key(
      "acs_place",
      base::list(
        city = city,
        state = state,
        year = y,
        variables = variables,
        survey = survey
      )
    )
    path <- ds_cache_path(cache_dir, key)

    cached <- ds_cache_read(path)
    if (!base::is.null(cached)) {
      return(cached)
    }

    dat <- tidycensus::get_acs(
      geography = "place",
      variables = variables,
      year = y,
      survey = survey,
      state = state,
      cache_table = TRUE
    ) %>%
      dplyr::filter(.data$NAME == ds_place_name(city, state)) %>%
      dplyr::transmute(
        source = base::paste0("ACS_", survey),
        series = .data$variable,
        year = y,
        estimate = .data$estimate,
        moe = .data$moe
      )

    ds_cache_write(dat, path)
  })
}

ds_get_decennial_place <- function(
  city,
  state,
  year,
  variable,
  sumfile,
  cache_dir = "cache/census"
) {
  # Decennial place totals. Example: 2010 sf1 P001001 ; 2020 pl P1_001N
  key <- ds_cache_key(
    "decennial_place",
    base::list(
      city = city,
      state = state,
      year = year,
      variable = variable,
      sumfile = sumfile
    )
  )
  path <- ds_cache_path(cache_dir, key)

  cached <- ds_cache_read(path)
  if (!base::is.null(cached)) {
    return(cached)
  }

  dat <- tidycensus::get_decennial(
    geography = "place",
    variables = variable,
    year = year,
    sumfile = sumfile,
    state = state,
    cache_table = TRUE
  ) %>%
    dplyr::filter(.data$NAME == ds_place_name(city, state)) %>%
    dplyr::transmute(
      source = base::paste0("Decennial_", year),
      series = variable,
      year = year,
      estimate = .data$value,
      moe = NA_real_
    )

  ds_cache_write(dat, path)
}

ds_get_cpi_annual <- function(
  start_year,
  end_year,
  base_year = end_year,
  cache_dir = "cache/inflation"
) {
  # CPI-U (CPIAUCSL) from FRED via quantmod; annual average CPI and factors to base_year dollars
  key <- ds_cache_key(
    "cpi_annual",
    base::list(
      start_year = start_year,
      end_year = end_year,
      base_year = base_year
    )
  )
  path <- ds_cache_path(cache_dir, key)

  cached <- ds_cache_read(path)
  if (!base::is.null(cached)) {
    return(cached)
  }

  xt <- quantmod::getSymbols("CPIAUCSL", src = "FRED", auto.assign = FALSE)
  df <- tibble::tibble(
    date = base::as.Date(zoo::index(xt)),
    cpi = base::as.numeric(xt[, 1])
  ) %>%
    dplyr::mutate(year = lubridate::year(.data$date)) %>%
    dplyr::filter(.data$year >= start_year, .data$year <= end_year) %>%
    dplyr::group_by(.data$year) %>%
    dplyr::summarise(
      cpi_annual_avg = base::mean(.data$cpi, na.rm = TRUE),
      .groups = "drop"
    )

  base_cpi <- df %>%
    dplyr::filter(.data$year == base_year) %>%
    dplyr::pull(.data$cpi_annual_avg)
  if (base::length(base_cpi) != 1) {
    base::stop("Base year CPI not found; check years.")
  }

  out <- df %>%
    dplyr::mutate(
      base_year = base_year,
      factor_to_base = base_cpi / .data$cpi_annual_avg
    )

  ds_cache_write(out, path)
}

ds_adjust_to_base_dollars <- function(
  df,
  value_col = "estimate",
  year_col = "year",
  cpi_tbl,
  out_col = "estimate_real"
) {
  # Adds an inflation-adjusted column to df
  v <- rlang::sym(value_col)
  y <- rlang::sym(year_col)

  df %>%
    dplyr::left_join(
      cpi_tbl %>% dplyr::select(year, factor_to_base),
      by = base::setNames("year", rlang::as_string(y))
    ) %>%
    dplyr::mutate(
      "{out_col}" := base::as.numeric(!!v) * .data$factor_to_base
    )
}

ds_xmr_limits <- function(x) {
  # Basic Individuals & Moving Range limits
  # UCL/LCL = Xbar +/- 2.66 * MRbar  (since E(MR)=d2*sigma with d2=1.128; 3*sigma => 3/d2=2.6596)
  x <- base::as.numeric(x)
  xbar <- base::mean(x, na.rm = TRUE)
  mr <- base::abs(base::diff(x))
  mrbar <- base::mean(mr, na.rm = TRUE)
  ucl <- xbar + 2.66 * mrbar
  lcl <- xbar - 2.66 * mrbar
  base::list(xbar = xbar, ucl = ucl, lcl = lcl, mrbar = mrbar)
}

ds_plot_xmr <- function(
  df,
  year_col = "year",
  value_col = "estimate",
  title = NULL
) {
  y <- rlang::sym(year_col)
  v <- rlang::sym(value_col)

  d <- df %>%
    dplyr::arrange(!!y) %>%
    dplyr::mutate(val = base::as.numeric(!!v))

  lim <- ds_xmr_limits(d$val)

  ggplot2::ggplot(d, ggplot2::aes(x = !!y, y = .data$val)) +
    ggplot2::geom_line() +
    ggplot2::geom_point() +
    ggplot2::geom_hline(yintercept = lim$xbar, linetype = "dashed") +
    ggplot2::geom_hline(yintercept = lim$ucl, linetype = "dotted") +
    ggplot2::geom_hline(yintercept = lim$lcl, linetype = "dotted") +
    ggplot2::labs(
      title = title %||% "XmR (Individuals) Chart",
      x = "Year",
      y = rlang::as_string(v),
      caption = "Limits computed as X̄ ± 2.66×MR̄ (Individuals/Moving Range)."
    ) +
    ggplot2::theme_minimal()
}
