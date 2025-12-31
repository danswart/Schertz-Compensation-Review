#' Get a city-level population estimate series (PEP) for 2010–2024
#'
#' @param place_name   City name, e.g. "Schertz"
#' @param state        State postal abbreviation, e.g. "TX"
#' @param start_year   First year (default 2010)
#' @param end_year     Last year (default 2024)
#' @param vintage_post2020 PEP vintage for post-2020 estimates (default 2024)
#'
#' @return A tibble with NAME, year, variable ("POP"), and value (population)
#'
get_city_pep_2010_2024 <- function(
  place_name,
  state = "TX",
  start_year = 2010,
  end_year = 2024,
  vintage_post2020 = 2024
) {
  if (!requireNamespace("tidycensus", quietly = TRUE)) {
    stop("Package 'tidycensus' is required. Please install it first.")
  }
  if (start_year < 2010) {
    warning("start_year < 2010; truncating to 2010.")
    start_year <- 2010
  }
  if (end_year > 2024) {
    warning("end_year > 2024; truncating to 2024.")
    end_year <- 2024
  }
  if (start_year > end_year) {
    stop("start_year must be <= end_year.")
  }

  # full state name for the NAME field ("Schertz city, Texas")
  full_state <- state.name[match(toupper(state), state.abb)]
  if (is.na(full_state)) {
    stop(
      "Could not match state abbreviation '",
      state,
      "' to a full state name."
    )
  }

  target_name <- sprintf("%s city, %s", place_name, full_state)

  # ----- 2010–2019 via time_series -----
  df_2010_2019 <- NULL
  if (start_year <= 2019) {
    ts_raw <- tidycensus::get_estimates(
      geography = "place",
      state = state,
      product = "population",
      year = 2019,
      time_series = TRUE
    )

    df_2010_2019 <- ts_raw |>
      dplyr::filter(
        .data$NAME == target_name,
        .data$variable == "POP",
        .data$DATE >= 3,
        .data$DATE <= 12
      ) |>
      dplyr::mutate(
        year = .data$DATE + 2007 # DATE 3 -> 2010, ..., 12 -> 2019
      ) |>
      dplyr::select(.data$NAME, .data$year, .data$variable, .data$value) |>
      dplyr::filter(
        .data$year >= start_year,
        .data$year <= min(end_year, 2019)
      ) |>
      dplyr::arrange(.data$year)
  }

  # ----- 2020–2024 via flat files -----
  df_2020_2024 <- NULL
  if (end_year >= 2020) {
    years_post2020 <- seq.int(max(start_year, 2020), end_year)

    df_2020_2024 <- purrr::map_dfr(
      years_post2020,
      \(yr) {
        tidycensus::get_estimates(
          geography = "place",
          state = state,
          place = place_name,
          variables = "POPESTIMATE",
          vintage = vintage_post2020,
          year = yr
        ) |>
          dplyr::filter(.data$NAME == target_name) |>
          dplyr::mutate(
            year = yr,
            variable = "POP" # harmonize name
          ) |>
          dplyr::select(.data$NAME, .data$year, .data$variable, .data$value)
      }
    ) |>
      dplyr::arrange(.data$year)
  }

  dplyr::bind_rows(df_2010_2019, df_2020_2024) |>
    dplyr::arrange(.data$year)
}

#' Get city population series with change and percent change (2010–2024)
#'
#' @inheritParams get_city_pep_2010_2024
#'
#' @return A tibble with NAME, year, POP (value), change, and pct_change.
#'
get_city_pep_2010_2024_with_change <- function(
  place_name,
  state = "TX",
  start_year = 2010,
  end_year = 2024,
  vintage_post2020 = 2024
) {
  df <- get_city_pep_2010_2024(
    place_name = place_name,
    state = state,
    start_year = start_year,
    end_year = end_year,
    vintage_post2020 = vintage_post2020
  )

  df |>
    dplyr::arrange(.data$year) |>
    dplyr::mutate(
      pop = .data$value,
      pop_lag = dplyr::lag(.data$pop),
      change = .data$pop - .data$pop_lag,
      pct_change = dplyr::if_else(
        is.na(.data$pop_lag) | .data$pop_lag == 0,
        NA_real_,
        100 * .data$change / .data$pop_lag
      )
    )
}
