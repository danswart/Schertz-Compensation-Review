#' Plot city population (level, numeric change, or percent change) for 2010–2024
#'
#' @param place_name City name, e.g. "Schertz"
#' @param state      State postal abbreviation, e.g. "TX"
#' @param start_year First year (default 2010)
#' @param end_year   Last year (default 2024)
#' @param type       One of: "level" (population),
#'                   "change" (numeric year-to-year change),
#'                   "pct_change" (percent year-to-year change)
#' @param vintage_post2020 PEP vintage (default 2024)
#'
#' @return A ggplot object
#'
plot_city_pop <- function(
  place_name,
  state = "TX",
  start_year = 2010,
  end_year = 2024,
  type = c("level", "change", "pct_change"),
  vintage_post2020 = 2024
) {
  type <- match.arg(type)

  df <- get_city_pep_2010_2024_with_change(
    place_name = place_name,
    state = state,
    start_year = start_year,
    end_year = end_year,
    vintage_post2020 = vintage_post2020
  )

  full_state <- state.name[match(toupper(state), state.abb)]
  city_label <- sprintf("%s, %s", place_name, state)
  year_lbl <- sprintf("%d–%d", min(df$year), max(df$year))

  if (type == "level") {
    p <- ggplot2::ggplot(df, ggplot2::aes(x = year, y = pop)) +
      ggplot2::geom_line() +
      ggplot2::geom_point() +
      ggplot2::labs(
        title = sprintf("Population of %s", city_label),
        subtitle = sprintf("Annual Population Estimates, %s", year_lbl),
        x = "Year",
        y = "Population (PEP)"
      )
  } else if (type == "change") {
    p <- ggplot2::ggplot(df, ggplot2::aes(x = year, y = change)) +
      ggplot2::geom_col() +
      ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
      ggplot2::labs(
        title = sprintf(
          "Numeric year-over-year population change in %s",
          city_label
        ),
        subtitle = sprintf("Annual Population Estimates, %s", year_lbl),
        x = "Year",
        y = "Change in population (people)"
      )
  } else if (type == "pct_change") {
    p <- ggplot2::ggplot(df, ggplot2::aes(x = year, y = pct_change)) +
      ggplot2::geom_col() +
      ggplot2::geom_hline(yintercept = 0, linetype = "dashed") +
      ggplot2::labs(
        title = sprintf(
          "Percent year-over-year population change in %s",
          city_label
        ),
        subtitle = sprintf("Annual Population Estimates, %s", year_lbl),
        x = "Year",
        y = "Percent change (%)"
      )
  }

  p
}
