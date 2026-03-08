# =============================================================================
# distrib_acceleration.R - Analyze rate of change and acceleration in distributions
# =============================================================================
# Requires: dplyr, flextable

# Helper function for weighted quantiles
weighted_quantile <- function(x, w, probs) {
  valid <- !is.na(x) & !is.na(w)
  x <- x[valid]
  w <- w[valid]
  ord <- order(x)
  x <- x[ord]
  w <- w[ord]
  cum_w <- cumsum(w) / sum(w)
  sapply(probs, function(p) {
    idx <- which(cum_w >= p)[1]
    if (is.na(idx)) return(NA_real_)
    x[idx]
  })
}

# Helper for weighted mean
weighted_mean <- function(x, w) {
  valid <- !is.na(x) & !is.na(w)
  sum(x[valid] * w[valid]) / sum(w[valid])
}

# Helper for weighted SD
weighted_sd <- function(x, w) {
  valid <- !is.na(x) & !is.na(w)
  x <- x[valid]
  w <- w[valid]
  mu <- sum(x * w) / sum(w)
  sqrt(sum(w * (x - mu)^2) / sum(w))
}

# Helper for weighted IQR
weighted_iqr <- function(x, w) {
  q <- weighted_quantile(x, w, c(0.25, 0.75))
  q[2] - q[1]
}

distrib_acceleration <- function(data,
                                 value_col,
                                 year_col = "year",
                                 weight_col = "PWGTP",
                                 group_col = NULL,
                                 title = "Distribution Change Analysis") {
  
  # Build base data
  df <- data.frame(
    value = data[[value_col]],
    year = data[[year_col]]
  )
  
  # Add weights if available
  if (weight_col %in% names(data)) {
    df$weight <- data[[weight_col]]
  } else {
    df$weight <- 1
    message("Note: No weight column '", weight_col, "' found. Using unweighted estimates.")
  }
  
  if (!is.null(group_col)) {
    df$group <- data[[group_col]]
  }
  
  # Remove NAs and non-positive
  df <- df[!is.na(df$value) & df$value > 0 & !is.na(df$weight), ]
  
  # Calculate WEIGHTED summary stats by year (and group)
  if (!is.null(group_col)) {
    summary_stats <- df |>
      dplyr::group_by(year, group) |>
      dplyr::summarise(
        n = sum(weight),  # Weighted count (population estimate)
        mean = weighted_mean(value, weight),
        median = weighted_quantile(value, weight, 0.5),
        sd = weighted_sd(value, weight),
        p10 = weighted_quantile(value, weight, 0.10),
        p90 = weighted_quantile(value, weight, 0.90),
        iqr = weighted_iqr(value, weight),
        .groups = "drop"
      ) |>
      dplyr::arrange(group, year)
    
    # Calculate changes within each group
    result <- summary_stats |>
      dplyr::group_by(group) |>
      dplyr::mutate(
        # Year-over-year percent changes
        mean_pct_chg = (mean - dplyr::lag(mean)) / dplyr::lag(mean) * 100,
        median_pct_chg = (median - dplyr::lag(median)) / dplyr::lag(median) * 100,
        
        # Acceleration (change in percent change)
        mean_accel = mean_pct_chg - dplyr::lag(mean_pct_chg),
        median_accel = median_pct_chg - dplyr::lag(median_pct_chg),
        
        # Spread changes
        iqr_pct_chg = (iqr - dplyr::lag(iqr)) / dplyr::lag(iqr) * 100,
        
        # 90/10 ratio (inequality measure)
        p90_p10_ratio = p90 / p10,
        ratio_chg = p90_p10_ratio - dplyr::lag(p90_p10_ratio),
        
        # Coefficient of variation (relative spread)
        cv = sd / mean * 100,
        cv_chg = cv - dplyr::lag(cv)
      ) |>
      dplyr::ungroup()
    
  } else {
    summary_stats <- df |>
      dplyr::group_by(year) |>
      dplyr::summarise(
        n = sum(weight),
        mean = weighted_mean(value, weight),
        median = weighted_quantile(value, weight, 0.5),
        sd = weighted_sd(value, weight),
        p10 = weighted_quantile(value, weight, 0.10),
        p90 = weighted_quantile(value, weight, 0.90),
        iqr = weighted_iqr(value, weight),
        .groups = "drop"
      ) |>
      dplyr::arrange(year)
    
    result <- summary_stats |>
      dplyr::mutate(
        mean_pct_chg = (mean - dplyr::lag(mean)) / dplyr::lag(mean) * 100,
        median_pct_chg = (median - dplyr::lag(median)) / dplyr::lag(median) * 100,
        mean_accel = mean_pct_chg - dplyr::lag(mean_pct_chg),
        median_accel = median_pct_chg - dplyr::lag(median_pct_chg),
        iqr_pct_chg = (iqr - dplyr::lag(iqr)) / dplyr::lag(iqr) * 100,
        p90_p10_ratio = p90 / p10,
        ratio_chg = p90_p10_ratio - dplyr::lag(p90_p10_ratio),
        cv = sd / mean * 100,
        cv_chg = cv - dplyr::lag(cv)
      )
  }
  
  # Format for display
  if (!is.null(group_col)) {
    display <- result |>
      dplyr::select(
        group, year, n, mean, median,
        mean_pct_chg, mean_accel,
        median_pct_chg, median_accel,
        p90_p10_ratio, ratio_chg
      ) |>
      dplyr::mutate(
        Year = as.character(year),
        Group = group,
        N = format(n, big.mark = ","),
        Mean = paste0("$", format(round(mean, 0), big.mark = ",")),
        Median = paste0("$", format(round(median, 0), big.mark = ",")),
        `Mean Δ%` = ifelse(is.na(mean_pct_chg), "—", 
                          paste0(ifelse(mean_pct_chg >= 0, "+", ""), round(mean_pct_chg, 1), "%")),
        `Accel` = ifelse(is.na(mean_accel), "—",
                        paste0(ifelse(mean_accel >= 0, "+", ""), round(mean_accel, 1), "pp")),
        `90/10` = round(p90_p10_ratio, 2),
        `90/10 Δ` = ifelse(is.na(ratio_chg), "—",
                          paste0(ifelse(ratio_chg >= 0, "+", ""), round(ratio_chg, 2)))
      ) |>
      dplyr::select(Group, Year, N, Mean, Median, `Mean Δ%`, `Accel`, `90/10`, `90/10 Δ`)
    
  } else {
    display <- result |>
      dplyr::select(
        year, n, mean, median,
        mean_pct_chg, mean_accel,
        median_pct_chg, median_accel,
        p90_p10_ratio, ratio_chg
      ) |>
      dplyr::mutate(
        Year = as.character(year),
        N = format(n, big.mark = ","),
        Mean = paste0("$", format(round(mean, 0), big.mark = ",")),
        Median = paste0("$", format(round(median, 0), big.mark = ",")),
        `Mean Δ%` = ifelse(is.na(mean_pct_chg), "—", 
                          paste0(ifelse(mean_pct_chg >= 0, "+", ""), round(mean_pct_chg, 1), "%")),
        `Accel` = ifelse(is.na(mean_accel), "—",
                        paste0(ifelse(mean_accel >= 0, "+", ""), round(mean_accel, 1), "pp")),
        `90/10` = round(p90_p10_ratio, 2),
        `90/10 Δ` = ifelse(is.na(ratio_chg), "—",
                          paste0(ifelse(ratio_chg >= 0, "+", ""), round(ratio_chg, 2)))
      ) |>
      dplyr::select(Year, N, Mean, Median, `Mean Δ%`, `Accel`, `90/10`, `90/10 Δ`)
  }
  
  # Create flextable
  ft <- flextable::flextable(display)
  
  # Add title
  ft <- flextable::add_header_lines(ft, values = title)
  ft <- flextable::add_header_lines(ft, values = "Accel = change in % change (pp) | 90/10 = P90÷P10 ratio (inequality)")
  
  # Style header rows
  ft <- flextable::color(ft, i = 1, part = "header", color = "gray50")
  ft <- flextable::fontsize(ft, i = 1, part = "header", size = 10)
  ft <- flextable::italic(ft, i = 1, part = "header")
  
  ft <- flextable::color(ft, i = 2, part = "header", color = "blue")
  ft <- flextable::fontsize(ft, i = 2, part = "header", size = 12)
  ft <- flextable::italic(ft, i = 2, part = "header")
  ft <- flextable::align(ft, i = 2, part = "header", align = "left")
  
  ft <- flextable::bg(ft, i = 1:2, part = "header", bg = "white")
  ft <- flextable::bg(ft, i = 3, part = "header", bg = "palegreen")
  
  ft <- flextable::autofit(ft)
  
  # Return both the data and the flextable
  return(list(
    data = result,
    table = ft
  ))
}
