# =============================================================================
# distrib_quantile_evolution.R - Track how percentiles shift over time
# =============================================================================
# Requires: ggplot2, dplyr, tidyr

# Helper function for weighted quantiles
weighted_quantile <- function(x, w, probs) {
  # Remove NAs
  valid <- !is.na(x) & !is.na(w)
  x <- x[valid]
  w <- w[valid]
  
  # Sort by value
  ord <- order(x)
  x <- x[ord]
  w <- w[ord]
  
  # Cumulative weights normalized to 0-1
  cum_w <- cumsum(w) / sum(w)
  
  # Find quantiles
  sapply(probs, function(p) {
    idx <- which(cum_w >= p)[1]
    if (is.na(idx)) return(NA_real_)
    x[idx]
  })
}

distrib_quantile_evolution <- function(data,
                                       value_col,
                                       year_col = "year",
                                       weight_col = "PWGTP",
                                       group_col = NULL,
                                       probs = c(0.10, 0.25, 0.50, 0.75, 0.90),
                                       title = "Earnings Distribution Evolution",
                                       currency = TRUE) {
  
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
  
  # Calculate WEIGHTED quantiles by year (and group if present)
  if (!is.null(group_col)) {
    quantiles <- df |>
      dplyr::group_by(year, group) |>
      dplyr::reframe(
        percentile = paste0("P", probs * 100),
        value = weighted_quantile(value, weight, probs)
      ) |>
      dplyr::ungroup()
  } else {
    quantiles <- df |>
      dplyr::group_by(year) |>
      dplyr::reframe(
        percentile = paste0("P", probs * 100),
        value = weighted_quantile(value, weight, probs)
      ) |>
      dplyr::ungroup()
  }
  
  # Order percentiles for legend
  quantiles$percentile <- factor(
    quantiles$percentile,
    levels = paste0("P", probs * 100)
  )
  
  # Define colors - darker for extremes, lighter for middle
  n_probs <- length(probs)
  colors <- c("#1a237e", "#5c6bc0", "#26a69a", "#ffb300", "#d84315")
  if (n_probs != 5) {
    colors <- viridis::viridis(n_probs, option = "turbo")
  }
  
  # Build plot
  p <- ggplot2::ggplot(quantiles, ggplot2::aes(x = year, y = value, color = percentile)) +
    ggplot2::geom_line(linewidth = 1.2) +
    ggplot2::geom_point(size = 3) +
    ggplot2::scale_color_manual(
      values = colors,
      name = "Percentile"
    ) +
    ggplot2::labs(
      title = title,
      subtitle = "How different parts of the distribution shift over time",
      x = "Year",
      y = "Earnings",
      caption = "Diverging lines = inequality growth | Parallel lines = uniform shift"
    ) +
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(face = "bold", size = 16),
      plot.subtitle = ggplot2::element_text(color = "gray40", size = 11),
      plot.caption = ggplot2::element_text(color = "gray50", size = 10, hjust = 0),
      legend.position = "right",
      panel.grid.minor = ggplot2::element_blank()
    )
  
  # Currency formatting
  if (currency) {
    p <- p + ggplot2::scale_y_continuous(
      labels = function(x) paste0("$", format(x / 1000, big.mark = ","), "K")
    )
  }
  
  # Facet by group if provided
  if (!is.null(group_col)) {
    p <- p + ggplot2::facet_wrap(~ group, scales = "free_y")
  }
  
  return(p)
}
