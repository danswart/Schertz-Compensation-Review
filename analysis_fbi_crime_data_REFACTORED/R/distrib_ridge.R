# =============================================================================
# distrib_ridge.R - Ridge plots showing distribution evolution over time
# =============================================================================
# Requires: ggplot2, ggridges, dplyr

distrib_ridge <- function(data, 
                          value_col, 
                          year_col = "year",
                          weight_col = "PWGTP",
                          group_col = NULL,
                          title = "Distribution Evolution Over Time",
                          x_label = "Value",
                          currency = TRUE) {
 
  # Build the base data frame
  df <- data.frame(
    value = data[[value_col]],
    year = factor(data[[year_col]])
  )
  
  # Add weights if available

  if (weight_col %in% names(data)) {
    df$weight <- data[[weight_col]]
  } else {
    df$weight <- 1
    message("Note: No weight column '", weight_col, "' found. Using unweighted estimates.")
  }
  
  # Add grouping if specified (for faceting by gov_level, etc.)
  if (!is.null(group_col)) {
    df$group <- data[[group_col]]
  }
  
  # Remove NAs and non-positive values
  df <- df[!is.na(df$value) & df$value > 0 & !is.na(df$weight), ]
  
  # Calculate means by year (and group if present) for reference lines
  if (!is.null(group_col)) {
    means <- dplyr::summarise(
      dplyr::group_by(df, year, group),
      mean_val = mean(value, na.rm = TRUE),
      .groups = "drop"
    )
  } else {
    means <- dplyr::summarise(
      dplyr::group_by(df, year),
      mean_val = mean(value, na.rm = TRUE),
      .groups = "drop"
    )
  }
  
  # Build plot - using weight aesthetic for survey weights
  p <- ggplot2::ggplot(df, ggplot2::aes(x = value, y = year, fill = year, weight = weight)) +
    ggridges::geom_density_ridges(
      alpha = 0.7,
      scale = 1.2,
      rel_min_height = 0.01,
      quantile_lines = TRUE,
      quantiles = 2  # Shows median as vertical line
    ) +
    ggplot2::scale_fill_viridis_d(option = "plasma", direction = -1) +
    ggplot2::labs(
      title = title,
      subtitle = "Vertical line within each ridge = median",
      x = x_label,
      y = "Year"
    ) +
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(face = "bold", size = 16),
      plot.subtitle = ggplot2::element_text(color = "gray40", size = 11),
      legend.position = "none",
      panel.grid.minor = ggplot2::element_blank()
    )
  
  # Format x-axis as currency if requested
  if (currency) {
    p <- p + ggplot2::scale_x_continuous(
      labels = function(x) paste0("$", format(x / 1000, big.mark = ","), "K"),
      expand = ggplot2::expansion(mult = c(0.02, 0.05))
    )
  }
  
  # Add faceting if group column provided
  if (!is.null(group_col)) {
    p <- p + ggplot2::facet_wrap(~ group, scales = "free_x", ncol = 1)
  }
  
  return(p)
}
