# =============================================================================
# quick_distrib.R - Visualize distribution with mean/median annotations
# =============================================================================

quick_distrib <- function(x, title = "Distribution") {
  
  # Remove NAs
  x <- x[!is.na(x)]
  
  # Calculate statistics
  mn <- mean(x)
  md <- median(x)
  
  # Create data frame
  df <- data.frame(value = x)
  
  # Determine y position for annotations (top of density)
  dens <- density(x)
  y_max <- max(dens$y)
  
  # Format values for display
  mean_label <- paste0("Mean: $", format(round(mn, 0), big.mark = ","))
  median_label <- paste0("Median: $", format(round(md, 0), big.mark = ","))
  
  # Build plot
  p <- ggplot2::ggplot(df, ggplot2::aes(x = value)) +
    ggplot2::geom_density(fill = "steelblue", alpha = 0.4) +
    
    # Mean line and label
    ggplot2::geom_vline(xintercept = mn, color = "red", linetype = "dashed", linewidth = 1) +
    ggplot2::annotate(
      "label",
      x = mn,
      y = y_max * 0.95,
      label = mean_label,
      color = "red",
      fill = "white",
      size = 4,
      hjust = -0.1
    ) +
    
    # Median line and label
    ggplot2::geom_vline(xintercept = md, color = "darkgreen", linetype = "solid", linewidth = 1) +
    ggplot2::annotate(
      "label",
      x = md,
      y = y_max * 0.80,
      label = median_label,
      color = "darkgreen",
      fill = "white",
      size = 4,
      hjust = -0.1
    ) +
    
    # Labels
    ggplot2::labs(
      title = title,
      subtitle = "Red dashed = Mean | Green solid = Median",
      x = "Value",
      y = "Density"
    ) +
    
    # X-axis formatting for currency
    ggplot2::scale_x_continuous(
      labels = function(x) paste0("$", format(x, big.mark = ","))
    ) +
    
    ggplot2::theme_minimal(base_size = 14) +
    ggplot2::theme(
      plot.title = ggplot2::element_text(face = "bold", size = 16),
      plot.subtitle = ggplot2::element_text(color = "gray40", size = 11)
    )
  
  return(p)
}
