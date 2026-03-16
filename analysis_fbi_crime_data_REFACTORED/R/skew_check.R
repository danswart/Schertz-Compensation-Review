# =============================================================================
# skew_check.R - Diagnose distribution skewness with flextable output
# =============================================================================

skew_check <- function(x, title = NULL) {
 
 # Remove NAs
 x <- x[!is.na(x)]
  
  # Calculate statistics
  n <- length(x)
  mn <- mean(x)
  md <- median(x)
  s <- sd(x)
  diff <- mn - md
  pearson2 <- 3 * (mn - md) / s
  
  # Interpret skewness
  direction <- dplyr::case_when(
    pearson2 > 1 ~ "Highly right-skewed",
    pearson2 > 0.5 ~ "Moderately right-skewed",
    pearson2 < -1 ~ "Highly left-skewed",
    pearson2 < -0.5 ~ "Moderately left-skewed",
    TRUE ~ "Roughly symmetric"
  )
  
  # Build results tibble
  results <- tibble::tibble(
    Statistic = c("N", "Mean", "Median", "Difference (Mean - Median)", 
                  "Std Dev", "Pearson Skewness", "Interpretation"),
    Value = c(
      format(n, big.mark = ","),
      paste0("$", format(round(mn, 0), big.mark = ",")),
      paste0("$", format(round(md, 0), big.mark = ",")),
      paste0("$", format(round(diff, 0), big.mark = ",")),
      paste0("$", format(round(s, 0), big.mark = ",")),
      format(round(pearson2, 3), nsmall = 3),
      direction
    )
  )
  
  # Create flextable
  ft <- flextable::flextable(results)
  
  # Apply title if provided
  if (!is.null(title)) {
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
  
  # Color-code the interpretation row
  interp_row <- nrow(results)
  if (grepl("right-skewed", direction)) {
    ft <- flextable::color(ft, i = interp_row, j = "Value", color = "darkorange")
    ft <- flextable::bold(ft, i = interp_row, j = "Value")
  } else if (grepl("left-skewed", direction)) {
    ft <- flextable::color(ft, i = interp_row, j = "Value", color = "steelblue")
    ft <- flextable::bold(ft, i = interp_row, j = "Value")
  } else {
    ft <- flextable::color(ft, i = interp_row, j = "Value", color = "darkgreen")
    ft <- flextable::bold(ft, i = interp_row, j = "Value")
  }
  
  ft <- flextable::autofit(ft)
  
  return(ft)
}
