# report_format_helpers.R
# Reusable formatting helpers for Quarto analytical reports

fmt_dollar <- function(x, digits = 0) {
  paste0("$", formatC(x, format = "f", digits = digits, big.mark = ","))
}

fmt_pct <- function(x, digits = 1) {
  paste0(formatC(x * 100, format = "f", digits = digits), "%")
}

`%||%` <- function(x, y) if (!base::is.null(x)) x else y
