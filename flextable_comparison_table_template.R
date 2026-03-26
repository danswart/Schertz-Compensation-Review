## ============================================================
##  Schertz Civic Analytics — Comparison Table
##  Dan Swart | Add rows to `comparison_data` to extend
## ============================================================

library(flextable)
library(ftExtra)
library(dplyr)

# ── 1.  DATA  ─────────────────────────────────────────────────
#  Add rows here — that's all you need to do to extend the table.

comparison_data <- tibble::tribble(
  ~Description,                                      ~Year,  ~City_Data,   ~Comparative_Data,
  "Per Capita Earnings — vs Surrounding Schertz Area",  2023,   "$90,000",    "$43,174"
  # ── add more rows below, same pattern ──────────────────────
  # "Next metric description",                          2024,   "$XX,XXX",    "$XX,XXX",
)

# ── 2.  COLUMN LABELS  ────────────────────────────────────────
col_labels <- c(
  Description       = "Description",
  Year              = "Year",
  City_Data         = "City Data",
  Comparative_Data  = "Comparative Data"
)

# ── 3.  BUILD FLEXTABLE  ──────────────────────────────────────
ds_style_flextable <- function(df, col_labels, title_text = NULL) {

  ft <- flextable::flextable(df) |>
    flextable::set_header_labels(values = col_labels)

  # Optional title row above column headers
  if (!is.null(title_text)) {
    ft <- ft |>
      flextable::add_header_lines(values = title_text)
  }

  # ── Header row 1 (title line, if present) ──
  if (!is.null(title_text)) {
    ft <- ft |>
      flextable::bold(part = "header", i = 1) |>
      flextable::color(part = "header", i = 1, color = "#1F3864") |>
      flextable::italic(part = "header", i = 1) |>
      flextable::fontsize(part = "header", i = 1, size = 11) |>
      flextable::bg(part = "header", i = 1, bg = "white") |>
      flextable::align(part = "header", i = 1, align = "left")
  }

  # ── Header row 2 (column labels) ──
  header_label_row <- if (!is.null(title_text)) 2L else 1L

  ft <- ft |>
    flextable::bold(part = "header", i = header_label_row) |>
    flextable::color(part = "header", i = header_label_row, color = "#1F3864") |>
    flextable::italic(part = "header", i = header_label_row) |>
    flextable::fontsize(part = "header", i = header_label_row, size = 10) |>
    flextable::bg(part = "header", i = header_label_row, bg = "palegreen") |>
    flextable::align(part = "header", i = header_label_row, align = "center")

  # ── Body ──
  ft <- ft |>
    ftExtra::colformat_md() |>
    flextable::fontsize(part = "body", size = 10) |>
    flextable::align(part = "body", j = "Year",             align = "center") |>
    flextable::align(part = "body", j = "City_Data",        align = "right")  |>
    flextable::align(part = "body", j = "Comparative_Data", align = "right")  |>
    flextable::align(part = "body", j = "Description",      align = "left")   |>
    flextable::autofit()

  ft
}

# ── 4.  RENDER  ───────────────────────────────────────────────
comparison_table <- ds_style_flextable(
  df          = comparison_data,
  col_labels  = col_labels,
  title_text  = "Schertz Fiscal Comparisons"
)

comparison_table
