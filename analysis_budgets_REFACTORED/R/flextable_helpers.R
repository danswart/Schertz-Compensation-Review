# flextable_helpers.R
# Reusable flextable helpers for Quarto analytical reports
# Source this file from your setup chunk after setting any document-wide
# flextable defaults you want to use.

ds_style_flextable <- function(ft, title = NULL, title_size = 16) {
  # Fixed layout helps long text wrap instead of expanding columns
  ft <- flextable::set_table_properties(ft, layout = "fixed")

  if (!base::is.null(title)) {
    ft <- flextable::add_header_lines(ft, values = title)
    ft <- flextable::color(ft, i = 1, part = "header", color = "blue")
    ft <- flextable::italic(ft, i = 1, part = "header")
    ft <- flextable::align(ft, i = 1, part = "header", align = "left")
    ft <- flextable::bold(ft, i = 1, part = "header")
    ft <- flextable::fontsize(ft, i = 1, part = "header", size = title_size)
    ft <- flextable::bg(ft, i = 1, part = "header", bg = "white")
    ft <- flextable::bg(ft, i = 2, part = "header", bg = "palegreen")

    # Optional: make wrapped titles read better
    ft <- flextable::line_spacing(ft, i = 1, part = "header", space = 1.2)
  } else {
    ft <- flextable::bg(ft, i = 1, part = "header", bg = "palegreen")
  }

  ft <- flextable::autofit(ft)
  ft
}

ds_ft_apply_spec <- function(ft, spec) {
  if (!base::is.null(spec$labels)) {
    ft <- do.call(flextable::set_header_labels, c(list(x = ft), spec$labels))
  }

  if (!base::is.null(spec$align_right)) {
    ft <- flextable::align(ft, j = spec$align_right, align = "right", part = "all")
  }
  if (!base::is.null(spec$align_center)) {
    ft <- flextable::align(ft, j = spec$align_center, align = "center", part = "all")
  }
  if (!base::is.null(spec$align_left)) {
    ft <- flextable::align(ft, j = spec$align_left, align = "left", part = "all")
  }

  if (!base::is.null(spec$bold_rows)) {
    ft <- flextable::bold(ft, i = spec$bold_rows, part = spec$bold_part %||% "body")
  }
  if (!base::is.null(spec$bold_cols)) {
    ft <- flextable::bold(ft, j = spec$bold_cols, part = spec$bold_col_part %||% "body")
  }
  if (!base::is.null(spec$bold_body) && isTRUE(spec$bold_body)) {
    ft <- flextable::bold(ft, part = "body")
  }

  if (!base::is.null(spec$bg_rows)) {
    for (b in spec$bg_rows) {
      ft <- flextable::bg(ft, i = b$i, bg = b$bg, part = b$part %||% "body")
    }
  }

  if (!base::is.null(spec$color_cells)) {
    for (c in spec$color_cells) {
      ft <- flextable::color(
        ft,
        i = c$i %||% NULL,
        j = c$j %||% NULL,
        color = c$color,
        part = c$part %||% "body"
      )
    }
  }

  if (!base::is.null(spec$hline)) {
    for (h in spec$hline) {
      ft <- flextable::hline(
        ft,
        i = h$i,
        border = h$border,
        part = h$part %||% "body"
      )
    }
  }

  if (!base::is.null(spec$italic_rows)) {
    ft <- flextable::italic(ft, i = spec$italic_rows, part = spec$italic_part %||% "body")
  }

  if (!base::is.null(spec$footer_lines)) {
    ft <- flextable::add_footer_lines(ft, values = spec$footer_lines)
  }
  if (!base::is.null(spec$footer_fontsize)) {
    ft <- flextable::fontsize(ft, part = "footer", size = spec$footer_fontsize)
  }
  if (!base::is.null(spec$footer_italic) && isTRUE(spec$footer_italic)) {
    ft <- flextable::italic(ft, part = "footer")
  }

  if (!base::is.null(spec$fontsize_cells)) {
    for (fs in spec$fontsize_cells) {
      ft <- flextable::fontsize(
        ft,
        i = fs$i %||% NULL,
        j = fs$j %||% NULL,
        part = fs$part %||% "body",
        size = fs$size
      )
    }
  }

  ds_style_flextable(ft, title = spec$title %||% NULL)
}

ds_ft <- function(df, spec) {
  flextable::flextable(df) |>
    ds_ft_apply_spec(spec)
}
