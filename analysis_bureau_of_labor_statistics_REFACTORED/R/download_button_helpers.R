#  DOWNLOAD BUTTON HELPERS

# -----------------------------------------------------------------------------
# Download button helper (HTML only)
# -----------------------------------------------------------------------------
ds_download_data_btn <- function(df, filename_base, label = "Download Data (CSV)",
                                 clean_df = NULL) {
  if (!knitr::is_html_output()) return(invisible(NULL))
  write_df <- if (!base::is.null(clean_df)) clean_df else df
  tmp <- base::tempfile(fileext = ".csv")
  readr::write_excel_csv(write_df, tmp)
  downloadthis::download_file(
    path         = tmp,
    output_name  = filename_base,
    button_label = label,
    button_type  = "primary",
    has_icon     = TRUE,
    icon         = "fa fa-download"
  )
}

ds_download_plot_btn <- function(plot_obj, filename_base, label = "Download Plot (PNG)",
                                 width = 13, height = 8, dpi = 300) {
  if (!knitr::is_html_output()) return(invisible(NULL))
  tmp <- base::tempfile(fileext = ".png")
  ggplot2::ggsave(tmp, plot = plot_obj, width = width, height = height,
                  dpi = dpi, bg = "white")
  downloadthis::download_file(
    path         = tmp,
    output_name  = filename_base,
    button_label = label,
    button_type  = "warning",
    has_icon     = TRUE,
    icon         = "fa fa-image"
  )
}
