
## Step 1: Load Cached Data


# =============================================================================
# LOAD CACHED DATA FROM OMNIBUS ANALYSIS
# =============================================================================
# Purpose: Load the pre-processed all_data object from the main analysis
# Cache: TRUE - data loads once, subsequent renders use cached version
#
# Expected columns in all_data:
#   - fiscal_year: FY20, FY21, FY22, FY23, FY24, FY25
#   - name: Employee name (Last, First format)
#   - name_std: Standardized name for matching
#   - department: Department name
#   - job_title: Job title
#   - regular_earnings: Base salary/regular pay
#   - additional_benefits: Healthcare, pension, etc.
#   - calc_total_compensation: Total compensation (earnings + benefits)
#   - leave_payout, overtime_earnings, additional_earnings, etc.

# Path to cleaned data (adjust if your path differs)
data_path <- here::here("data_clean", "compensation_wide_fy20_fy25.rds")

# Check if file exists; if not, provide instructions
if (base::file.exists(data_path)) {
  all_data <- base::readRDS(data_path)
  base::saveRDS(all_data, file = here::here("data_clean", "all_data.rds"))  # save all_data for downstream use
  base::cat("Saved all_data.rds to data_clean/\n")
  base::cat("Loaded data:", base::nrow(all_data), "rows\n")
  base::cat("Fiscal years:", base::paste(base::unique(all_data$fiscal_year), collapse = ", "), "\n")
} else {
  base::stop(
    "Data file not found at: ", data_path, "\n",
    "Please run the omnibus analysis first to generate the cleaned data,\n",
    "or update the data_path variable to point to your RDS file."
  )
}

