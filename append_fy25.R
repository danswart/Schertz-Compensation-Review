# ==============================================================================
# APPEND FY25 TO EXISTING CLEAN DATAFRAME
# ==============================================================================
#
# This script does ONE thing: takes the FY25 Excel file and appends it to
# your validated compensation_long_fy20_fy24.rds
#
# ==============================================================================

library(here)
library(readxl)
library(dplyr)
library(tidyr)
library(stringr)
library(janitor)

# ------------------------------------------------------------------------------
# STEP 1: Load existing clean data
# ------------------------------------------------------------------------------

long_df <- readRDS(here::here("data_clean", "compensation_long_fy20_fy24.rds"))

cat("Existing data:\n")
cat("  Rows:", nrow(long_df), "\n")
cat("  Years:", paste(unique(long_df$fiscal_year), collapse = ", "), "\n")
cat("  Unique employees:", n_distinct(long_df$employee_key), "\n\n")

# ------------------------------------------------------------------------------
# STEP 2: Read and clean FY25
# ------------------------------------------------------------------------------

fy25_raw <- read_excel(
  here::here("data_raw", "Employee_Compensation_Report_FY25.xlsx")
) |>
  clean_names()

cat("FY25 raw data:\n")
cat("  Rows:", nrow(fy25_raw), "\n\n")

# ------------------------------------------------------------------------------
# STEP 3: Transform FY25 to match existing structure
# ------------------------------------------------------------------------------

fy25_clean <- fy25_raw |>
  # Remove TOTALS row and any empty rows
  filter(
    !is.na(last_name),
    !str_detect(tolower(last_name), "^total")
  ) |>
  # Create name fields to match existing format
  mutate(
    # Combined name: "Last, First"
    name = paste0(last_name, ", ", first_name),

    # Standardized name: "LAST FIRST" (uppercase, no comma)
    name_std = name |>
      str_to_upper() |>
      str_remove_all(",") |>
      str_squish(),

    # Keep last_name and first_name as-is (FY25 has them)
    last_name = last_name,
    first_name = first_name,

    # Department and job_title as-is
    department = department,
    job_title = job_title,

    # Employee key: "NAME_STD_Department"
    employee_key = paste0(name_std, "_", department),

    # Fiscal year
    fiscal_year = "FY25"
  ) |>
  # Select and rename earnings columns for pivot
  select(
    fiscal_year,
    name,
    name_std,
    last_name,
    first_name,
    department,
    job_title,
    employee_key,
    # Map FY25 columns to earnings types
    Leave = fy25_leave_payout,
    Regular = fy25_regular_earnings,
    Overtime = fy25_overtime_earnings,
    Additional = fy25_additional_earnings,
    Deployment = fy25_deployment_earnings,
    Arbitration = arbitration_settlements_fy25,
    Benefits = fy25_additional_benefits
  ) |>
  # Pivot to long format (one row per earnings type)
  pivot_longer(
    cols = c(
      Leave,
      Regular,
      Overtime,
      Additional,
      Deployment,
      Arbitration,
      Benefits
    ),
    names_to = "earnings_type",
    values_to = "amount"
  ) |>
  # Ensure amount is numeric
  mutate(
    amount = as.numeric(amount)
  )

cat("FY25 cleaned:\n")
cat("  Rows:", nrow(fy25_clean), "\n")
cat("  Employees:", n_distinct(fy25_clean$employee_key), "\n")
cat(
  "  Columns match existing:",
  all(names(fy25_clean) == names(long_df)),
  "\n\n"
)

# ------------------------------------------------------------------------------
# STEP 4: Validate before binding
# ------------------------------------------------------------------------------

cat("Column comparison:\n")
cat("  Existing:", paste(names(long_df), collapse = ", "), "\n")
cat("  FY25:    ", paste(names(fy25_clean), collapse = ", "), "\n\n")

# Check for any structure mismatches
if (!all(names(fy25_clean) == names(long_df))) {
  stop("Column mismatch! Review before binding.")
}

# Quick sanity check on amounts
cat("FY25 earnings summary:\n")
fy25_clean |>
  group_by(earnings_type) |>
  summarise(
    records = n(),
    total = sum(amount, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(total = scales::dollar(total)) |>
  print()

# ------------------------------------------------------------------------------
# STEP 5: Bind and save
# ------------------------------------------------------------------------------

# Combine
long_df_updated <- bind_rows(long_df, fy25_clean)

cat("\n\nCombined data:\n")
cat("  Rows:", nrow(long_df_updated), "\n")
cat(
  "  Years:",
  paste(unique(long_df_updated$fiscal_year), collapse = ", "),
  "\n"
)
cat("  Unique employees:", n_distinct(long_df_updated$employee_key), "\n\n")

# Row count by year
cat("Rows by fiscal year:\n")
long_df_updated |>
  count(fiscal_year) |>
  print()

# Save updated dataframe
saveRDS(
  long_df_updated,
  here::here("data_clean", "compensation_long_fy20_fy25.rds")
)
cat("\nSaved: data_clean/compensation_long_fy20_fy25.rds\n")


write.csv(
  long_df_updated,
  here::here("data_clean", "compensation_long_fy20_fy25.csv"),
  row.names = FALSE
)
cat("Saved: data_clean/compensation_long_fy20_fy25.csv\n")

# ------------------------------------------------------------------------------
# STEP 6: Verify key positions (the test that matters)
# ------------------------------------------------------------------------------

cat("\n============================================================\n")
cat("KEY POSITIONS CHECK\n")
cat("============================================================\n")

key_positions <- c("City Manager", "Police Chief", "Fire Chief", "EMS Chief")

long_df_updated |>
  filter(job_title %in% key_positions, earnings_type == "Regular") |>
  distinct(fiscal_year, job_title, name) |>
  arrange(job_title, fiscal_year) |>
  print(n = 30)
