# Schertz Compensation Data Pipeline Configuration
# ================================================
# This file defines all inputs, mappings, corrections, and validation targets.
#
# To add a new fiscal year:
#   1. Add the file info to file_registry below
#   2. Run the profiler on the new file
#   3. Add column mappings if structure differs from existing years
#   4. Add any manual corrections discovered during profiling
#   5. Run the full pipeline

# ------------------------------------------------------------------------------
# PATHS
# ------------------------------------------------------------------------------

paths <- list(
  # Source Excel files
  data_raw = file.path(
    path.expand("~"),
    "R Working Directory",
    "Schertz Compensation Review",
    "data_raw"
  ),

  # Final clean data (production)
  data_clean = file.path(
    path.expand("~"),
    "R Working Directory",
    "Schertz Compensation Review",
    "data_clean"
  ),

  # Test outputs (during development/validation)
  output_test = file.path(
    path.expand("~"),
    "R Working Directory",
    "Schertz Compensation Review",
    "output"
  ),

  # Reconciliation reports
  reports = file.path(
    path.expand("~"),
    "R Working Directory",
    "Schertz Compensation Review",
    "reports"
  ),

  # Pipeline code
  pipeline = file.path(
    path.expand("~"),
    "R Working Directory",
    "Schertz Compensation Review",
    "pipeline"
  )
)

# ------------------------------------------------------------------------------
# FILE REGISTRY
# ------------------------------------------------------------------------------
# Each fiscal year's source file and its structure type
# structure_type determines which column mapping to use

file_registry <- dplyr::tribble(
  ~fiscal_year , ~filename                         , ~structure_type ,
  "FY20"       , "FY20 Earnings.xlsx"              , "early"         ,
  "FY21"       , "FY21 Earnings.xlsx"              , "early"         ,
  "FY22"       , "FY22 Earnings.xlsx"              , "early"         ,
  "FY23"       , "FY23Employee_Comp_FY23.xlsx"     , "late"          ,
  "FY24"       , "FY24Employee_Comp_FY24_All.xlsx" , "late"
)

# ------------------------------------------------------------------------------
# COLUMN MAPPINGS
# ------------------------------------------------------------------------------
# Maps source column names (after clean_names()) to standardized names
# Use regex patterns where column names vary

column_mappings <- list(
  # FY20, FY21, FY22 structure
  early = list(
    name_source = "combined",
    name_col = "name",
    department = "department|home_department",
    job_title = "job_title",
    hire_date = "hire_date",
    separation_date = "termination_date",
    regular_earnings = "regular_earnings",
    overtime_earnings = "overtime_earnings",
    additional_earnings = "additional_earnings1"
  ),

  # FY23, FY24 structure (separate first/last name columns)
  late = list(
    name_source = "separate",
    last_name = "last_name",
    first_name = "first_name",
    department = "home_department",
    job_title = "job_title",
    hire_date = "hire_date",
    separation_date = "separation_date",
    regular_earnings = "fy\\d+_regular_earnings",
    overtime_earnings = "fy\\d+_overtime_earnings",
    additional_earnings = "^fy\\d+_additional$"
  )
)

# ------------------------------------------------------------------------------
# MANUAL NAME CORRECTIONS
# ------------------------------------------------------------------------------
# Patterns applied to standardized names (after middle initial removal, etc.)
# Use regex patterns; $ anchors to end of string
# Add new corrections as discovered during profiling

name_corrections <- dplyr::tribble(
  ~pattern           , ~replacement        , ~note                  ,
  "Pitts jr,"        , "Pitts Jr.,"        , "Case standardization" ,
  "Dammann, Aa$"     , "Dammann, Aaron"    , "Truncated first name" ,
  "McGuire, Mich$"   , "McGuire, Michael"  , "Truncated first name" ,
  "Hernandez, Joe$"  , "Hernandez, Joseph" , "Name variant"         ,
  "Hernandez, Jose$" , "Hernandez, Joseph" , "Name variant"
  # Add new corrections here as discovered
)

# ------------------------------------------------------------------------------
# KEY ENTITIES FOR COMPLETENESS VERIFICATION
# ------------------------------------------------------------------------------
# People/positions you KNOW should appear in the data
# Used to catch silent omissions before presenting analysis
# Update this as you learn about key personnel

key_entities <- dplyr::tribble(
  ~name_pattern       , ~job_title_pattern , ~expected_years    , ~note                  ,
  "Browne, James"     , "City Manager"     , "FY20, FY21, FY22" , "Former City Manager"  ,
  "Williams, Stephen" , "City Manager"     , "FY23, FY24"       , "Current City Manager" ,
  "Gutierrez, Rafael" , "Mayor"            , "FY23, FY24"       , "Mayor"
  # Add Council Members, Department Heads, etc. as needed
)

# ------------------------------------------------------------------------------
# EXTERNAL RECONCILIATION TARGETS (Optional)
# ------------------------------------------------------------------------------
# Published totals from city budget, CAFR, or transparency portal
# Used to verify your totals match official figures
# Set to NULL if not available

external_totals <- dplyr::tribble(
  ~fiscal_year , ~published_total , ~source ,
  #  "FY20",       32000000,         "City Budget FY20",
  #  "FY21",       35000000,         "City Budget FY21",
  #  "FY22",       40000000,         "City Budget FY22",
  #  "FY23",       52000000,         "City Budget FY23",
  #  "FY24",       64000000,         "City Budget FY24"
)
# Uncomment and fill in when you have official figures

# ------------------------------------------------------------------------------
# VALIDATION THRESHOLDS
# ------------------------------------------------------------------------------

validation_thresholds <- list(
  # Name similarity detection

  name_edit_distance_max = 2,

  # Acceptable variance from external totals (as decimal, e.g., 0.01 = 1%)
  external_total_tolerance = 0.02,

  # Year-over-year growth that triggers a warning (as decimal)
  yoy_growth_warning = 0.25,

  # Minimum expected rows per fiscal year
  min_rows_per_fy = 100
)

# ------------------------------------------------------------------------------
# REPORT SETTINGS
# ------------------------------------------------------------------------------

report_settings <- list(
  # Include TEST in filenames during development
  test_mode = TRUE,

  # Date format for report filenames
  date_format = "%Y-%m-%d",

  # Maximum similar name pairs to show in report
  max_similar_names_display = 50
)
