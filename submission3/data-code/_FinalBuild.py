import pandas as pd
import numpy as np
import warnings
warnings.simplefilter("ignore")

# Load necessary files
full_ma_data = pd.read_csv("/Users/ilsenovis/ECON470HW4/data/output/1_full_plan_data.csv")
contract_service_area = pd.read_csv("/Users/ilsenovis/ECON470HW4/data/output/3_full_contract_service_area.csv", dtype={10:str})
star_ratings = pd.read_csv("/Users/ilsenovis/ECON470HW4/data/output/5_star_ratings.csv")
ma_penetration_data = pd.read_csv("/Users/ilsenovis/ECON470HW4/data/output/4_ma_penetration.csv")
benchmark = pd.read_csv("/Users/ilsenovis/ECON470HW4/data/output/7_ma_benchmark.csv")

# Ensure SSA is numeric
benchmark["ssa"] = pd.to_numeric(benchmark["ssa"], errors="coerce")

# Merge full MA with service area
final = full_ma_data.merge(
    contract_service_area[["contractid", "fips", "year"]],
    on=["contractid", "fips", "year"],
    how="inner"
)

# Filter data
final = final[
    (~final["state"].isin(["VI", "PR", "MP", "GU", "AS", ""])) &
    (final["snp"] == "No") &
    ((final["planid"] < 800) | (final["planid"] >= 900)) &
    final["planid"].notna() &
    final["fips"].notna()
]

# Merge star ratings
final = final.merge(
    star_ratings,
    on=["contractid", "year"],
    how="left"
)

# Calculate domain average and rounding difference
domain_cols = [
    'breastcancer_screen', 'rectalcancer_screen', 'cv_diab_cholscreen', 'glaucoma_test',
    'monitoring', 'flu_vaccine', 'pn_vaccine', 'physical_health', 'mental_health',
    'osteo_test', 'physical_monitor', 'primaryaccess', 'osteo_manage', 'diab_healthy',
    'bloodpressure', 'ra_manage', 'copd_test', 'bladder', 'falling', 'nodelays',
    'doctor_communicate', 'carequickly', 'customer_service', 'overallrating_care',
    'overallrating_plan', 'complaints_plan', 'appeals_timely', 'appeals_review',
    'leave_plan', 'audit_problems', 'hold_times', 'info_accuracy', 'ttyt_available',
    'cv_cholscreen', 'diab_cholscreen', 'diabetes_eye', 'diabetes_kidney',
    'diabetes_bloodsugar', 'diabetes_chol', 'bmi_assess', 'older_medication',
    'older_function', 'older_pain', 'readmissions', 'access_problems', 'coordination',
    'improve', 'enroll_timely', 'specialneeds_manage'
]

final["domain_avg"] = final[domain_cols].mean(axis=1, skipna=True)
final["rounding_diff"] = final["partc_score"] - final["domain_avg"]

for threshold in [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]:
    final[f"rounded_up_to_{threshold}"] = (
        (final["partc_score"] == threshold) &
        (np.floor(final["domain_avg"] * 2) / 2 < threshold)
    ).astype(int)

    final[f"rounded_down_to_{threshold}"] = (
        (final["partc_score"] == threshold) &
        (np.floor(final["domain_avg"] * 2) / 2 > threshold)
    ).astype(int)

# Clean penetration data
ma_penetration_data = ma_penetration_data.drop(columns=["ssa_first"], errors="ignore")
ma_penetration_data = ma_penetration_data.rename(columns={
    "fips_": "fips",
    "state_": "state",
    "county_": "county",
    "year_last": "year",
    "state": "state_long",
    "county": "county_long"
})

# Merge penetration data
final = final.merge(
    ma_penetration_data.drop(columns=["ssa"], errors="ignore"),
    on=["fips", "year"],
    how="left"
)

# Calculate Star Rating
final["Star_Rating"] = np.where(
    final["partd"] == "No",
    final["partc_score"],
    np.where(
        final["partd"] == "Yes",
        np.where(
            final["partcd_score"].isna(),
            final["partc_score"],
            final["partcd_score"]
        ),
        np.nan
    )
)

# Get final state name mapping
final_state = (
    final.sort_values("year")
         .groupby("state", as_index=False)
         .agg(state_name=("state_long", lambda x: x.dropna().iloc[-1] if not x.dropna().empty else np.nan))
)
final = final.merge(final_state, on="state", how="left")

# Merge benchmark
final = final.merge(
    benchmark,
    on=["ssa", "year"],
    how="left"
)

# Calculate MA rate based on Star Rating
conditions = [
    final["year"] < 2012,
    (final["year"].between(2012, 2014)) & (final["Star_Rating"] == 5),
    (final["year"].between(2012, 2014)) & (final["Star_Rating"] == 4.5),
    (final["year"].between(2012, 2014)) & (final["Star_Rating"] == 4),
    (final["year"].between(2012, 2014)) & (final["Star_Rating"] == 3.5),
    (final["year"].between(2012, 2014)) & (final["Star_Rating"] == 3),
    (final["year"].between(2012, 2014)) & (final["Star_Rating"] < 3),
    (final["year"].between(2012, 2014)) & (final["Star_Rating"].isna()),
    (final["year"] >= 2015) & (final["Star_Rating"] >= 4),
    (final["year"] >= 2015) & (final["Star_Rating"] < 4),
    (final["year"] >= 2015) & (final["Star_Rating"].isna())
]

choices = [
    final["risk_ab"],
    final["risk_star5"],
    final["risk_star45"],
    final["risk_star4"],
    final["risk_star35"],
    final["risk_star3"],
    final["risk_star25"],
    final["risk_star35"],
    final["risk_bonus5"],
    final["risk_bonus0"],
    final["risk_bonus35"]
]

final["ma_rate"] = np.select(conditions, choices, default=np.nan)
final["ma_rate"] = pd.to_numeric(final["ma_rate"], errors="coerce")

# Save final dataset
final.to_csv("/Users/ilsenovis/ECON470HW4/data/output/_final_ma_data.csv", index=False)
print("Final MA data saved.")
