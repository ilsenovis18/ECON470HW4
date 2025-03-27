import pandas as pd
import numpy as np

## load files
data_path = "/Users/ilsenovis/ECON470HW4/data/output"
full_ma_data = pd.read_csv('/Users/ilsenovis/ECON470HW4/data/output/1_full_plan_data.csv')
contract_service_area = pd.read_csv('/Users/ilsenovis/ECON470HW4/data/output/3_full_contract_service_area.csv', dtype={10:str})
star_ratings = pd.read_csv('/Users/ilsenovis/ECON470HW4/data/output/5_star_ratings.csv')
ma_penetration_data = pd.read_csv('/Users/ilsenovis/ECON470HW4/data/output/4_ma_penetration.csv')
plan_premiums = pd.read_csv('/Users/ilsenovis/ECON470HW4/data/output/2_plan_premiums.csv')
risk_rebates = pd.read_csv('/Users/ilsenovis/ECON470HW4/data/output/6_risk_rebates.csv')
benchmark = pd.read_csv('/Users/ilsenovis/ECON470HW4/data/output/7_ma_benchmark.csv')
ffs_costs = pd.read_csv('/Users/ilsenovis/ECON470HW4/data/output/8_ffs_costs.csv')

## ensure necessary columns are numeric
benchmark['ssa'] = pd.to_numeric(benchmark['ssa'], errors='coerce')
ffs_costs['ssa'] = pd.to_numeric(ffs_costs['ssa'], errors='coerce')

## merge full MA with service area
final = full_ma_data.merge(
    contract_service_area[['contractid', 'fips', 'year']],
    on=['contractid', 'fips', 'year'],
    how='inner'
)

## filter data
final = final[
    (~final["state"].isin(["VI", "PR", "MP", "GU", "AS", ""])) &
    (final["snp"] == "No") &
    ((final["planid"] < 800) | (final["planid"] >= 900)) &
    final["planid"].notna() &
    final["fips"].notna()
]

## join star ratings
final = final.merge(
    star_ratings.drop(columns=["contract_name", "org_type", "org_marketing"], errors='ignore'),
    on=["contractid", "year"],
    how="left"
)

## clean penetration data
ma_penetration_data = ma_penetration_data.rename(columns={
    "fips_": "fips",
    "state_": "state",
    "county_": "county",
    "year_last": "year"
})

ma_penetration_data = ma_penetration_data.drop(columns=["ssa_first"], errors='ignore')

ma_penetration_data = ma_penetration_data.rename(columns={
    "state": "state_long",
    "county": "county_long"
})

## merge penetration into final
final = final.merge(
    ma_penetration_data,
    on=["fips", "year"],
    how="left",
    validate="many_to_one"
)

## calculate star rating based on partc and partd scores
conditions = [
    final["partd"] == "No",
    (final["partd"] == "Yes") & (final["partcd_score"].isna()),
    (final["partd"] == "Yes") & (final["partcd_score"].notna())
]

choices = [
    final["partc_score"],
    final["partc_score"],
    final["partcd_score"]
]

final["Star_Rating"] = np.select(conditions, choices, default=np.nan)

final_state = (
    final[["state", "state_long"]]
    .dropna(subset=["state", "state_long"])
    .drop_duplicates()
    .groupby("state", as_index=False)
    .last()
    .rename(columns={"state_long": "state_name"})
)

final = final.merge(final_state, on="state", how="left")

## join plan premiums using state name
final = final.merge(
    plan_premiums,
    left_on=["contractid", "planid", "state_name", "county", "year"],
    right_on=["contractid", "planid", "state", "county", "year"],
    how="left"
)

## join risk rebates
risk_rebates_clean = risk_rebates.drop(columns=["contract_name", "plan_type"], errors="ignore")

final = final.merge(
    risk_rebates_clean,
    on=["contractid", "planid", "year"],
    how="left"
)

## join benchmark
final = final.merge(
    benchmark,
    on=["ssa", "year"],
    how="left"
)

## calculate relevant benchmark rate based on star rating
conditions = [
    final["year"] < 2012,
    (final["year"] >= 2012) & (final["year"] < 2015) & (final["Star_Rating"] == 5),
    (final["year"] >= 2012) & (final["year"] < 2015) & (final["Star_Rating"] == 4.5),
    (final["year"] >= 2012) & (final["year"] < 2015) & (final["Star_Rating"] == 4),
    (final["year"] >= 2012) & (final["year"] < 2015) & (final["Star_Rating"] == 3.5),
    (final["year"] >= 2012) & (final["year"] < 2015) & (final["Star_Rating"] == 3),
    (final["year"] >= 2012) & (final["year"] < 2015) & (final["Star_Rating"] < 3),
    (final["year"] >= 2012) & (final["year"] < 2015) & (final["Star_Rating"].isna()),
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

## final premium and bid variables
# calulate basic premium
final["basic_premium"] = np.select(
    [
        final["rebate_partc"] > 0,
        (final["partd"] == "No") & final["premium"].notna() & final["premium_partc"].isna()
    ],
    [
        0,
        final["premium"]
    ],
    default=final["premium_partc"]
)

# calculate bid
final["bid"] = np.select(
    [
        (final["rebate_partc"] == 0) & (final["basic_premium"] > 0),
        (final["rebate_partc"] > 0) | (final["basic_premium"] == 0)
    ],
    [
        (final["payment_partc"] + final["basic_premium"]) / final["riskscore_partc"],
        final["payment_partc"] / final["riskscore_partc"]
    ],
    default=np.nan
)

ffs_costs_clean = ffs_costs.drop(columns=["state"], errors="ignore")

final = final.merge(
    ffs_costs_clean,
    on=["ssa", "year"],
    how="left"
)

# calculate avgerage ffs cost
conditions = [
    (final["parta_enroll"] == 0) & (final["partb_enroll"] == 0),
    (final["parta_enroll"] == 0) & (final["partb_enroll"] > 0),
    (final["parta_enroll"] > 0) & (final["partb_enroll"] == 0),
    (final["parta_enroll"] > 0) & (final["partb_enroll"] > 0)
]

choices = [
    0,
    final["partb_reimb"] / final["partb_enroll"],
    final["parta_reimb"] / final["parta_enroll"],
    (final["parta_reimb"] / final["parta_enroll"]) + (final["partb_reimb"] / final["partb_enroll"])
]

final["avg_ffscost"] = np.select(conditions, choices, default=np.nan)

final.to_csv("/Users/ilsenovis/ECON470HW4/data/output/_final_ma_data.csv", index=False)