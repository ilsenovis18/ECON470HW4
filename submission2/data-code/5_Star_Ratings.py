import pandas as pd
import os
import pickle

# Load column name dictionary
with open("/Users/ilsenovis/ECON470HW4/data/output/rating_variables.pkl", "rb") as f:
    rating_vars = pickle.load(f)

# Define base path
star_rating_path = "/Users/ilsenovis/ECON470HW4/data/input/5_StarRating"

# Custom file suffix mapping
file_suffixes = {
    2010: ("2009_11_30_domain.csv", "2009_11_30_summary.csv"),
    2011: ("2011_04_20_star.csv", "2011_04_20_summary.csv"),
    2012: ("2011_11_01_Star.csv", "2011_11_01_Summary.csv"),
    2013: ("2012_10_17_Star.csv", "2012_10_17_Summary.csv"),
    2014: ("2014_Part_C_Report_Card_Master_Table_2013_10_17_stars.csv", "2014_Part_C_Report_Card_Master_Table_2013_10_17_summary.csv"),
    2015: ("2015_Report_Card_Master_Table_2014_10_03_stars.csv", "2015_Report_Card_Master_Table_2014_10_03_summary.csv")
}

# Initialize container
star_data_all = []

for year in range(2010, 2016):
    print(f"Processing {year}...")

    # File paths depend on year
    if year <= 2013:
        domain_file = os.path.join(
            star_rating_path,
            f"{year}_Part_C_Report_Card_Master_Table_{file_suffixes[year][0]}"
        )
        summary_file = os.path.join(
            star_rating_path,
            f"{year}_Part_C_Report_Card_Master_Table_{file_suffixes[year][1]}"
        )
    else:
        domain_file = os.path.join(star_rating_path, file_suffixes[year][0])
        summary_file = os.path.join(star_rating_path, file_suffixes[year][1])

    # Read domain file
    domain_cols = rating_vars[str(year)]
    skiprows = 5 if year in [2011, 2012] else 4 if year in [2010, 2013, 2015] else 3

    star_domain = pd.read_csv(domain_file, skiprows=skiprows, names=domain_cols,
                              usecols=range(len(domain_cols)), encoding='latin1')

    # Convert star columns to numeric
    id_vars = [col for col in ["contractid", "org_type", "contract_name", "org_marketing", "org_parent"]
               if col in star_domain.columns]
    num_cols = star_domain.columns.difference(id_vars)
    star_domain[num_cols] = star_domain[num_cols].apply(pd.to_numeric, errors='coerce')

    # Define summary file columns per year
    if year == 2011:
        summary_cols = ["contractid", "org_type", "contract_name", "org_marketing",
                        "partc_lowstar", "partc_score", "partcd_score"]
    elif year == 2012:
        summary_cols = ["contractid", "org_type", "org_parent", "org_marketing",
                        "partc_score", "partc_lowscore", "partc_highscore",
                        "partcd_score", "partcd_lowscore", "partcd_highscore"]
    elif year == 2013:
        summary_cols = ["contractid", "org_type", "org_marketing", "contract_name", "org_parent",
                        "partc_score", "partc_lowscore", "partc_highscore",
                        "partcd_score", "partcd_lowscore", "partcd_highscore"]
    elif year == 2014:
        summary_cols = ["contractid", "org_type", "org_marketing", "contract_name", "org_parent",
                        "snp", "sanction", "partc_score", "partcd_score"]
    elif year == 2015:
        summary_cols = ["contractid", "org_type", "org_marketing", "contract_name", "org_parent",
                        "snp", "sanction", "partc_score", "partdscore", "partcd_score"]
    else:
        summary_cols = ["contractid", "org_type", "contract_name", "org_marketing", "partc_score"]

    # Read summary file
    star_summary = pd.read_csv(summary_file, skiprows=2, names=summary_cols,
                               usecols=range(len(summary_cols)), encoding='latin1')

    # Flag new contracts
    if "partcd_score" in star_summary.columns:
        star_summary["new_contract"] = star_summary.apply(
            lambda row: int("Plan too new to be measured" in str(row["partc_score"]) or
                            "Plan too new to be measured" in str(row["partcd_score"])), axis=1)
    else:
        star_summary["new_contract"] = star_summary["partc_score"].apply(
            lambda x: int("Plan too new to be measured" in str(x)))

    # Convert scores to numeric
    for col in ["partc_score", "partcd_score"]:
        if col in star_summary.columns:
            star_summary[col] = pd.to_numeric(star_summary[col], errors='coerce')

    # Flag low score plans
    if "partc_lowscore" in star_summary.columns:
        star_summary["low_score"] = star_summary["partc_lowscore"].apply(
            lambda x: 1 if str(x).strip().lower() == "yes" else 0
        )

    keep_cols = [col for col in ["contractid", "new_contract", "low_score", "partc_score", "partcd_score"]
                 if col in star_summary.columns]
    star_summary = star_summary[keep_cols]

    # Merge and save
    star = pd.merge(star_domain, star_summary, on="contractid", how="left")
    star["year"] = year
    star_data_all.append(star)

# Combine all years
star_ratings = pd.concat(star_data_all, ignore_index=True)
star_ratings.to_csv("/Users/ilsenovis/ECON470HW4/data/output/5_star_ratings.csv", index=False)

print("Star ratings saved.")