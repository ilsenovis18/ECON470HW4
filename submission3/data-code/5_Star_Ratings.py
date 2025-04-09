import pandas as pd
import os
import pickle

# Load column names for 2010 from pickle
with open("/Users/ilsenovis/ECON470HW4/data/output/rating_variables.pkl", "rb") as f:
    rating_vars = pickle.load(f)

star_rating_path = "/Users/ilsenovis/ECON470HW4/data/input/5_StarRating"

## 2010
# File paths for 2010
domain_file10 = os.path.join(star_rating_path, "2010_Part_C_Report_Card_Master_Table_2009_11_30_domain.csv")
summary_file10 = os.path.join(star_rating_path, "2010_Part_C_Report_Card_Master_Table_2009_11_30_summary.csv")

# Get column names from the pickle
column_names_2010 = rating_vars["2010"]

# Define common replacements
star_replacements10 = {
    "1 out of 5 stars": "1",
    "1.5 out of 5 stars": "1.5",
    "2 out of 5 stars": "2",
    "2.5 out of 5 stars": "2.5",
    "3 out of 5 stars": "3",
    "3.5 out of 5 stars": "3.5",
    "4 out of 5 stars": "4",
    "4.5 out of 5 stars": "4.5",
    "5 out of 5 stars": "5",
    "5 stars": "5"
}

# Load domain file
star_data_2010a = pd.read_csv(
    domain_file10,
    skiprows=4,
    names=column_names_2010,
    usecols=range(len(column_names_2010)),
    encoding='latin1'
)

# Clean all string values and apply replacements
star_data_2010a = star_data_2010a.applymap(
    lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
)

# Convert rating columns to numeric (excluding ID columns)
id_cols = ["contractid", "org_type", "contract_name", "org_marketing"]
cols_to_convert = [col for col in star_data_2010a.columns if col not in id_cols]
star_data_2010a[cols_to_convert] = star_data_2010a[cols_to_convert].apply(pd.to_numeric, errors='coerce')

# Load summary file
star_data_2010b = pd.read_csv(
    summary_file10,
    skiprows=2,
    names=["contractid", "org_type", "contract_name", "org_marketing", "partc_score"],
    usecols=range(5),
    encoding='latin1'
)

# Flag new contracts
star_data_2010b["new_contract"] = star_data_2010b["partc_score"].apply(
    lambda x: 1 if str(x).strip().lower() == "plan too new to be measured" else 0
)

# Normalize and convert partc_score
star_data_2010b["partc_score"] = star_data_2010b["partc_score"].apply(
    lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
)
star_data_2010b["partc_score"] = pd.to_numeric(star_data_2010b["partc_score"], errors="coerce")

# Keep relevant columns
star_data_2010b = star_data_2010b[["contractid", "new_contract", "partc_score"]]

# Merge domain and summary data
star_data_2010 = pd.merge(star_data_2010a, star_data_2010b, on="contractid", how="left")
star_data_2010["year"] = 2010

## 2011
# File paths for 2011
domain_file11 = os.path.join(star_rating_path, "2011_Part_C_Report_Card_Master_Table_2011_04_20_star.csv")
summary_file11 = os.path.join(star_rating_path, "2011_Part_C_Report_Card_Master_Table_2011_04_20_summary.csv")

# Get column names from the pickle
column_names_2011 = rating_vars["2011"]

# Load domain file
star_data_2011a = pd.read_csv(
    domain_file11,
    skiprows=5,
    names=column_names_2011,
    usecols=range(len(column_names_2011)),
    encoding='latin1'
)

# Clean all string values and apply replacements
star_data_2011a = star_data_2011a.applymap(
    lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
)

# Convert numeric columns
id_cols_2011 = ["contractid", "org_type", "contract_name", "org_marketing"]
cols_to_convert_2011 = [col for col in star_data_2011a.columns if col not in id_cols_2011]
star_data_2011a[cols_to_convert_2011] = star_data_2011a[cols_to_convert_2011].apply(pd.to_numeric, errors="coerce")

# Load summary file
summary_cols_2011 = ["contractid", "org_type", "contract_name", "org_marketing",
                     "partc_lowstar", "partc_score", "partcd_score"]
star_data_2011b = pd.read_csv(
    summary_file11,
    skiprows=2,
    names=summary_cols_2011,
    usecols=range(len(summary_cols_2011)),
    encoding='latin1'
)

# Flag new contracts
star_data_2011b["new_contract"] = star_data_2011b.apply(
    lambda row: 1 if str(row["partc_score"]).strip().lower() == "plan too new to be measured"
                  or str(row["partcd_score"]).strip().lower() == "plan too new to be measured" else 0,
    axis=1
)

# Normalize and convert partc_score and partcd_score
for col in ["partc_score", "partcd_score"]:
    star_data_2011b[col] = star_data_2011b[col].apply(
        lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
    )
    star_data_2011b[col] = pd.to_numeric(star_data_2011b[col], errors="coerce")

# Create low_score flag
star_data_2011b["low_score"] = star_data_2011b["partc_lowstar"].apply(
    lambda x: 1 if str(x).strip().lower() == "yes" else 0
)

# Keep relevant columns
star_data_2011b = star_data_2011b[["contractid", "new_contract", "low_score", "partc_score", "partcd_score"]]

# Merge domain and summary data
star_data_2011 = pd.merge(star_data_2011a, star_data_2011b, on="contractid", how="left")
star_data_2011["year"] = 2011

##2012
# 2012 files
domain_file12 = os.path.join(
    star_rating_path, "2012_Part_C_Report_Card_Master_Table_2011_11_01_Star.csv"
)
summary_file12 = os.path.join(
    star_rating_path, "2012_Part_C_Report_Card_Master_Table_2011_11_01_Summary.csv"
)

# Column names from rating_variables.pkl for 2012
column_names_2012 = rating_vars["2012"]

# Load domain file
star_data_2012a = pd.read_csv(
    domain_file12,
    skiprows=5,
    names=column_names_2012,
    usecols=range(len(column_names_2012)),
    encoding='latin1'
)

# Clean all string values and apply replacements
star_data_2012a = star_data_2012a.applymap(
    lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
)

# Convert relevant columns to numeric
cols_to_convert = star_data_2012a.columns.difference(
    ["contractid", "org_type", "org_parent", "org_marketing"]
)
star_data_2012a[cols_to_convert] = star_data_2012a[cols_to_convert].apply(
    pd.to_numeric, errors="coerce"
)

# Load summary file
star_data_2012b = pd.read_csv(
    summary_file12,
    skiprows=2,
    names=[
        "contractid", "org_type", "org_parent", "org_marketing",
        "partc_score", "partc_lowscore", "partc_highscore",
        "partcd_score", "partcd_lowscore", "partcd_highscore"
    ],
    usecols=range(10),
    encoding='latin1'
)

# Create flags
star_data_2012b['new_contract'] = star_data_2012b.apply(
    lambda row: 1 if str(row['partc_score']).strip().lower() == "plan too new to be measured" or str(row['partcd_score']).strip().lower() == "plan too new to be measured" else 0,
    axis=1
)

# Normalize and convert partc_score and partcd_score
for col in ["partc_score", "partcd_score"]:
    star_data_2012b[col] = star_data_2012b[col].apply(
        lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
    )
    star_data_2012b[col] = pd.to_numeric(star_data_2012b[col], errors="coerce")

# Create low_score flag
star_data_2012b['low_score'] = star_data_2012b['partc_lowscore'].apply(
    lambda x: 1 if str(x).strip().lower() == "yes" else 0
)

# Keep relevant columns
star_data_2012b = star_data_2012b[[
    "contractid", "new_contract", "low_score", "partc_score", "partcd_score"
]]

# Merge domain and summary
star_data_2012 = pd.merge(star_data_2012a, star_data_2012b, on="contractid", how="left")
star_data_2012["year"] = 2012

## 2013
# 2013 file paths
domain_file13 = os.path.join(
    star_rating_path, "2013_Part_C_Report_Card_Master_Table_2012_10_17_Star.csv"
)
summary_file13 = os.path.join(
    star_rating_path, "2013_Part_C_Report_Card_Master_Table_2012_10_17_Summary.csv"
)

# Get column names from the pickle
column_names_2013 = rating_vars["2013"]

# Read domain data
star_data_2013a = pd.read_csv(
    domain_file13,
    skiprows=4,
    names=column_names_2013,
    usecols=range(len(column_names_2013)),
    encoding="latin1"
)

# Clean all string values and apply replacements
star_data_2013a = star_data_2013a.applymap(
    lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
)

# Convert all quality measures to numeric
cols_to_convert = star_data_2013a.columns.difference(
    ["contractid", "org_type", "contract_name", "org_marketing", "org_parent"]
)
star_data_2013a[cols_to_convert] = star_data_2013a[cols_to_convert].apply(
    pd.to_numeric, errors="coerce"
)

# Read summary data
star_data_2013b = pd.read_csv(
    summary_file13,
    skiprows=2,
    names=[
        "contractid", "org_type", "org_marketing", "contract_name", "org_parent",
        "partc_score", "partc_lowscore", "partc_highscore",
        "partcd_score", "partcd_lowscore", "partcd_highscore"
    ],
    usecols=range(11),
    encoding="latin1"
)

# Flag new contracts
star_data_2013b["new_contract"] = star_data_2013b.apply(
    lambda x: 1 if str(x["partc_score"]).strip().lower() == "plan too new to be measured" or str(x["partcd_score"]).strip().lower() == "plan too new to be measured" else 0,
    axis=1
)

# Normalize and convert partc_score and partcd_score
for col in ["partc_score", "partcd_score"]:
    star_data_2013b[col] = star_data_2013b[col].apply(
        lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
    )
    star_data_2013b[col] = pd.to_numeric(star_data_2013b[col], errors="coerce")

# Create low_score flag
star_data_2013b["low_score"] = star_data_2013b["partc_lowscore"].apply(
    lambda x: 1 if str(x).strip().lower() == "yes" else 0
)

# Keep selected columns
star_data_2013b = star_data_2013b[[
    "contractid", "new_contract", "low_score", "partc_score", "partcd_score"
]]

# Merge and add year
star_data_2013 = pd.merge(star_data_2013a, star_data_2013b, on="contractid", how="left")
star_data_2013["year"] = 2013

## 2014
# 2014 file paths
domain_file14 = os.path.join(
    star_rating_path, "2014_Part_C_Report_Card_Master_Table_2013_10_17_stars.csv"
)
summary_file14 = os.path.join(
    star_rating_path, "2014_Part_C_Report_Card_Master_Table_2013_10_17_summary.csv"
)

# Column names from rating_variables.pkl for 2014
column_names_2014 = rating_vars["2014"]

# Read domain file
star_data_2014a = pd.read_csv(
    domain_file14,
    skiprows=3,
    names=column_names_2014,
    usecols=range(len(column_names_2014)),
    encoding="latin1"
)

# Clean all string values and apply replacements
star_data_2014a = star_data_2014a.applymap(
    lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
)

# Convert relevant columns to numeric
cols_to_convert = star_data_2014a.columns.difference([
    "contractid", "org_type", "contract_name", "org_marketing", "org_parent"
])
star_data_2014a[cols_to_convert] = star_data_2014a[cols_to_convert].apply(pd.to_numeric, errors="coerce")

# Read summary file
star_data_2014b = pd.read_csv(
    summary_file14,
    skiprows=2,
    names=[
        "contractid", "org_type", "org_marketing", "contract_name", "org_parent",
        "snp", "sanction", "partc_score", "partcd_score"
    ],
    usecols=range(9),
    encoding="latin1"
)

# Flag new contracts
star_data_2014b["new_contract"] = star_data_2014b.apply(
    lambda row: 1 if str(row["partc_score"]).strip().lower() == "plan too new to be measured" or str(row["partcd_score"]).strip().lower() == "plan too new to be measured" else 0,
    axis=1
)

# Normalize and convert partc_score and partcd_score
for col in ["partc_score", "partcd_score"]:
    star_data_2014b[col] = star_data_2014b[col].apply(
        lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
    )
    star_data_2014b[col] = pd.to_numeric(star_data_2014b[col], errors="coerce")

# Keep only necessary columns
star_data_2014b = star_data_2014b[["contractid", "new_contract", "partc_score", "partcd_score"]]

# Merge and label year
star_data_2014 = pd.merge(star_data_2014a, star_data_2014b, on="contractid", how="left")
star_data_2014["year"] = 2014

## 2015
# 2015 file paths
domain_file15 = os.path.join(
    star_rating_path, "2015_Report_Card_Master_Table_2014_10_03_stars.csv"
)
summary_file15 = os.path.join(
    star_rating_path, "2015_Report_Card_Master_Table_2014_10_03_summary.csv"
)

# Column names from rating_variables.pkl for 2015
column_names_2015 = rating_vars["2015"]

# Read domain file
star_data_2015a = pd.read_csv(
    domain_file15,
    skiprows=4,
    names=column_names_2015,
    usecols=range(len(column_names_2015)),
    encoding="latin1"
)

# Clean all string values and apply replacements
star_data_2015a = star_data_2015a.applymap(
    lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
)

# Convert to numeric where appropriate
cols_to_convert = star_data_2015a.columns.difference([
    "contractid", "org_type", "contract_name", "org_marketing", "org_parent"
])
star_data_2015a[cols_to_convert] = star_data_2015a[cols_to_convert].apply(pd.to_numeric, errors="coerce")

# Read summary file
star_data_2015b = pd.read_csv(
    summary_file15,
    skiprows=2,
    names=[
        "contractid", "org_type", "org_marketing", "contract_name", "org_parent",
        "snp", "sanction", "partc_score", "partdscore", "partcd_score"
    ],
    usecols=range(10),
    encoding="latin1"
)

# Flag new contracts
star_data_2015b["new_contract"] = star_data_2015b.apply(
    lambda row: 1 if str(row["partc_score"]).strip().lower() == "plan too new to be measured" or str(row["partcd_score"]).strip().lower() == "plan too new to be measured" else 0,
    axis=1
)

# Normalize and convert partc_score and partcd_score
for col in ["partc_score", "partcd_score"]:
    star_data_2015b[col] = star_data_2015b[col].apply(
        lambda x: star_replacements10.get(str(x).strip().lower(), x) if isinstance(x, str) else x
    )
    star_data_2015b[col] = pd.to_numeric(star_data_2015b[col], errors="coerce")

# Keep only needed columns
star_data_2015b = star_data_2015b[["contractid", "new_contract", "partc_score", "partcd_score"]]

# Merge and assign year
star_data_2015 = pd.merge(star_data_2015a, star_data_2015b, on="contractid", how="left")
star_data_2015["year"] = 2015

star_ratings = pd.concat([
    star_data_2010,
    star_data_2011,
    star_data_2012,
    star_data_2013,
    star_data_2014,
    star_data_2015
], ignore_index=True)

star_ratings = pd.DataFrame(star_ratings)

if 'new_contract' not in star_ratings.columns:
    star_ratings['new_contract'] = 0

star_ratings.to_csv("/Users/ilsenovis/ECON470HW4/data/output/5_star_ratings.csv", index=False)

# Identify plans likely rounded up or down
def tag_rounded_plans(df, score_col="partc_score", bw=0.125):
    # Identify domain columns: exclude non-domain vars
    domain_cols = df.columns.difference([
        "contractid", "year", "partc_score", "partcd_score",
        "new_contract", "low_score"
    ])

    # Compute average of domain scores (excluding NaNs)
    df["domain_avg"] = df[domain_cols].mean(axis=1, skipna=True)

    # Compute difference between final score and domain average
    df["rounding_diff"] = df[score_col] - df["domain_avg"]

    # Tag rounded up/down based on bandwidth
    df["rounded_up"] = (df["rounding_diff"] >= bw).astype(int)
    df["rounded_down"] = (df["rounding_diff"] <= -bw).astype(int)

    return df

# Apply rounding flags by star threshold
def tag_rounded_plans_by_threshold(df, score_col="partc_score", bw=0.125):
    # Identify domain columns: only numeric columns excluding specific metadata
    domain_cols = df.select_dtypes(include='number').columns.difference([
        "partc_score", "partcd_score", "new_contract", "low_score"
    ])

    # Compute domain average
    df["domain_avg"] = df[domain_cols].mean(axis=1, skipna=True)
    df["rounding_diff"] = df[score_col] - df["domain_avg"]

    # Check for rounding to specific star thresholds
    thresholds = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    for t in thresholds:
        df[f"rounded_up_to_{t}"] = ((df[score_col] == t) & ((df["domain_avg"] + bw) <= t)).astype(int)
        df[f"rounded_down_to_{t}"] = ((df[score_col] == t) & ((df["domain_avg"] - bw) >= t)).astype(int)

    return df

# Tag rounded plans and save final output
star_ratings = tag_rounded_plans_by_threshold(star_ratings, score_col="partc_score", bw=0.125)
star_ratings.to_csv("/Users/ilsenovis/ECON470HW4/data/output/5_star_ratings.csv", index=False)