import pandas as pd
import os

ffs_base_path = '/Users/ilsenovis/ECON470HW4/data/input/8_FFS_Costs'

ffs_paths = {
    2010: os.path.join(ffs_base_path, "aged10.csv"),
    2011: os.path.join(ffs_base_path, "aged11.csv"),
    2012: os.path.join(ffs_base_path, "aged12.csv"),
    2013: os.path.join(ffs_base_path, "aged13.csv"),
    2014: os.path.join(ffs_base_path, "aged14.csv"),
    2015: os.path.join(ffs_base_path, "FFS15.xlsx")
}

ffs_rows_to_skip = {
    2010: 7,
    2011: 2,
    2012: 2,
    2013: 2,
    2014: 2,
    2015: 2
}

ffs_data_yearly = {}

## 2010-2014
for year in range(2010, 2015):
    file_path = ffs_paths[year]
    skip_rows = ffs_rows_to_skip[year]

    df = pd.read_csv(
        file_path,
        skiprows=skip_rows,
        header=None,
        na_values='*',
        encoding='latin1'
    )

    df = df.iloc[:, :15]

    df.columns = [
        "ssa", "state", "county_name", "parta_enroll",
        "parta_reimb", "parta_percap", "parta_reimb_unadj",
        "parta_percap_unadj", "parta_ime", "parta_dsh",
        "parta_gme", "partb_enroll", "partb_reimb",
        "partb_percap", "mean_risk"
    ]

    df = df[[
        "ssa", "state", "county_name", "parta_enroll", "parta_reimb",
        "partb_enroll", "partb_reimb", "mean_risk"
    ]]

    df['year'] = year
    df['ssa'] = pd.to_numeric(df['ssa'], errors='coerce')

    for col in ['parta_enroll', 'parta_reimb', 'partb_enroll', 'partb_reimb', 'mean_risk']:
        df[col] = df[col].astype(str).str.replace(",", "")
        df[col] = pd.to_numeric(df[col], errors='coerce')

    ffs_data_yearly[year] = df

## 2015
year = 2015
file_path = ffs_paths[year]
skip_rows = ffs_rows_to_skip[year]

ffs_2015 = pd.read_excel(
    file_path,
    skiprows=skip_rows,
    na_values="*",
    names=[
        "ssa", "state", "county_name", "parta_enroll",
        "parta_reimb", "parta_percap", "parta_reimb_unadj",
        "parta_percap_unadj", "parta_ime", "parta_dsh",
        "parta_gme", "partb_enroll",
        "partb_reimb", "partb_percap",
        "mean_risk"
    ]
)

ffs_2015 = ffs_2015[[
    "ssa", "state", "county_name", "parta_enroll", "parta_reimb",
    "partb_enroll", "partb_reimb", "mean_risk"
]].copy()

ffs_2015["year"] = year
ffs_2015["ssa"] = pd.to_numeric(ffs_2015["ssa"], errors="coerce")

for col in ["parta_enroll", "parta_reimb", "partb_enroll", "partb_reimb", "mean_risk"]:
    ffs_2015[col] = ffs_2015[col].astype(str).str.replace(",", "")
    ffs_2015[col] = pd.to_numeric(ffs_2015[col], errors="coerce")

ffs_data_yearly[2015] = ffs_2015

ffs_costs_final = pd.concat(ffs_data_yearly.values(), ignore_index=True)

ffs_costs_final.to_csv('/Users/ilsenovis/ECON470HW4/data/output/ffs_costs.csv', index=False)