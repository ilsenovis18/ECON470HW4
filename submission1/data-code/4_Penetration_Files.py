import pandas as pd
import os

penetration_path = "/Users/ilsenovis/ECON470HW4/data/input/4_PenetrationRate"

monthlist = {
    2010: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
    2011: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
    2012: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
    2013: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
    2014: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
    2015: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
}

yearly_penetration = []

numeric_cols = [
            'fips_state', 'fips_cnty', 'fips',
            'ssa_state', 'ssa_cnty', 'ssa',
            'eligibles', 'enrolled', 'penetration'
        ]

for year in range(2010, 2016):
    months = monthlist[year]
    ma_penetration = pd.DataFrame()  # initialize here

    for m in months:
        ma_path = os.path.join(
            penetration_path, f"State_County_Penetration_MA_{year}_{m}.csv"
        )
        if not os.path.exists(ma_path):
            print(f" missing file: {ma_path}")
            continue

        pene_data = pd.read_csv(
            ma_path,
            skiprows=1,
            names=[
                "state", "county", "fips_state", "fips_cnty", "fips",
                "ssa_state", "ssa_cnty", "ssa", "eligibles", "enrolled", "penetration"
            ],
            na_values="*",
            dtype={
                'state': str,
                'county': str,
            }
        )

        for col in numeric_cols:
            if col in pene_data.columns:
                pene_data[col] = pd.to_numeric(pene_data[col], errors='coerce')

        pene_data["month"] = m
        pene_data["year"] = year

        ma_penetration = pd.concat([ma_penetration, pene_data], ignore_index=True)

    # Fill missing fips codes
    ma_penetration["fips"] = (
        ma_penetration.groupby(["state", "county"])["fips"]
        .transform(lambda x: x.ffill().bfill())
    )

    # Collapse to yearly data
    collapsed = (
        ma_penetration.groupby(["fips", "state", "county"], as_index=False)
        .agg({
            'eligibles': ['mean', 'std', 'min', 'max', 'first', 'last'],
            'enrolled': ['mean', 'std', 'min', 'max', 'first', 'last'],
            'year': 'last',
            'ssa': 'first'
        })
    )

    # Flatten MultiIndex columns
    collapsed.columns = [
        "_".join(col) if isinstance(col, tuple) else col
        for col in collapsed.columns.values
    ]

    yearly_penetration.append(collapsed)

# Combine all years
ma_penetration_data = pd.concat(yearly_penetration, ignore_index=True)
ma_penetration_data.reset_index(drop=True, inplace=True)

# Optional: save it
ma_penetration_data.to_csv("/Users/ilsenovis/ECON470HW4/data/output/4_ma_penetration.csv", index=False)