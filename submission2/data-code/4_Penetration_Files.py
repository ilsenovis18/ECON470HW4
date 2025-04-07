import pandas as pd
import os

penetration_path = "/Users/ilsenovis/ECON470HW4/data/input/4_PenetrationRate"

monthlist = {year: [f"{m:02d}" for m in range(1, 13)] for year in range(2010, 2016)}

yearly_penetration = []


for year, months in monthlist.items():
    monthly_data = []

    for m in months:
        ma_path = os.path.join(penetration_path, f"State_County_Penetration_MA_{year}_{m}.csv")

        if not os.path.exists(ma_path):
            print(f"Missing file: {ma_path}")
            continue

        df = pd.read_csv(
            ma_path,
            skiprows=1,
            names=[
                "state", "county", "fips_state", "fips_cnty", "fips",
                "ssa_state", "ssa_cnty", "ssa", "eligibles", "enrolled", "penetration"
            ],
            na_values="*",
            dtype={"state": str, "county": str}
        )

        # Clean and convert numeric values
        df["eligibles"] = df["eligibles"].replace(",", "", regex=True).astype(float)
        df["enrolled"] = df["enrolled"].replace(",", "", regex=True).astype(float)
        df["penetration"] = df["penetration"].replace("%", "", regex=True).astype(float)
        df["fips"] = pd.to_numeric(df["fips"], errors="coerce")

        df["month"] = m
        df["year"] = year
        monthly_data.append(df)

    if not monthly_data:
        continue

    ma_penetration = pd.concat(monthly_data, ignore_index=True)

    ma_penetration["fips"] = ma_penetration.groupby(["state", "county"])["fips"].transform(lambda x: x.ffill().bfill())

    collapsed = (
        ma_penetration
        .groupby(["fips", "state", "county"], as_index=False)
        .agg(
            avg_eligibles=('eligibles', 'mean'),
            sd_eligibles=('eligibles', 'std'),
            min_eligibles=('eligibles', 'min'),
            max_eligibles=('eligibles', 'max'),
            first_eligibles=('eligibles', 'first'),
            last_eligibles=('eligibles', 'last'),
            avg_enrolled=('enrolled', 'mean'),
            sd_enrolled=('enrolled', 'std'),
            min_enrolled=('enrolled', 'min'),
            max_enrolled=('enrolled', 'max'),
            first_enrolled=('enrolled', 'first'),
            last_enrolled=('enrolled', 'last'),
            year=('year', 'last'),
            ssa=('ssa', 'first')
        )
    )

    yearly_penetration.append(collapsed)

if yearly_penetration:
    ma_penetration_data = pd.concat(yearly_penetration, ignore_index=True)
    ma_penetration_data.to_csv("/Users/ilsenovis/ECON470HW4/data/output/4_ma_penetration.csv", index=False)
    print("File saved successfully!")
else:
    print("No data available to export.")