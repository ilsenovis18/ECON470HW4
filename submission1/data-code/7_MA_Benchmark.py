import pandas as pd
import os

benchmark_base_path = '/Users/ilsenovis/ECON470HW4/data/input/7_MABenchmark'

bench_path = {
    2010: os.path.join(benchmark_base_path, "CountyRate2010.csv"),
    2011: os.path.join(benchmark_base_path, "CountyRate2011.csv"),
    2012: os.path.join(benchmark_base_path, "CountyRate2012.csv"),
    2013: os.path.join(benchmark_base_path, "CountyRate2013.csv"),
    2014: os.path.join(benchmark_base_path, "CountyRate2014.csv"),
    2015: os.path.join(benchmark_base_path, "CountyRate2015.csv")
}

# define number of rows to skip
benchmark_rows_to_skip = {
    2010: 9,
    2011: 11,
    2012: 8,
    2013: 4,
    2014: 2,
    2015: 3
}

## Years 2010-2016
benchmark_data = []

for year in range(2010, 2016):
    file_path = bench_path[year]
    skip_rows = benchmark_rows_to_skip[year]

    if year in [2010, 2011]:
        df = pd.read_csv(
            file_path,
            skiprows=skip_rows,
            names=[
                'ssa', 'state', 'county_name', 'aged_parta',
                'aged_partb', 'disabled_parta', 'disabled_partb',
                'esrd_ab', 'risk_ab'
            ],
            encoding='latin1'
        )
        df = df[['ssa', 'aged_parta', 'aged_partb', 'risk_ab']]
        df = df.assign(
            risk_star5=pd.NA, risk_star45=pd.NA, risk_star4=pd.NA,
            risk_star35=pd.NA, risk_star3=pd.NA, risk_star25=pd.NA,
            risk_bonus5=pd.NA, risk_bonus35=pd.NA, risk_bonus0=pd.NA,
            year=year
        )
    
    elif year in [2012, 2013, 2014]:
        df = pd.read_csv(
            file_path,
            skiprows=skip_rows,
            names=[
                'ssa', 'state', 'county_name', 'risk_star5', 'risk_star45',
                'risk_star4', 'risk_star35', 'risk_star3', 'risk_star25', 'esrd_ab',
            ],
            encoding='latin1'
        )

        df = df[['ssa', 'risk_star5', 'risk_star45', 'risk_star4', 'risk_star35', 'risk_star3', 'risk_star25']]
        df = df.assign(
            aged_parta=pd.NA, aged_partb=pd.NA, risk_ab=pd.NA,
            risk_bonus5=pd.NA, risk_bonus35=pd.NA, risk_bonus0=pd.NA,
            year=year
        )
   
    elif year == 2015:
        df = pd.read_csv(
            file_path,
            skiprows=skip_rows,
            names=[
                'ssa', 'state', 'county_name', 'risk_bonus5', 'risk_bonus35',
                'risk_bonus0', 'esrd_ab'
            ],
            na_values='#N/A',
            encoding='latin1'
        )

        df = df[['ssa', 'risk_bonus5', 'risk_bonus35', 'risk_bonus0']]
        df = df.assign(
            risk_star5=pd.NA, risk_star45=pd.NA, risk_star4=pd.NA,
            risk_star35=pd.NA, risk_star3=pd.NA, risk_star25=pd.NA,
            aged_parta=pd.NA, aged_partb=pd.NA, risk_ab=pd.NA,
            year=year
        )

    benchmark_data.append(df)

benchmark_final = pd.concat(benchmark_data, ignore_index=True)
benchmark_final.to_csv('/Users/ilsenovis/ECON470HW4/data/output/ma_benchmark.csv', index=False)