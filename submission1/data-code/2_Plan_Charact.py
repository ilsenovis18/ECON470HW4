import pandas as pd
import pyreadr

def clean_currency_columns(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r'[\$,]', '', regex=True)
                .str.strip()
                .replace(['-', ' - ', ' -   ', '', 'N/A'], pd.NA)
                .astype(pd.Float64Dtype())
            )

## 2010 data
ma_2010_a = pd.read_csv(
    "/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2010LandscapeSourceData_MA_12_01_09_A_to_M.csv",
    skiprows=5,
    encoding='latin1',
    names=["state","county","org_name","plan_name","plan_type","premium","partd_deductible",
           "drug_type","gap_coverage","drug_type_detail","demo_type","contractid",
           "planid","segmentid","moop"],
    dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "premium": str,
        "partd_deductible": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "demo_type": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str
    }
)


ma_2010_b = pd.read_csv(
    "/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2010LandscapeSourceData_MA_12_01_09_N_to_W.csv",
    skiprows=5,
    encoding='latin1',
    names=["state","county","org_name","plan_name","plan_type","premium","partd_deductible",
           "drug_type","gap_coverage","drug_type_detail","demo_type","contractid",
           "planid","segmentid","moop"],
    dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "premium": str,
        "partd_deductible": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "demo_type": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str
    }
)

macd_2010_a = pd.read_excel(
    "/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2010 Plan Report 09-14-09.xls",
    sheet_name="Alabama to Montana",
    skiprows=4,
    nrows=26368,
    names=["state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
           "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
           "national_pdp", "partd_rein_demo", "partd_rein_demo_type", "premium_partc",
           "premium_partd_basic", "premium_partd_supp", "premium_partd_total",
           "partd_assist_full", "partd_assist_75", "partd_assist_50", "partd_assist_25",
           "partd_deductible", "deductible_exclusions", "increase_coverage_limit",
           "gap_coverage", "gap_coverage_type"],
    dtype=str
)

macd_2010_b = pd.read_excel(
    "/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2010 Plan Report 09-14-09.xls",
    sheet_name="Nebraska to Wyoming",
    skiprows=4,
    nrows=31069,
    names=["state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
           "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
           "national_pdp", "partd_rein_demo", "partd_rein_demo_type", "premium_partc",
           "premium_partd_basic", "premium_partd_supp", "premium_partd_total",
           "partd_assist_full", "partd_assist_75", "partd_assist_50", "partd_assist_25",
           "partd_deductible", "deductible_exclusions", "increase_coverage_limit",
           "gap_coverage", "gap_coverage_type"],
    dtype=str
)

## 2011 data
ma_2011_a = pd.read_csv(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2011LandscapeSourceData_MA_12_17_10_AtoM.csv',
    skiprows=6,
    encoding='latin1',
    names=["state","county","org_name","plan_name","plan_type","premium","partd_deductible",
           "drug_type","gap_coverage","drug_type_detail","contractid",
           "planid","segmentid","moop", "free_preventative_care"],
    dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "premium": str,
        "partd_deductible": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str,
        "free_preventive_care": str
    }
)


ma_2011_b = pd.read_csv(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2011LandscapeSourceData_MA_12_17_10_NtoW.csv',
    skiprows=6,
    encoding='latin1',
    names=["state","county","org_name","plan_name","plan_type","premium", "partd_deductible",
           "drug_type","gap_coverage","drug_type_detail","contractid",
           "planid","segmentid","moop", "free_preventative_care"],
   dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "premium": str,
        "partd_deductible": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str,
        "free_preventive_care": str
    }
)

macd_2011_a = pd.read_excel(
    "/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2011 Plan Report 09-15-10.xls",
    sheet_name="Alabama to Montana",
    skiprows=4,
    nrows=18101,
    names=[
        "state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
        "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
        "national_pdp", "premium_partc", "premium_partd_basic", "premium_partd_supp",
        "premium_partd_total", "partd_assist_full", "partd_assist_75", "partd_assist_50",
        "partd_assist_25", "partd_deductible", "deductible_exclusions",
        "increase_coverage_limit", "gap_coverage", "gap_coverage_type"
    ],
    dtype=str
)

macd_2011_b = pd.read_excel(
    "/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2011 Plan Report 09-15-10.xls",
    sheet_name="Nebraska to Wyoming",
    skiprows=4,
    nrows=22998,
    names=[
        "state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
        "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
        "national_pdp", "premium_partc", "premium_partd_basic", "premium_partd_supp",
        "premium_partd_total", "partd_assist_full", "partd_assist_75", "partd_assist_50",
        "partd_assist_25", "partd_deductible", "deductible_exclusions",
        "increase_coverage_limit", "gap_coverage", "gap_coverage_type"
    ],
    dtype=str
)

## 2012 data
ma_2012_a = pd.read_csv(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2012LandscapeSourceData_MA_3_08_12_AtoM.csv',
    skiprows=6,
    encoding='latin1',
    names=['state', 'county', 'org_name', 'plan_name', 'plan_type', 'premium', 'partd_deductible',
           'drug_type', 'gap_coverage', 'drug_type_detail', 'contractid',
           'planid', 'segmentid', 'moop', 'star_rating'],
    dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "premium": str,
        "partd_deductible": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str,
        "star_rating": str
    }
)

ma_2012_b = pd.read_csv(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2012LandscapeSourceData_MA_3_08_12_NtoW.csv',
    skiprows=6,
    encoding='latin1',
    names=["state", "county", "org_name", "plan_name", "plan_type", "premium", "partd_deductible",
        "drug_type", "gap_coverage", "drug_type_detail", "contractid",
        "planid", "segmentid", "moop", "star_rating"],
    dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "premium": str,
        "partd_deductible": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str,
        "star_rating": str
    }
)


macd_2012_a = pd.read_excel(
    "/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2012 Plan Report 09-08-11.xls",
    sheet_name="Alabama to Montana",
    skiprows=4,
    nrows=18517,
    names=[
        "state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
        "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
        "national_pdp", "premium_partc",
        "premium_partd_basic", "premium_partd_supp", "premium_partd_total",
        "pard_assist_full", "partd_assist_75", "partd_assist_50", "partd_assist_25",
        "partd_deductible", "deductible_exclusions", "increase_coverage_limit",
        "gap_coverage", "gap_coverage_type"],
    dtype=str
)

macd_2012_b = pd.read_excel(
    "/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2012 Plan Report 09-08-11.xls",
    sheet_name="Nebraska to Wyoming",
    skiprows=4,
    nrows=26678,
    names=[
        "state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
        "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
        "national_pdp", "premium_partc",
        "premium_partd_basic", "premium_partd_supp", "premium_partd_total",
        "pard_assist_full", "partd_assist_75", "partd_assist_50", "partd_assist_25",
        "partd_deductible", "deductible_exclusions", "increase_coverage_limit",
        "gap_coverage", "gap_coverage_type"],
    dtype=str
)

## 2013 data
ma_2013_a = pd.read_csv(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2013LandscapeSource file MA_AtoM 11212012.csv',
    skiprows=6,
    encoding='latin1',
    names=[
        "state", "county", "org_name", "plan_name", "plan_type", "premium", "partd_deductible",
        "drug_type", "gap_coverage", "drug_type_detail", "contractid",
        "planid", "segmentid", "moop", "star_rating"
    ],
    dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "premium": str,
        "partd_deductible": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str,
        "star_rating": str
    }
)


ma_2013_b = pd.read_csv(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2013LandscapeSource file MA_NtoW 11212012.csv',
    skiprows=6,
    encoding='latin1',
    names=[
        "state", "county", "org_name", "plan_name", "plan_type", "premium", "partd_deductible",
        "drug_type", "gap_coverage", "drug_type_detail", "contractid",
        "planid", "segmentid", "moop", "star_rating"
    ],
    dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "premium": str,
        "partd_deductible": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str,
        "star_rating": str
    }
)

macd_2013_a = pd.read_excel(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2013 Plan Report 04252013v1.xls',
    sheet_name="Alabama to Montana",
    skiprows=4,
    nrows=20936,
    names=[
        "state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
        "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
        "national_pdp", "premium_partc",
        "premium_partd_basic", "premium_partd_supp", "premium_partd_total",
        "pard_assist_full", "partd_assist_75", "partd_assist_50", "partd_assist_25",
        "partd_deductible", "deductible_exclusions", "increase_coverage_limit",
        "gap_coverage", "gap_coverage_type"],
    dtype=str
)

macd_2013_b = pd.read_excel(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2013 Plan Report 04252013v1.xls',
    sheet_name="Nebraska to Wyoming",
    skiprows=4,
    nrows=2872,
    names=[
        "state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
        "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
        "national_pdp", "premium_partc",
        "premium_partd_basic", "premium_partd_supp", "premium_partd_total",
        "pard_assist_full", "partd_assist_75", "partd_assist_50", "partd_assist_25",
        "partd_deductible", "deductible_exclusions", "increase_coverage_limit",
        "gap_coverage", "gap_coverage_type"],
    dtype=str
)

# 2014 data
ma_2014_a = pd.read_csv(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2014LandscapeSource file MA_AtoM 05292014.csv',
    skiprows=6,
    encoding='latin1',
   names=[
        "state", "county", "org_name", "plan_name", "plan_type", "premium", "partd_deductible",
        "drug_type", "gap_coverage", "drug_type_detail", "contractid",
        "planid", "segmentid", "moop", "star_rating"],
    dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str,
        "star_rating": str
    }
)

ma_2014_b = pd.read_csv(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2014LandscapeSource file MA_NtoW 05292014.csv',
    skiprows=6,
    encoding='latin1',
    names=[
        "state", "county", "org_name", "plan_name", "plan_type", "premium", "partd_deductible",
        "drug_type", "gap_coverage", "drug_type_detail", "contractid",
        "planid", "segmentid", "moop", "star_rating"
    ],
    dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "premium": str,
        "partd_deductible": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str,
        "star_rating": str
    }
)


macd_2014_a = pd.read_excel(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2014 Plan Report 05292014.xls',
    sheet_name="Alabama to Montana",
    skiprows=4,
    nrows=15855,
    names=[
        "state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
        "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
        "national_pdp", "premium_partc",
        "premium_partd_basic", "premium_partd_supp", "premium_partd_total",
        "pard_assist_full", "partd_assist_75", "partd_assist_50", "partd_assist_25",
        "partd_deductible", "deductible_exclusions", "increase_coverage_limit",
        "gap_coverage", "gap_coverage_type"],
    dtype=str
)

macd_2014_b = pd.read_excel(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2014 Plan Report 05292014.xls',
    sheet_name="Nebraska to Wyoming",
    skiprows=4,
    nrows=20301,
    names=[
        "state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
        "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
        "national_pdp", "premium_partc",
        "premium_partd_basic", "premium_partd_supp", "premium_partd_total",
        "pard_assist_full", "partd_assist_75", "partd_assist_50", "partd_assist_25",
        "partd_deductible", "deductible_exclusions", "increase_coverage_limit",
        "gap_coverage", "gap_coverage_type"],
    dtype=str         
)


# 2015 data
ma_2015_a = pd.read_csv(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2015LandscapeSource file MA_AtoM 11042014.csv',
    skiprows=6,
    encoding='latin1',
    names=[
        "state", "county", "org_name", "plan_name", "plan_type", "premium", "partd_deductible",
        "drug_type", "gap_coverage", "drug_type_detail", "contractid",
        "planid", "segmentid", "moop", "star_rating"
    ],
    dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "premium": str,
        "partd_deductible": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str,
        "star_rating": str
    }
)


ma_2015_b = pd.read_csv(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/2015LandscapeSource file MA_NtoW 11042014.csv',
    skiprows=6,
    encoding='latin1',
    names=[
        "state", "county", "org_name", "plan_name", "plan_type", "premium", "partd_deductible",
        "drug_type", "gap_coverage", "drug_type_detail", "contractid",
        "planid", "segmentid", "moop", "star_rating"
    ],
    dtype={
        "state": str,
        "county": str,
        "org_name": str,
        "plan_name": str,
        "plan_type": str,
        "premium": str,
        "partd_deductible": str,
        "drug_type": str,
        "gap_coverage": str,
        "drug_type_detail": str,
        "contractid": str,
        "planid": float,
        "segmentid": float,
        "moop": str,
        "star_rating": str
    }
)


macd_2015_a = pd.read_excel(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2015 Plan Report 03182015.xls',
    sheet_name="Alabama to Montana",
    skiprows=4,
    nrows=16662,
    names=[
        "state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
        "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
        "national_pdp", "premium_partc",
        "premium_partd_basic", "premium_partd_supp", "premium_partd_total",
        "pard_assist_full", "partd_assist_75", "partd_assist_50", "partd_assist_25",
        "partd_deductible", "deductible_exclusions", "increase_coverage_limit",
        "gap_coverage"],
    dtype=str
)

macd_2015_b = pd.read_excel(
    '/Users/ilsenovis/ECON470HW4/data/input/2_PlanCharacter/Medicare Part D 2015 Plan Report 03182015.xls',
    sheet_name="Nebraska to Wyoming",
    skiprows=4,
    nrows=17034,
    names=[
        "state", "county", "org_name", "plan_name", "contractid", "planid", "segmentid",
        "org_type", "plan_type", "snp", "snp_type", "benefit_type", "below_benchmark",
        "national_pdp", "premium_partc",
        "premium_partd_basic", "premium_partd_supp", "premium_partd_total",
        "pard_assist_full", "partd_assist_75", "partd_assist_50", "partd_assist_25",
        "partd_deductible", "deductible_exclusions", "increase_coverage_limit",
        "gap_coverage"],
    dtype=str      
)

ma_currency_cols = ['premium', 'partd_deductible']

for df in [ma_2010_a, ma_2010_b, ma_2011_a, ma_2011_b, ma_2012_a, ma_2012_b,
           ma_2013_a, ma_2013_b, ma_2014_a, ma_2014_b, ma_2015_a, ma_2015_b]:
    clean_currency_columns(df, ma_currency_cols)

macd_currency_cols = ['premium_partc', 'premium_partd_basic', 'premium_partd_supp', 'premium_partd_total']

for df in [macd_2010_a, macd_2010_b, macd_2011_a, macd_2011_b, macd_2012_a, macd_2012_b,
           macd_2013_a, macd_2013_b, macd_2014_a, macd_2014_b, macd_2015_a, macd_2015_b]:
    clean_currency_columns(df, macd_currency_cols)

ma_2010 = pd.concat([ma_2010_a, ma_2010_b], ignore_index=True)
ma_2011 = pd.concat([ma_2011_a, ma_2011_b], ignore_index=True)
ma_2012 = pd.concat([ma_2012_a, ma_2012_b], ignore_index=True)
ma_2013 = pd.concat([ma_2013_a, ma_2013_b], ignore_index=True)
ma_2014 = pd.concat([ma_2014_a, ma_2014_b], ignore_index=True)
ma_2015 = pd.concat([ma_2015_a, ma_2015_b], ignore_index=True)

macd_2010 = pd.concat([macd_2010_a, macd_2010_b], ignore_index=True)
macd_2011 = pd.concat([macd_2011_a, macd_2011_b], ignore_index=True)
macd_2012 = pd.concat([macd_2012_a, macd_2012_b], ignore_index=True)
macd_2013 = pd.concat([macd_2013_a, macd_2013_b], ignore_index=True)
macd_2014 = pd.concat([macd_2014_a, macd_2014_b], ignore_index=True)
macd_2015 = pd.concat([macd_2015_a, macd_2015_b], ignore_index=True)

years = list(range(2010, 2016))
ma_data_dict = {
    2010: ma_2010,
    2011: ma_2011,
    2012: ma_2012,
    2013: ma_2013,
    2014: ma_2014,
    2015: ma_2015
}

macd_data_dict = {
    2010: macd_2010,
    2011: macd_2011,
    2012: macd_2012,
    2013: macd_2013,
    2014: macd_2014,
    2015: macd_2015
}

# CLEAN MA DATA ONLY
def clean_and_merged_ma_macd(year, ma_df, macd_df):
    ma_clean = ma_df[['contractid', 'planid', 'state', 'county', 'premium']].copy()
    ma_clean['planid'] = ma_clean['planid'].astype('Int64')
    ma_clean = (
        ma_clean
        .sort_values(['contractid', 'planid', 'state', 'county'])
        .drop_duplicates(['contractid', 'planid', 'state', 'county'])
    )

    macd_clean = macd_df[[
        'contractid', 'planid', 'state', 'county',
        'premium_partc', 'premium_partd_basic', 'premium_partd_supp',
        'premium_partd_total', 'partd_deductible'
    ]].copy()

    macd_clean['planid'] = pd.to_numeric(macd_clean['planid'], errors='coerce').astype('Int64')
    macd_clean = (
        macd_clean
        .sort_values(['contractid', 'planid', 'state', 'county'])
        .drop_duplicates(['contractid', 'planid', 'state', 'county'])
    )

    merged = pd.merge(ma_clean, macd_clean, how='outer',
                      on=['contractid', 'planid', 'state', 'county'])
    merged['year'] = year
    return merged

all_years_data = []

for year in years:
    ma_df = ma_data_dict[year]
    macd_df = macd_data_dict[year]
    combined_df = clean_and_merged_ma_macd(year, ma_df, macd_df)
    all_years_data.append(combined_df)

plan_premiums = pd.concat(all_years_data, ignore_index=True)

plan_premiums.to_csv("/Users/ilsenovis/ECON470HW4/data/output/plan_premiums.csv", index=False)