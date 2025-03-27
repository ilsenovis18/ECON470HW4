import pandas as pd

## 2010 data
partc_file_2010 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2010PartCPlanLevel2.xlsx'
partd_file_2010 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2010PartDPlans2.xlsx'

columns_partc = [
    "contractid", "planid", "contract_name", "plan_type",
    "riskscore_partc", "payment_partc", "rebate_partc", "msa_deposit_partc"
]

columns_partd = [
    "contractid", "planid", "contract_name", "plan_type",
    "directsubsidy_partd", "riskscore_partd", "reinsurance_partd", "costsharing_partd"
]

risk_rebate_2010a = pd.read_excel(
    partc_file_2010,
    skiprows=3,
    names=columns_partc,
    header=None
)

risk_rebate_2010b = pd.read_excel(
    partd_file_2010,
    skiprows=3,
    names=columns_partd,
    header=None
)

## 2011 data
partc_file_2011 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2011PartCPlanLevel.xlsx'
partd_file_2011 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2011PartDPlans.xlsx'


columns_partc_2011 = [
    "contractid", "planid", "contract_name", "plan_type",
    "riskscore_partc", "payment_partc", "rebate_partc", "msa_deposit_partc"
]

columns_partd_2011 = [
    "contractid", "planid", "contract_name", "plan_type",
    "directsubsidy_partd", "riskscore_partd", "reinsurance_partd", "costsharing_partd"
]

risk_rebate_2011a = pd.read_excel(
    partc_file_2011,
    skiprows=3,
    names=columns_partc_2011,
    header=None
)

risk_rebate_2011b = pd.read_excel(
    partd_file_2011,
    skiprows=3,
    names=columns_partd_2011,
    header=None
)

## 2012 data
partc_file_2012 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2012PartCPlanLevel.xlsx'
partd_file_2012 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2012PartDPlans.xlsx'

columns_partc_2012 = [
    "contractid", "planid", "contract_name", "plan_type",
    "riskscore_partc", "payment_partc", "rebate_partc", "msa_deposit_partc"
]

columns_partd_2012 = [
    "contractid", "planid", "contract_name", "plan_type",
    "directsubsidy_partd", "riskscore_partd", "reinsurance_partd", "costsharing_partd"
]

risk_rebate_2012a = pd.read_excel(
    partc_file_2012,
    skiprows=3,
    names=columns_partc_2012,
    header=None
)

risk_rebate_2012b = pd.read_excel(
    partd_file_2012,
    skiprows=3,
    names=columns_partd_2012,
    header=None
)

## 2013 data
partc_file_2013 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2013PartCPlan Level.xlsx'
partd_file_2013 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2013PartDPlans.xlsx'

columns_partc_2013 = [
    "contractid", "planid", "contract_name", "plan_type",
    "riskscore_partc", "payment_partc", "rebate_partc"
]

columns_partd_2013 = [
    "contractid", "planid", "contract_name", "plan_type",
    "directsubsidy_partd", "riskscore_partd", "reinsurance_partd", "costsharing_partd"
]

risk_rebate_2013a = pd.read_excel(
    partc_file_2013,
    skiprows=3,
    names=columns_partc_2013,
    header=None
)

risk_rebate_2013b = pd.read_excel(
    partd_file_2013,
    skiprows=3,
    names=columns_partd_2013,
    header=None
)

## 2014 data
partc_file_2014 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2014PartCPlan Level.xlsx'
partd_file_2014 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2014PartDPlans.xlsx'

columns_partc_2014 = [
    "contractid", "planid", "contract_name", "plan_type",
    "riskscore_partc", "payment_partc", "rebate_partc"
]

columns_partd_2014 = [
    "contractid", "planid", "contract_name", "plan_type",
    "directsubsidy_partd", "riskscore_partd", "reinsurance_partd", "costsharing_partd"
]

risk_rebate_2014a = pd.read_excel(
    partc_file_2014,
    skiprows=3,
    names=columns_partc_2014,
    header=None
)

risk_rebate_2014b = pd.read_excel(
    partd_file_2014,
    skiprows=3,
    names=columns_partd_2014,
    header=None
)

## 2015 data
partc_file_2015 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2015PartCPlanLevel.xlsx'
partd_file_2015 = '/Users/ilsenovis/ECON470HW4/data/input/6_RiskRebates/2015PartDPlans.xlsx'

columns_partc_2015 = [
    "contractid", "planid", "contract_name", "plan_type",
    "riskscore_partc", "payment_partc", "rebate_partc"
]

columns_partd_2015 = [
    "contractid", "planid", "contract_name", "plan_type",
    "directsubsidy_partd", "riskscore_partd", "reinsurance_partd", "costsharing_partd"
]

risk_rebate_2015a = pd.read_excel(
    partc_file_2015,
    skiprows=3,
    names=columns_partc_2015,
    header=None
)

risk_rebate_2015b = pd.read_excel(
    partd_file_2015,
    skiprows=3,
    names=columns_partd_2015,
    header=None
)

risk_rebate_yearly = {}

for year in range (2010, 2016):
    partc = globals().get(f"risk_rebate_{year}a")
    partd = globals().get(f"risk_rebate_{year}b")

    if partc is None or partd is None:
        print(f"Missing data for {year}")
        continue

    partc = partc.copy()
    for col in ["riskscore_partc", "payment_partc", "rebate_partc"]:
        partc[col] = pd.to_numeric(partc[col].astype(str).str.replace(r"/$", "", regex=True), errors="coerce")

    partc["planid"] = pd.to_numeric(partc["planid"], errors="coerce")
    partc["year"] = year

    partc = partc[[
        "contractid", "planid", "contract_name", "plan_type",
        "riskscore_partc", "payment_partc", "rebate_partc", "year"
    ]]

    partd = partd.copy()
    partd['planid'] = pd.to_numeric(partd['planid'], errors='coerce')

    partd['payment_partd'] = (
        partd['directsubsidy_partd']
        + partd['reinsurance_partd']
        + partd['costsharing_partd']
    )

    partd = partd[[
        'contractid', 'planid', 'payment_partd',
        'directsubsidy_partd', 'reinsurance_partd', 
        'costsharing_partd', 'riskscore_partd'
    ]]

    merged = pd.merge(partc, partd, on=['contractid', 'planid'], how='left')
    risk_rebate_yearly[year] = merged

risk_rebate_final = pd.concat(risk_rebate_yearly.values(), ignore_index=True)

risk_rebate_final.to_csv('/Users/ilsenovis/ECON470HW4/data/output/risk_rebates.csv', index=False)

