import pandas as pd
import os

star_rating_path = "/Users/ilsenovis/ECON470HW4/data/input/5_StarRating"

## 2010 data

domain_file10 = os.path.join(star_rating_path, '/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2010_Part_C_Report_Card_Master_Table_2009_11_30_domain.csv')
summary_file10 = os.path.join(star_rating_path, '/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2010_Part_C_Report_Card_Master_Table_2009_11_30_summary.csv')

column_names_2010 = [
    "contractid", "org_type", "contract_name", "org_marketing",
    "breast_cancer_screening", "colorectal_cancer_screening", "cholesterol_screening",
    "glaucoma_testing", "medication_monitoring", "flu_vaccine", "pneumonia_vaccine",
    "physical_health", "mental_health", "osteoporosis_testing", "physical_activity_monitoring",
    "primary_care_visit", "osteoporosis_management", "diabetes_care", "blood_pressure_control",
    "arthritis_management", "copd_testing", "bladder_control", "fall_risk", "access_to_specialists",
    "doctor_communication", "appointments_care_quickly", "customer_service",
    "overall_quality_rating", "overall_plan_rating", "complaints_per_1000",
    "timely_appeals", "appeal_fairness", "members_leaving_plan", "audit_problems",
    "hold_time", "info_accuracy", "accessibility_services"
]

star_data_2010a = pd.read_csv(
    domain_file10, 
    skiprows=4, 
    names=column_names_2010,
    usecols=range(len(column_names_2010)),
    encoding='latin1')


# replace star rating
star_replacements10 = {"1 out of 5 stars": "1",
                     "2 out of 5 stars": "2",
                    "3 out of 5 stars": "3",
                    "4 out of 5 stars": "4",
                    "5 out of 5 stars": "5"
}

star_data_2010a = star_data_2010a.replace(star_replacements10)

cols_to_convert = star_data_2010a.columns.difference(["contractid", "org_type", "contract_name", "org_marketing"])
star_data_2010a[cols_to_convert] = star_data_2010a[cols_to_convert].apply(pd.to_numeric, errors='coerce')

star_data_2010b = pd.read_csv(
    summary_file10,
    skiprows=2,
    names=['contractid', 'org_type', 'contract_name', 'org_marketing', 'partc_score'],
    usecols=range(5),
    encoding='latin1'
)

# create new_contract
star_data_2010b['new_contract'] = star_data_2010b['partc_score'].apply(
    lambda x: 1 if x == "Plan too new to be measured" else 0
)

score_replacements10 = {
    "1 out of 5 stars": "1",
    "1.5 out of 5 stars": "1.5",
    "2 out of 5 stars": "2",
    "2.5 out of 5 stars": "2.5",
    "3 out of 5 stars": "3",
    "3.5 out of 5 stars": "3.5",
    "4 out of 5 stars": "4",
    "4.5 out of 5 stars": "4.5",
    "5 stars": "5"
}

star_data_2010b["partc_score"] = star_data_2010b["partc_score"].replace(score_replacements10)
star_data_2010b["partc_score"] = pd.to_numeric(star_data_2010b["partc_score"], errors="coerce")

# keep only needed columns
star_data_2010b = star_data_2010b[['contractid', 'new_contract', 'partc_score']]

# merge
star_data_2010 = pd.merge(star_data_2010a, star_data_2010b, on='contractid', how='left')

star_data_2010['year'] = 2010

## 2011 data
domain_file11 = os.path.join(star_rating_path, '/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2011_Part_C_Report_Card_Master_Table_2011_04_20_star.csv')
summary_file11 = os.path.join(star_rating_path, '/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2011_Part_C_Report_Card_Master_Table_2011_04_20_summary.csv')

column_names_2011 = [
    "contractid", "org_type", "contract_name", "org_marketing",
    "breast_cancer_screening", "colorectal_cancer_screening", "cvd_cholesterol_screening",
    "diabetes_cholesterol_screening", "glaucoma_testing", "medication_monitoring",
    "flu_vaccine", "pneumonia_vaccine", "physical_health", "mental_health",
    "osteoporosis_testing", "physical_activity_monitoring", "primary_care_access",
    "osteoporosis_management", "diabetes_eye_exam", "diabetes_kidney_monitoring",
    "diabetes_blood_sugar", "diabetes_cholesterol", "bp_control", "ra_management",
    "copd_testing", "bladder_control", "fall_risk", "care_without_delays",
    "doctor_communication", "quick_appointments", "customer_service",
    "overall_quality_rating", "overall_plan_rating", "complaints",
    "timely_appeals", "appeal_reviews", "corrective_action",
    "call_hold_time", "call_info_accuracy", "call_accessibility"
]

star_data_2011a = pd.read_csv(
    domain_file11,
    skiprows=5,
    names=column_names_2011,
    usecols=range(len(column_names_2011)),
    encoding='latin1'
)

# replace star rating
star_replacements11 = {"1 out of 5 stars": "1",
                     "2 out of 5 stars": "2",
                    "3 out of 5 stars": "3",
                    "4 out of 5 stars": "4",
                    "5 out of 5 stars": "5"
}

star_data_2011a = star_data_2011a.replace(star_replacements11)

star_columns11 = [col for col in star_data_2011a.columns if col not in ["contractid", "org_type", "contract_name", "org_marketing"]]
star_data_2011a[star_columns11] = star_data_2011a[star_columns11].apply(pd.to_numeric, errors='coerce')


star_data_2011b = pd.read_csv(
    summary_file11,
    skiprows=2,
    names=['contractid', 'org_type', 'contract_name', 'org_marketing',
           'partc_lowstar', 'partc_score', 'partcd_score'],
    usecols=range(7),
    encoding='latin1'
)

star_data_2011b['new_contract'] = star_data_2011b.apply(
    lambda row: 1 if row['partc_score'] == "Plan too new to be measured" or row['partcd_score'] == "Plan too new to be measured" else 0,
    axis=1
)

score_replacements11 = {
    "1 out of 5 stars": "1",
    "1.5 out of 5 stars": "1.5",
    "2 out of 5 stars": "2",
    "2.5 out of 5 stars": "2.5",
    "3 out of 5 stars": "3",
    "3.5 out of 5 stars": "3.5",
    "4 out of 5 stars": "4",
    "4.5 out of 5 stars": "4.5",
    "5 stars": "5"
}

star_data_2011b["partc_score"] = star_data_2011b["partc_score"].replace(score_replacements11)
star_data_2011b["partc_score"] = pd.to_numeric(star_data_2011b["partc_score"], errors="coerce")

star_data_2011b["partcd_score"] = star_data_2011b["partcd_score"].replace(score_replacements11)
star_data_2011b["partcd_score"] = pd.to_numeric(star_data_2011b["partcd_score"], errors="coerce")

star_data_2011b["low_score"] = star_data_2011b["partc_lowstar"].apply(lambda x: 1 if str(x).strip().lower() == "yes" else 0)

# keep only needed columns
star_data_2011b = star_data_2011b[['contractid', 'new_contract', 'low_score', 'partc_score', 'partcd_score']]

# merge
star_data_2011 = pd.merge(star_data_2011a, star_data_2011b, on='contractid', how='left')

star_data_2011['year'] = 2011

## 2012 data
domain_file12 = os.path.join(star_rating_path, '/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2012_Part_C_Report_Card_Master_Table_2011_11_01_Star.csv')
summary_file12 = os.path.join(star_rating_path, '/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2012_Part_C_Report_Card_Master_Table_2011_11_01_Summary.csv')

column_names_2012 = [
    "contractid", "org_type", "org_parent", "org_marketing",
    "breast_cancer_screening", "colorectal_cancer_screening", "cardio_cholesterol",
    "diabetes_cholesterol", "glaucoma_testing", "flu_vaccine", "pneumonia_vaccine",
    "physical_health", "mental_health", "physical_activity_monitoring", "primary_care_visits",
    "bmi_assessment", "medication_review", "functional_status", "pain_screening",
    "osteoporosis_management", "diabetes_eye_exam", "diabetes_kidney_monitoring",
    "diabetes_blood_sugar", "diabetes_cholesterol_control", "blood_pressure_control",
    "arthritis_management", "bladder_control", "fall_risk", "readmissions",
    "access_to_care", "appointments_care_quickly", "customer_service",
    "overall_quality_rating", "overall_plan_rating", "complaints",
    "access_performance_problems", "members_leaving_plan",
    "timely_appeals", "appeal_review", "callcenter_accessibility"
]

star_data_2012a = pd.read_csv(
    domain_file12,
    skiprows=5,
    names=column_names_2012,
    usecols=range(len(column_names_2012)),
    encoding='latin1'
)

cols_to_convert = star_data_2012a.columns.difference(["contractid", "org_type", "org_parent", "org_marketing"])
star_data_2012a[cols_to_convert] = star_data_2012a[cols_to_convert].apply(pd.to_numeric, errors="coerce")


star_data_2012b = pd.read_csv(
    summary_file12,
    skiprows=2,
    names=[
        "contractid", "org_type", "org_parent", "org_marketing",
        "partc_score", "partc_lowscore", "partc_highscore",
        "partcd_score", "partcd_lowscore", "partcd_highscore"],
    usecols=range(10),
    encoding='latin1'
)

star_data_2012b['new_contract'] = star_data_2012b.apply(
    lambda row: 1 if row['partc_score'] == "Plan too new to be measured" or row['partcd_score'] == "Plan too new to be measured" else 0,
    axis=1
)

star_data_2012b['low_score'] = star_data_2012b['partc_lowscore'].apply(
    lambda x: 1 if x == "Yes" else 0
)

score_replacements12 = {
    "1 out of 5 stars": "1",
    "1.5 out of 5 stars": "1.5",
    "2 out of 5 stars": "2",
    "2.5 out of 5 stars": "2.5",
    "3 out of 5 stars": "3",
    "3.5 out of 5 stars": "3.5",
    "4 out of 5 stars": "4",
    "4.5 out of 5 stars": "4.5",
    "5 stars": "5"
}

for col in ["partc_score", "partcd_score"]:
    star_data_2012b[col] = star_data_2012b[col].replace(score_replacements12)
    star_data_2012b[col] = pd.to_numeric(star_data_2012b[col], errors="coerce")

# keep only needed columns
star_data_2012b = star_data_2012b[['contractid', 'new_contract', 'low_score', 'partc_score', 'partcd_score']]

# merge
star_data_2012 = pd.merge(star_data_2012a, star_data_2012b, on='contractid', how='left')

star_data_2012['year'] = 2012

## 2013 data
domain_file13 = os.path.join(star_rating_path, '/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2013_Part_C_Report_Card_Master_Table_2012_10_17_Star.csv')
summary_file13 = os.path.join(star_rating_path, '/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2013_Part_C_Report_Card_Master_Table_2012_10_17_Summary.csv')

column_names_2013 = [
    "contractid", "org_type", "contract_name", "org_marketing", "org_parent",
    "breast_cancer_screening", "colorectal_cancer_screening", "cardio_cholesterol",
    "diabetes_cholesterol", "glaucoma_testing", "flu_vaccine",
    "physical_health", "mental_health", "physical_activity_monitoring", "bmi_assessment",
    "medication_review", "functional_status", "pain_screening", "osteoporosis_management",
    "diabetes_eye_exam", "diabetes_kidney_monitoring", "diabetes_blood_sugar",
    "diabetes_cholesterol_control", "bp_control", "arthritis_management",
    "bladder_control", "fall_risk", "readmissions", "access_to_care",
    "appointments_care_quickly", "customer_service", "overall_quality_rating",
    "overall_plan_rating", "care_coordination", "complaints",
    "access_performance_problems", "members_leaving_plan", "improvement",
    "timely_appeals", "appeal_review", "callcenter_accessibility", "enrollment_timeliness"
]

star_data_2013a = pd.read_csv(
    domain_file13,
    skiprows=4,
    names=column_names_2013,
    usecols=range(len(column_names_2013)),
    encoding="latin1"
)

cols_to_convert = star_data_2013a.columns.difference(["contractid", "org_type", "contract_name", "org_marketing", "org_parent"])
star_data_2013a[cols_to_convert] = star_data_2013a[cols_to_convert].apply(pd.to_numeric, errors="coerce")


star_data_2013b = pd.read_csv(
    summary_file13,
    skiprows=2,
    names=[
        "contractid", "org_type", "org_marketing", "contract_name", "org_parent",
        "partc_score", "partc_lowscore", "partc_highscore",
        "partcd_score", "partcd_lowscore", "partcd_highscore"],
    usecols=range(11),
    encoding="latin1"
)

star_data_2013b["new_contract"] = star_data_2013b.apply(
    lambda x: 1 if x["partc_score"] == "Plan too new to be measured" or x["partcd_score"] == "Plan too new to be measured" else 0,
    axis=1
)

star_data_2013b["partc_score"] = pd.to_numeric(star_data_2013b["partc_score"], errors="coerce")
star_data_2013b["partcd_score"] = pd.to_numeric(star_data_2013b["partcd_score"], errors="coerce")
star_data_2013b["low_score"] = star_data_2013b["partc_lowscore"].apply(lambda x: 1 if str(x).strip().lower() == "yes" else 0)

star_data_2013b = star_data_2013b[["contractid", "new_contract", "low_score", "partc_score", "partcd_score"]]

star_data_2013 = pd.merge(star_data_2013a, star_data_2013b, on="contractid", how="left")
star_data_2013["year"] = 2013

## 2014
domain_file14 = os.path.join(star_rating_path, '/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2014_Part_C_Report_Card_Master_Table_2013_10_17_stars.csv')
summary_file14 = os.path.join(star_rating_path, '/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2014_Part_C_Report_Card_Master_Table_2013_10_17_summary.csv')

column_names_2014 = [
    "contractid", "org_type", "contract_name", "org_marketing", "org_parent",
    "breast_cancer_screening", "colorectal_cancer_screening", "cvd_cholesterol_screening",
    "diabetes_cholesterol_screening", "glaucoma_testing", "flu_vaccine",
    "physical_health", "mental_health", "physical_activity_monitoring", "bmi_assessment",
    "medication_review", "functional_status", "pain_screening", "osteoporosis_management",
    "diabetes_eye_exam", "diabetes_kidney_monitoring", "diabetes_blood_sugar",
    "diabetes_cholesterol_control", "blood_pressure_control", "arthritis_management",
    "bladder_control", "fall_risk", "readmissions", "access_to_care",
    "appointments_care_quickly", "customer_service", "quality_rating",
    "plan_rating", "care_coordination", "complaints", "access_perf_issues",
    "leaving_plan", "quality_improvement", "timely_appeals", "appeal_review",
    "call_accessibility"]

star_data_2014a = pd.read_csv(
    domain_file14,
    skiprows=3,
    names=column_names_2014,
    usecols=range(len(column_names_2014)),
    encoding='latin1')

cols_to_convert = star_data_2014a.columns.difference(["contractid", "org_type", "contract_name", "org_marketing", "org_parent"])
star_data_2014a[cols_to_convert] = star_data_2014a[cols_to_convert].apply(pd.to_numeric, errors="coerce")

star_data_2014b = pd.read_csv(
    summary_file14,
    skiprows=2,
    usecols=range(9),
    names=["contractid", "org_type", "org_marketing", "contract_name", "org_parent", "snp", "sanction", "partc_score", "partcd_score"],
    encoding='latin1'
)

star_data_2014b["new_contract"] = star_data_2014b.apply(
    lambda row: 1 if row["partc_score"] == "Plan too new to be measured" or row["partcd_score"] == "Plan too new to be measured" else 0,
    axis=1
)

for col in ["partc_score", "partcd_score"]:
    star_data_2014b[col] = pd.to_numeric(star_data_2014b[col], errors="coerce")

star_data_2014b = star_data_2014b[["contractid", "new_contract", "partc_score", "partcd_score"]]

star_data_2014 = pd.merge(star_data_2014a, star_data_2014b, on="contractid", how="left")

star_data_2014["year"] = 2014

## 2015

domain_file15 = os.path.join(star_rating_path, "/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2015_Report_Card_Master_Table_2014_10_03_stars.csv")
summary_file15 = os.path.join(star_rating_path, "/Users/ilsenovis/ECON470HW4/data/input/5_StarRating/2015_Report_Card_Master_Table_2014_10_03_summary.csv")

column_names_2015 = [
    "contractid", "org_type", "contract_name", "org_marketing", "org_parent",
    "colorectal_cancer_screening", "cvd_cholesterol_screening", "diabetes_cholesterol_screening",
    "flu_vaccine", "physical_health", "mental_health", "physical_activity_monitoring",
    "bmi_assessment", "snp_care_management", "medication_review", "functional_status",
    "pain_assessment", "osteoporosis_management", "diabetes_eye_exam", "diabetes_kidney_monitoring",
    "diabetes_blood_sugar", "diabetes_cholesterol_control", "blood_pressure_control",
    "arthritis_management", "bladder_control", "fall_risk", "readmissions",
    "access_to_care", "appointments_care_quickly", "customer_service",
    "quality_rating", "plan_rating", "care_coordination", "complaints",
    "leaving_plan", "quality_improvement", "timely_appeals", "appeal_review"]

star_data_2015a = pd.read_csv(domain_file15,
    skiprows=4,
    usecols=range(len(column_names_2015)),
    names=column_names_2015,
    encoding='latin1')

cols_to_convert = star_data_2015a.columns.difference(["contractid", "org_type", "contract_name", "org_marketing", "org_parent"])
star_data_2015a[cols_to_convert] = star_data_2015a[cols_to_convert].apply(pd.to_numeric, errors="coerce")

star_data_2015b = pd.read_csv(
    summary_file15,
    skiprows=2,
    usecols=range(10),
    names=[
        "contractid", "org_type", "org_marketing", "contract_name", "org_parent",
        "snp", "sanction", "partc_score", "partdscore", "partcd_score"
    ],
    encoding='latin1'
)

star_data_2015b["new_contract"] = star_data_2015b.apply(
    lambda row: 1 if row["partc_score"] == "Plan too new to be measured" or row["partcd_score"] == "Plan too new to be measured" else 0,
    axis=1
)

star_data_2015b["partc_score"] = pd.to_numeric(star_data_2015b["partc_score"], errors="coerce")
star_data_2015b["partcd_score"] = pd.to_numeric(star_data_2015b["partcd_score"], errors="coerce")

star_data_2015b = star_data_2015b[["contractid", "new_contract", "partc_score", "partcd_score"]]

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