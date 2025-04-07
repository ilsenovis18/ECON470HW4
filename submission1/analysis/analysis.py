import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import statsmodels.formula.api as smf


final = pd.read_csv('/Users/ilsenovis/ECON470HW4/data/output/_final_ma_data.csv')

### Summarize the Data

## Question 1: Remove all SNPs, 800-series plans, and prescription drug only plans (i.e., plans that do not offer Part C benefits)
# filter out SNPs, 800, drug only
filtered = final[
                 (final['snp'] == 'No') &
                 ((final['planid'] < 800) | (final['planid'] >= 900)) &
                 (final['partc_score'].notna())
                 ]
 
 # Count plans per county per year
plan_counts = (
    filtered.groupby(["year", "fips"])
    .size()
    .reset_index(name="plan_count")
)

# Boxplot
plt.figure(figsize=(10, 6))
sns.boxplot(data=plan_counts, x="year", y="plan_count")
plt.title("Distribution of Plan Counts per County by Year")
plt.xlabel("Year")
plt.ylabel("Plan Count")
plt.grid(True)
plt.tight_layout()

# Save the figure
output_path = '/Users/ilsenovis/ECON470HW4/submission1/analysis/results1_plan_count_boxplot.png'
plt.savefig(output_path, bbox_inches='tight')

## Question 2: Provide bar graphs showing the distribution of star ratings in 2010, 2012, and 2015. How has this distribution changed over time?
# bar graph for star ratings
filtered['Star_Rating'] = pd.to_numeric(filtered['Star_Rating'], errors='coerce')
years = [2010, 2012, 2015]

for year in years:
    yearly_data = filtered[filtered['year'] == year]

    plt.figure(figsize=(8, 5))
    sns.countplot(
        data=yearly_data,
        x='Star_Rating',
        palette='crest',
        order=sorted(yearly_data['Star_Rating'].dropna().unique())
    )
    plt.title(f"Distribution of Star Ratings in {year}")
    plt.xlabel("Star Rating")
    plt.ylabel("Number of Plans")
    plt.xticks(rotation=0)
    plt.tight_layout()

    # Save each year's figure individually
    output_path = f'/Users/ilsenovis/ECON470HW4/submission1/analysis/results2_star_ratings_{year}.png'
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()

 ## Question 3: Plot the average benchmark payment over time from 2010 through 2015. How much has the average benchmark payment risen over the years?
# Ensure ma_rate is numeric
final["ma_rate"] = pd.to_numeric(final["ma_rate"], errors="coerce")

# Filter valid years and benchmark data
benchmark_trend = (
    final[(final['year'] >= 2010) & (final['year'] <= 2015) & (final['ma_rate'].notna())]
    .groupby("year", as_index=False)["ma_rate"]
    .mean()
)

# Plot average benchmark payment by year
plt.figure(figsize=(8, 5))
sns.lineplot(data=benchmark_trend, x="year", y="ma_rate", marker="o")
plt.title("Average Benchmark Payment (2010–2015)")
plt.xlabel("Year")
plt.ylabel("Average Benchmark Payment ($)")
plt.grid(True)
plt.tight_layout()

# Save the figure
output_path = '/Users/ilsenovis/ECON470HW4/submission1/analysis/results3_benchmark_trend.png'
plt.savefig(output_path, bbox_inches='tight')
plt.close()

print("Average Benchmark Payment by Year:")
print(benchmark_trend)

growth = benchmark_trend['ma_rate'].iloc[-1] - benchmark_trend['ma_rate'].iloc[0]
print(f"\nIncrease from 2010 to 2015: ${growth:.2f}")

## Questin 4: Plot the average share of Medicare Advantage (relative to all Medicare eligibles) over time from 2010 through 2015. Has Medicare Advantage increased or decreased in popularity? How does this share correlate with benchmark payments?
# Convert to numeric just in case
final["enrolled_mean"] = pd.to_numeric(final["enrolled_mean"], errors="coerce")
final["eligibles_mean"] = pd.to_numeric(final["eligibles_mean"], errors="coerce")

# Calculate Medicare Advantage (MA) share per plan
final["ma_share"] = final["enrolled_mean"] / final["eligibles_mean"]

# Group by year to calculate average MA share across counties/plans
ma_share_trend = (
    final[(final["year"] >= 2010) & (final["year"] <= 2015) & (final["ma_share"].notna())]
    .groupby("year", as_index=False)["ma_share"]
    .mean()
)

# Merge MA share trend and benchmark trend
merged_trend = pd.merge(ma_share_trend, benchmark_trend, on="year")

# Create figure and primary y-axis
fig, ax1 = plt.subplots(figsize=(10, 6))

# Plot MA share on primary axis
color1 = "tab:blue"
ax1.set_xlabel("Year")
ax1.set_ylabel("MA Share", color=color1)
ax1.plot(merged_trend["year"], merged_trend["ma_share"], marker="o", color=color1, label="MA Share")
ax1.tick_params(axis="y", labelcolor=color1)

# Create secondary y-axis for benchmark payment
ax2 = ax1.twinx()
color2 = "tab:green"
ax2.set_ylabel("Benchmark Payment ($)", color=color2)
ax2.plot(merged_trend["year"], merged_trend["ma_rate"], marker="o", linestyle="--", color=color2, label="Benchmark Payment")
ax2.tick_params(axis="y", labelcolor=color2)

# Titles and layout
plt.title("MA Share and Benchmark Payments (2010–2015)")
fig.tight_layout()

# Save plot
output_path = '/Users/ilsenovis/ECON470HW4/submission1/analysis/results4_ma_share_vs_benchmark.png'
plt.savefig(output_path, bbox_inches='tight')
plt.close()

# Correlation
correlation = merged_trend["ma_share"].corr(merged_trend["ma_rate"])
print(f"\nCorrelation between MA share and benchmark payment: {correlation:.3f}")


### Estimate ATEs
## Question 5: Calculate the running variable underlying the star rating. Provide a table showing the number of plans that are rounded up into a 3-star, 3.5-star, 4-star, 4.5-star, and 5-star rating.
# Filter for 2010 only
rdd_data = final[final['year'] == 2010].copy()

# Ensure numeric type for scores
rdd_data['partc_score'] = pd.to_numeric(rdd_data['partc_score'], errors='coerce')
rdd_data['partcd_score'] = pd.to_numeric(rdd_data['partcd_score'], errors='coerce')

# Running variable logic (score used to assign star rating)
rdd_data['running_score'] = np.where(
    rdd_data['partd'] == 'No',
    rdd_data['partc_score'],
    rdd_data['partcd_score']
)

# Define custom function to round to nearest 0.5
def round_half(x):
    if pd.isna(x):
        return np.nan
    return round(x * 2) / 2

# Star rating rounding (mimicking CMS rounding logic)
rdd_data['rounded_star'] = rdd_data['running_score'].round(1)
rdd_data['official_star'] = rdd_data['running_score'].apply(round_half)

# Count how many plans are in each rounded bucket
rating_counts = (
    rdd_data['official_star']
    .value_counts()
    .sort_index()
    .reset_index()
    .rename(columns={'index': 'Star_Rating', 'official_star': 'Plan_Count'})
)

print(rating_counts)

## Question 6: Using the RD estimator with a bandwidth of 0.125, provide an estimate of the effect of receiving a 3-star versus a 2.5 star rating on enrollments. Repeat the exercise to estimate the effects at 3.5 stars, and summarize your results in a table.
# Ensure numeric
final['partc_score'] = pd.to_numeric(final['partc_score'], errors='coerce')
final['partcd_score'] = pd.to_numeric(final['partcd_score'], errors='coerce')
final['enrolled_mean'] = pd.to_numeric(final['enrolled_mean'], errors='coerce')

# Filter to 2010 with valid scores and enrollment
rdd_data['running_score'] = np.where(
    rdd_data['partd'] == 'No',
    rdd_data['partc_score'],
    rdd_data['partcd_score']
)
rdd_data = rdd_data[rdd_data['running_score'].notna() & rdd_data['enrolled_mean'].notna()]

# RD estimation function
def estimate_rd(data, cutoff, bandwidth):
    window = data[np.abs(data['running_score'] - cutoff) <= bandwidth].copy()
    window['above'] = (window['running_score'] >= cutoff).astype(int)
    window['score_centered'] = window['running_score'] - cutoff

    model = smf.ols("enrolled_mean ~ above + score_centered + above:score_centered", data=window).fit()
    return {
        'Cutoff': cutoff,
        'Bandwidth': bandwidth,
        'ATE': model.params['above'],
        'Std_Error': model.bse['above'],
        'N': len(window)
    }

# Run for 3.0 and 3.5 cutoffs
results = [estimate_rd(rdd_data, cutoff=3.0, bandwidth=0.125),
           estimate_rd(rdd_data, cutoff=3.5, bandwidth=0.125)]

# Display
results_df = pd.DataFrame(results)
print(results_df)

## Question 7: Repeat your results for bandwidhts of 0.1, 0.12, 0.13, 0.14, and 0.15 (again for 3 and 3.5 stars). Show all of the results in a graph. How sensitive are your findings to the choice of bandwidth?
# Prep data
final['partc_score'] = pd.to_numeric(final['partc_score'], errors='coerce')
final['partcd_score'] = pd.to_numeric(final['partcd_score'], errors='coerce')
final['enrolled_mean'] = pd.to_numeric(final['enrolled_mean'], errors='coerce')

rdd_data['running_score'] = np.where(
    rdd_data['partd'] == 'No',
    rdd_data['partc_score'],
    rdd_data['partcd_score']
)
rdd_data = rdd_data[rdd_data['running_score'].notna() & rdd_data['enrolled_mean'].notna()]

# Function to estimate ATE at given cutoff and bandwidth
def estimate_rd(data, cutoff, bandwidth):
    subset = data[np.abs(data['running_score'] - cutoff) <= bandwidth].copy()
    subset['above'] = (subset['running_score'] >= cutoff).astype(int)
    subset['score_centered'] = subset['running_score'] - cutoff

    model = smf.ols("enrolled_mean ~ above + score_centered + above:score_centered", data=subset).fit()

    return {
        'Cutoff': cutoff,
        'Bandwidth': bandwidth,
        'ATE': model.params.get('above', np.nan),
        'Std_Error': model.bse.get('above', np.nan),
        'N': len(subset)
    }

# Bandwidths and cutoffs to test
bandwidths = [0.10, 0.12, 0.13, 0.14, 0.15]
cutoffs = [3.0, 3.5]

# Run estimations
results = []
for c in cutoffs:
    for bw in bandwidths:
        res = estimate_rd(rdd_data, cutoff=c, bandwidth=bw)
        results.append(res)

results_df = pd.DataFrame(results)

# Plot ATEs across bandwidths
plt.figure(figsize=(10, 6))
sns.lineplot(data=results_df, x='Bandwidth', y='ATE', hue='Cutoff', marker='o')
plt.fill_between(
    results_df[results_df['Cutoff'] == 3.0]['Bandwidth'],
    results_df[results_df['Cutoff'] == 3.0]['ATE'] - 1.96 * results_df[results_df['Cutoff'] == 3.0]['Std_Error'],
    results_df[results_df['Cutoff'] == 3.0]['ATE'] + 1.96 * results_df[results_df['Cutoff'] == 3.0]['Std_Error'],
    alpha=0.2, label='95% CI (3-star)'
)
plt.fill_between(
    results_df[results_df['Cutoff'] == 3.5]['Bandwidth'],
    results_df[results_df['Cutoff'] == 3.5]['ATE'] - 1.96 * results_df[results_df['Cutoff'] == 3.5]['Std_Error'],
    results_df[results_df['Cutoff'] == 3.5]['ATE'] + 1.96 * results_df[results_df['Cutoff'] == 3.5]['Std_Error'],
    alpha=0.2, label='95% CI (3.5-star)'
)
plt.title("RD Estimates of Enrollment Effect Across Bandwidths")
plt.xlabel("Bandwidth")
plt.ylabel("Estimated ATE on Enrollment")
plt.legend(title="Star Rating Cutoff")
plt.grid(True)
plt.tight_layout()

# Question 8: Examine (graphically) whether contracts appear to manipulate the running variable. In other words, look at the distribution of the running variable before and after the relevent threshold values. What do you find?
final['partc_score'] = pd.to_numeric(final['partc_score'], errors='coerce')
final['partcd_score'] = pd.to_numeric(final['partcd_score'], errors='coerce')

# Define running variable
rdd_data['running_score'] = np.where(
    rdd_data['partd'] == 'No',
    rdd_data['partc_score'],
    rdd_data['partcd_score']
)

# Keep only plans with valid running scores
rdd_data = rdd_data[rdd_data['running_score'].notna()]

# Function to plot density around cutoff
def plot_density(data, cutoff, bandwidth=0.3):
    window = data[
        (data['running_score'] >= cutoff - bandwidth) &
        (data['running_score'] <= cutoff + bandwidth)
    ]

    plt.figure(figsize=(8, 5))
    sns.histplot(window['running_score'], bins=40, kde=False, color="slateblue", edgecolor="white")
    plt.axvline(cutoff, color='red', linestyle='--', label=f"Cutoff = {cutoff}")
    plt.title(f"Density of Running Variable Near Cutoff = {cutoff}")
    plt.xlabel("Running Score")
    plt.ylabel("Number of Plans")
    plt.legend()
    plt.tight_layout()

    # Save plot
    output_path = f'/Users/ilsenovis/ECON470HW4/submission1/analysis/results8_density_cutoff_{str(cutoff).replace(".", "")}.png'
    plt.savefig(output_path, bbox_inches='tight')
    plt.close()

plot_density(rdd_data, cutoff=3.0)
plot_density(rdd_data, cutoff=3.5)

## Question 9: Similar to question 4, examine whether plans just above the threshold values have different characteristics than contracts just below the threshold values. Use HMO and Part D status as your plan characteristics.
rdd_data['partc_score'] = pd.to_numeric(rdd_data['partc_score'], errors='coerce')
rdd_data['partcd_score'] = pd.to_numeric(rdd_data['partcd_score'], errors='coerce')

# Define the running variable
rdd_data['running_score'] = np.where(
    rdd_data['partd'] == 'No',
    rdd_data['partc_score'],
    rdd_data['partcd_score']
)

# Convert characteristics to binary
rdd_data['is_hmo'] = (rdd_data['plan_type'].str.upper() == 'HMO').astype(int)
rdd_data['has_partd'] = (rdd_data['partd'] == 'Yes').astype(int)

# Function to compare means above/below cutoff
def compare_characteristics(data, cutoff, bandwidth=0.125):
    subset = data[
        (data['running_score'] >= cutoff - bandwidth) &
        (data['running_score'] <= cutoff + bandwidth)
    ].copy()

    subset['above_cutoff'] = (subset['running_score'] >= cutoff).astype(int)

    summary = subset.groupby('above_cutoff')[['is_hmo', 'has_partd']].mean().reset_index()
    summary['group'] = summary['above_cutoff'].map({0: f"< {cutoff}", 1: f"≥ {cutoff}"})
    summary = summary[['group', 'is_hmo', 'has_partd']]

    return summary

# Compare around 3.0 and 3.5
summary_3 = compare_characteristics(rdd_data, cutoff=3.0)
summary_35 = compare_characteristics(rdd_data, cutoff=3.5)

# Combine and print
characteristics_summary = pd.concat([summary_3, summary_35], keys=['Cutoff 3.0', 'Cutoff 3.5'])
print(characteristics_summary)

## Question 10: Summarize your findings from 5-9. What is the effect of increasing a star rating on enrollments? Briefly explain your results.
# Create a clean summary table for export or display
summary_table = results_df.copy()
summary_table['ATE'] = summary_table['ATE'].round(2)
summary_table['Std_Error'] = summary_table['Std_Error'].round(2)
summary_table['CI_95_Lower'] = (summary_table['ATE'] - 1.96 * summary_table['Std_Error']).round(2)
summary_table['CI_95_Upper'] = (summary_table['ATE'] + 1.96 * summary_table['Std_Error']).round(2)

# Reorder and rename columns for clarity
summary_table = summary_table[[
    'Cutoff', 'Bandwidth', 'ATE', 'Std_Error', 'CI_95_Lower', 'CI_95_Upper', 'N'
]]
summary_table.columns = [
    'Cutoff', 'Bandwidth', 'Estimated ATE', 'Standard Error',
    '95% CI Lower', '95% CI Upper', 'Sample Size'
]

# Save to CSV
summary_table.to_csv('/Users/ilsenovis/ECON470HW4/submission1/analysis/results10_rd_summary_table.csv', index=False)
print(summary_table)