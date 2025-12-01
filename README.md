# Group Analytics Project

**Course:** BANA 320 - Predictive Analytics  
**Dataset:** [Yelp Open Dataset](https://business.yelp.com/data/resources/open-dataset/)  
**Group Name:** Three Gits  

[![CD](https://github.com/vbalalian/three-gits/actions/workflows/cd.yml/badge.svg)](https://github.com/vbalalian/three-gits/actions/workflows/cd.yml)

## Overview

**Problem Statement:** Does adding sentiment features from the first 90 days of reviews significantly improve the accuracy of predicting a restaurant's 1-year Yelp rating, compared to using non-text features alone?

**Target Variable:** Average star rating 12 months after first review

**Result:** No — sentiment features did not significantly improve prediction accuracy when geographic and user-based context features were already included.

## Data Pipeline

Raw Yelp JSON data (hosted in Google Cloud Storage) is transformed via **dbt + BigQuery**:

1. **Filtering:** Business dataset filtered to restaurants only
2. **Qualification criteria:**
   - Minimum 1 year of review history
   - At least 3 reviews in first 90 days
   - At least 10 reviews in first year
3. **Feature aggregation:** Check-ins, user metrics, and zip-code comparisons aggregated to restaurant level
4. **Time windowing:** Separate datasets for 90-day (features) and 365-day (target) review periods

## Feature Categories

| Category | Description | Count |
|----------|-------------|-------|
| Early Review Metrics | Review count & average rating (first 90 days) | 2 |
| Check-in Patterns | Day-of-week and time-of-day distributions | 11 |
| User Characteristics | Reviewer reputation, engagement, experience | 10 |
| Zip Code Context | Restaurant metrics vs. local averages | 4 |
| **Sentiment (VADER)** | Positive, negative, neutral, compound scores | 4 |

## Methodology

**Phase 1:** Train models using all features *except* sentiment  
**Phase 2:** Add sentiment features derived from VADER analysis of first-90-day reviews  
**Comparison:** Paired one-tailed t-test on absolute prediction errors (α = 0.05)

### Models Tested
- Support Vector Regression (linear kernel)
- Linear Regression
- Random Forest Regressor
- XGBoost Regressor
- Stacked Ensemble (average of all four)

## Key Findings

1. **Geographic context subsumes sentiment signal** — Zip-code comparison features (rating vs. local average, etc.) already capture competitive positioning that sentiment would otherwise indicate.

2. **Early rating is the dominant predictor** — The 90-day average rating had the highest mutual information with 1-year rating by a wide margin.

3. **Reviewer quality correlates with outcomes** — User characteristics (average stars given, engagement metrics) ranked among top predictive features.

## Tools & Technologies

- **Data Warehouse:** Google BigQuery
- **Transformation:** dbt
- **Analysis:** Python (pandas, scikit-learn, XGBoost, VADER)
- **Environment:** Google Colab
- **CI/CD:** GitHub Actions

## Analysis

The complete analysis notebook is available [here](./analysis/bana320_three_gits.ipynb). It includes all code, visualizations, and statistical test results.

> **Note:** The data pipeline requires access to a private Google Cloud project. The notebook is provided for review purposes; reproducing it would require setting up your own BigQuery environment with the Yelp Open Dataset.

*BANA 320 — Fall 2025*
