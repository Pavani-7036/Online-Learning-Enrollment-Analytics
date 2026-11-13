# Online Learning Enrollment Analytics | Databricks Data Engineering Project

**End-to-End Data Engineering • Lakehouse Architecture • SQL • Business Intelligence**

---

## 1. Executive Summary

This project demonstrates the design and implementation of an end-to-end data engineering solution for an online learning platform using Databricks and Lakeflow Spark Declarative Pipelines.

The solution transforms raw, inconsistent enrollment data into high-quality, analytics-ready datasets through a structured Medallion Architecture.

The curated datasets enable business stakeholders to monitor enrollment trends, revenue performance, learner engagement, course completion rates and data quality through an interactive Databricks AI/BI dashboard.

## 2. Business Problem

Online learning platforms generate enrollment data containing valuable information about learners, courses, payments and learning progress.

However, raw datasets often contain duplicate records, inconsistent formatting, missing values and invalid business attributes, resulting in inaccurate reporting and unreliable business decisions.

This project addresses these challenges by developing a repeatable ETL pipeline that ensures data accuracy, consistency and reliability across the analytics lifecycle.

## 3. Project Objectives

- Develop an end-to-end data engineering pipeline using Databricks.
- Implement Bronze, Silver and Gold layers following Medallion Architecture.
- Apply data cleansing, transformation and validation rules.
- Ensure data integrity through reconciliation and quality checks.
- Create business-ready aggregated datasets for analytical reporting.
- Deliver an interactive dashboard to support data-driven decision-making.

## 4. Technology Stack

| Technology | Implementation |
|---|---|
| Databricks | Unified data engineering and analytics platform |
| Apache Spark | Distributed data processing |
| SQL | Data transformation and business aggregations |
| Lakeflow Spark Declarative Pipelines | Declarative ETL pipeline orchestration |
| Unity Catalog | Centralized data management |
| Delta Lake | Reliable table storage |
| Medallion Architecture | Multi-layer data processing |
| Databricks AI/BI | Business intelligence and visualization |

## 5. Solution Architecture

The project follows the Medallion Architecture pattern, progressively improving data quality across three processing layers.

```text
              Source Dataset
       course_enrollments_raw.csv
                     |
                     v
           Unity Catalog Volume
                     |
                     v
              BRONZE LAYER
          Raw Data Ingestion
                     |
                     v
              SILVER LAYER
       Cleansing & Transformation
              /             \
             v               v
       Clean Records    Rejected Records
             |
             v
              GOLD LAYER
        Business Aggregations
             |
             v
       Databricks AI/BI Dashboard
```

### Bronze Layer — Data Ingestion

The Bronze layer serves as the foundation of the data pipeline by preserving the original source data.

**Implementation:**

- Ingested CSV data from Unity Catalog Volumes.
- Created a streaming table using Lakeflow Spark Declarative Pipelines.
- Preserved raw enrollment attributes.
- Captured source file metadata and ingestion timestamps.
- Maintained source-level traceability for auditing and reconciliation.

**Output Table:** `bronze_course_enrollments`

### Silver Layer — Data Transformation and Validation

The Silver layer transforms raw enrollment records into standardized, validated and reliable datasets.

**Key transformations:**

- Duplicate enrollment removal.
- Text normalization using TRIM and INITCAP.
- Email standardization using LOWER and TRIM.
- Missing-value handling using COALESCE.
- Recovery of missing payment amounts.
- Validation of learner progress and payment values.
- Derived learner status using CASE expressions.
- Isolation of invalid records with rejection reasons.

**Business validation rules:**

| Rule | Validation |
|---|---|
| Enrollment uniqueness | One record per enrollment ID |
| Learner progress | Between 0 and 100 |
| Payment amount | Greater than or equal to zero |
| Email formatting | Lowercase and trimmed |
| Missing category or city | Replaced with Unknown |
| Missing payment | List price minus discount |

**Output Tables:**

- `silver_enrollments_clean`
- `silver_enrollments_rejected`

### Gold Layer — Business-Ready Analytics

The Gold layer contains curated materialized views optimized for business reporting and analytical consumption.

| Gold View | Business Purpose |
|---|---|
| gold_monthly_enrollments | Monthly enrollment and revenue trends |
| gold_category_summary | Category-level revenue and completion analysis |
| gold_course_summary | Course performance, ratings and engagement |
| gold_channel_summary | Enrollment channel effectiveness |
| gold_data_quality | Data-quality monitoring and reconciliation |

All business aggregations are derived from validated Silver-layer records, ensuring consistent reporting across dashboard visualizations.

## 6. Dataset Description

The project uses a synthetic online learning enrollment dataset representing course enrollment activities during 2025.

| Attribute | Description |
|---|---|
| Source file | course_enrollments_raw.csv |
| Raw records | 1,283 |
| Attributes | 20 |
| Courses | 18 |
| Course categories | 6 |
| Instructors | 10 |
| Enrollment period | January–December 2025 |
| Data granularity | One learner enrollment per course |

The dataset includes learner demographics, course information, enrollment channels, pricing, discounts, progress percentages, ratings and certificate information.

## 7. Data Quality and Reconciliation

Data quality is a core component of the pipeline. Invalid records are isolated from analytical datasets while maintaining visibility into source-level issues.

### Data Processing Results

| Metric | Result |
|---|---:|
| Total Source Records | 1,283 |
| Valid Enrollment Records | 1,241 |
| Rejected Records | 19 |
| Duplicate Records Removed | 23 |
| Valid Record Percentage | 96.73% |
| Reconciliation Status | Passed |

### Reconciliation Framework

```text
Source Records = Clean Records + Rejected Records + Duplicates

1,283 = 1,241 + 19 + 23

Reconciliation Status: PASSED
```

This validation ensures that every source record is accounted for throughout the transformation process.

## 8. Business Intelligence Dashboard

An interactive three-page Databricks AI/BI dashboard was developed using Gold-layer datasets to provide actionable business insights.

### Page 1 — Executive Overview

Provides a consolidated view of platform performance.

**Key Performance Indicators:**

| Business KPI | Result |
|---|---:|
| Total Enrollments | 1,241 |
| Total Revenue | ₹27.21 Lakhs |
| Total Course Completions | 349 |
| Total Learning Hours | 16,856.1 |
| Overall Completion Rate | 28.12% |

**Visualizations:**

- Monthly revenue trends.
- Revenue distribution by course category.
- Learner status distribution.
- Interactive month, category and course-level filters.

### Page 2 — Course and Channel Performance

Provides detailed insights into course profitability, learner engagement and enrollment acquisition channels.

**Top Five Courses by Revenue:**

| Course | Revenue | Completion Rate |
|---|---:|---:|
| Machine Learning A to Z | ₹427,854.90 | 32.4% |
| Python for Data Science | ₹397,967.30 | 25.0% |
| Deep Learning with PyTorch | ₹232,953.40 | 36.4% |
| AWS Cloud Practitioner | ₹224,025.30 | 27.9% |
| Azure Data Engineering | ₹209,147.70 | 23.7% |

Additional analysis includes course ratings, category-level completion rates, enrollments by channel and channel-wise revenue.

### Page 3 — Data Quality and Governance

Provides transparency into pipeline processing and data reliability.

**Monitoring metrics:**

- Bronze and Silver record counts.
- Duplicate and rejected record counts.
- Learner counts before and after email normalization.
- Data-quality issues across source attributes.
- Source-to-target reconciliation.

## 9. Key Business Insights

- Machine Learning A to Z emerged as the highest revenue-generating course, contributing approximately ₹4.28 lakhs.
- Python for Data Science generated approximately ₹3.98 lakhs in revenue.
- Deep Learning with PyTorch achieved a completion rate of 36.4% among the five highest-revenue courses.
- The platform recorded 349 completed enrollments and over 16,856 hours of learning activity.
- The overall course completion rate of 28.12% highlights an opportunity to improve learner engagement and retention.
- Data-quality transformations eliminated duplicate and invalid records, improving the reliability of business reporting.


## 10. Conclusion

This project demonstrates how modern data engineering practices can transform raw operational data into trusted, actionable business intelligence.

By leveraging Databricks, Lakeflow Spark Declarative Pipelines and Medallion Architecture, the solution establishes a structured and repeatable approach to data ingestion, transformation, validation and analytical reporting.

The resulting platform enables reliable business insights while maintaining data quality, traceability and transparency throughout the data lifecycle.


If you find this project valuable, consider giving the repository a star.
