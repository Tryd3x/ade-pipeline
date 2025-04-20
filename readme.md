# 💊 Medication Safety and Adverse Drug Events in Older Adults

## 📌 Project Overview

This project focuses on identifying high-risk medications and patterns of adverse drug events (ADEs) among older adults (65+). It aims to provide actionable insights that healthcare providers, policy makers, and researchers can use to reduce preventable hospitalizations, improve medication safety, and optimize patient outcomes.

## 🎯 Objective

To build an end-to-end data engineering pipeline that:
- Ingests real-world adverse drug event data from the FDA Open Data API
- Transforms and models the data into a star-schema format
- Enables analytical queries and dashboards focused on medication safety in elderly populations

The final output helps stakeholders answer critical questions about drug-related risks in aging populations.

## ❓ Key Questions the Project Aims to Answer

- Which medications are most frequently associated with serious adverse events in patients over 65?
- What types of events (e.g., hospitalization, disability, death) are most common in older adults taking specific drugs?
- Are there patterns or trends in ADEs over time that suggest worsening safety profiles?
- Which age sub-groups (e.g., 65–70, 71–80, 81+) are most at risk?
- What drug combinations are disproportionately represented in adverse event reports?

Run tests:
```
pytest -v src
```