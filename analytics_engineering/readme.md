# Analytics Engineering Basics

Current development in data domain developments left a gaps between data engineer and data analytics. Where data analyst only take care of analysis to solve problem, they aren't meant to be write more and more code (software engineer task). Also data engineer which maintain the infrasturcture of the data do not know what the data will used for for analytics. Then come *Analytics Engineer* to fill the gap. Analytics Engineer introduces the good software engineering practices to the efforts of data analytics and data scientist. 

Tools: 
- Data Loading
- Data Storing: Snowflake, BigQuery, Redshift
- Data Modelling: dbt, Dataform
- Data presentation: Looker, Mode, Tableau

## Data Modelling Concept
- ETL (Extract Transform Load)
Longer to implement. Slightly more stable and compliant data analysis. Higher storage and compute costs.
- ELT (Extract Load Transform)
Transform the data where is it in the data warehouse. Faster and more flexible data analysis. Lower cost and lower maintenance.

## Kimball's Dimensional Modeling
- Objective: Deliver data that understanable to business user & deliver fast query performance
- Approach: Prioritise user understandability and query performance over non redundant data (3NF)

## Elements of Dimensional Modelling
- Fact tables: Like 'verbs'. Measurments, metrucs ir facts. Corresponds to a business process.
- Dimension table: 'nouns'. Corresponds to a business entity. Provides context to a business process.

## Architechture of Dimensional Modeling
- Stage Area: Contains raw data. Not meant to exposed to public. Like warehouse of foodstuff in restaurant
- Processing Area: Kitchen in a restaurant. Take raws data and make model. Limited access to the chef.
- Presentation Area: Dining hall. Exposure to business stakeholder. 