Lab 2 – Analytics Reporting Pipeline

Please, note that in the first lab normalized dataset was prepared by cutting data (70k raws left in each month). The document of data preparation is load_sample.ipynb. So it impacted the results of the Lab 2

1. Docker-Compose Setup
    * docker-compose.jupyter.yml started JupiterNotebook (exposing port 8888).
    * Ensured the notebook can connect to the MySQL container (ad_analytics_db) on port 3306 via host.docker.internal.
2. Data Exploration & SQL Queries
    * Restored and normalized sample data into dim_campaign, dim_advertiser, dim_user, dim_targeting, and fact_event schemas.
    * Developed SQL queries to answer key questions:
1. Campaign Performance: Retrieve the top 5 campaigns with the highest Click-Through Rate (CTR) over the 30 days period.  Which advertisers are the biggest spenders, and how does their spending correlate with engagement?
2. Advertiser Spending: Identify the advertisers that have spent the most money on ad impressions in the last month. How efficiently is each campaign spending its budget relative to clicks and impressions?
3.Cost Efficiency: Calculate the average Cost Per Click (CPC) and Cost Per Mille (CPM) for each campaign. In which countries or regions do ads generate the highest revenue, and where should advertisers focus their efforts?
4. Regional Analysis: Find the top-performing locations based on total ad revenue generated from clicks. Which users are the most engaged with advertising content?
5. User Engagement: Retrieve the top 10 users who have clicked on the most ads.  Which campaigns might need a budget increase to continue running effectively?
6. Budget Consumption: Identify campaigns that have spent more than 80% of their total budget and are close to exhausting their funds.  Do certain types of ads perform better on mobile than desktop? How should advertisers adjust their strategies?
7. Device Performance Comparison: Compare CTR across different device types (mobile, desktop, tablet).

1. Jupyter Notebook Integration
    * Installed necessary Python packages (mysql-connector-python, SQLAlchemy, pandas) inside Jupyter.
    * Connected to MySQL via SQLAlchemy, executed all queries, and visualized results interactively.
    * Exported query results to CSV and JSON directly from the notebook.
2. Automated Reporting Script
    * Created DE_Lab_2.ipynb, which:
        1. Reads environment variables for database credentials.
        2. Connects to MySQL using SQLAlchemy.
        3. Executes each predefined SQL query.
        4. Saves individual CSV files under reports/.
        5. Aggregates all results into a single full_report.json.
3. Screenshots
