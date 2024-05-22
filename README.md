# Four Assets Project
## ELT Architecture & Data Sources
![image](https://github.com/razikaalmira/four-assets/blob/main/csv_files/four_assets.drawio%20(6))
1. CPI data is extracted from BPS API with Python, while other data (Bitcoin, IHSG, USD to IDR, Gold) is downloaded from investing.com.
2. All data is loaded directly to PostgreSQL Dev schema, all raw with no transformations.
3. Dbt performs data validation, data type conversion, and other necessary transformation.
4. Dbt builds models and summary tables, then they are loaded to PostgreSQL Prod schema.
5. Exploratory data analysis and simulation is done in Python notebook.
