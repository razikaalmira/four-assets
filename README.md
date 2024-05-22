# Four Assets Project
## ELT Architecture & Data Sources
![image](https://github.com/razikaalmira/four-assets/blob/main/csv_files/four_assets.drawio(6).png)
1. CPI data is extracted from BPS API with Python, while other data (Bitcoin, IHSG, USD to IDR, Gold) is downloaded from investing.com.
2. All data is loaded directly to PostgreSQL Dev schema, all raw with no transformations.
3. Dbt performs data validation, data type conversion, and other necessary transformation.
4. Dbt builds models and summary tables, then they are loaded to PostgreSQL Prod schema.
5. Exploratory data analysis and simulation is done in Python notebook.

## Summary & Recommendations
- Bitcoin is significantly more volatile compared to IHSG, USD, and gold
- Incorporating USD and IHSG in a portfolio is a good option to diversify risks because they relatively move to different direction based on the correlation coefficient
- This entire analysis is solely based on historical data with relatively small data points, more forward-looking analysis is needed to get better analysis especially for Bitcoin that cannot be easily extrapolated
- This project only shows the performance of Indonesian stock in general (thus using IHSG), more detailed analysis can be done using specific indices or even individual stocks
