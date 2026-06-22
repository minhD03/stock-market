# Airflow Project:
This project utilized Airflow to manage DAG (Directed Acyclic Graph, a modelling technique to demonstrate sequential process without loop) to crawl and transform data from [Yahoo Stock Market](https://query1.finance.yahoo.com/v8/finance/chart/amzn)(Amazon For Example).
Using collected data, I will transform it to datasets that are usable in visualization tools like Power BI.
## 1. Overview:
![alt text](https://github.com/minhD03/stock-market/blob/6b27908dd56cb02b73b6ff1c8d70f25c1526b4bf/images/Diagram%201.png)
The project is initialized inside a Docker container with Apache Airflow to ingest and process data before saving it to data warehouse in PostgreSQL. Inside Airflow, Minio is considered as Data Lake to contain Raw and processed data. Meanwhile, Spark is used as an external tool to process data. For notification, I use Slack as a free and simple method to receive message from Airflow for process minitoring.
## 2. Core functions:
![alt text](https://github.com/minhD03/stock-market/blob/6b27908dd56cb02b73b6ff1c8d70f25c1526b4bf/images/Diagram%202.png)
These are the steps processed in the workflow:
+) Check if API is available: Try to get the response from the host website and notify user if there is no exist website (The website formula is https://query1.finance.yahoo.com/v8/finance/chart/"SYMBOL", where "SYMBOL" coressponding to the companies that user wants to collect data). Currently, the available companies are: AAPL, MSFT, GOOGL, AMZN, TSLA, META, NFLX, NVDA, AMD, INTC.
