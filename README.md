# Stock Market Project:


## Table of Contents

- [1) Introduction](#1-introduction)
- [2) Architecture](#2-architecture)
- [3) Code Structure](#3-code-structure)
- [4) User Guide](#4-user-guide)
  
## 1. Introduction:
![alt text](https://github.com/minhD03/stock-market/blob/6b27908dd56cb02b73b6ff1c8d70f25c1526b4bf/images/Diagram%201.png)
This project utilized Airflow to manage DAG (Directed Acyclic Graph, a modelling technique to demonstrate sequential process without loop) to crawl and transform data from [Yahoo Stock Market](https://query1.finance.yahoo.com/v8/finance/chart/amzn)(Amazon For Example).
Using collected data, I will transform it to datasets that are usable in visualization tools like Power BI.

The project is initialized inside a Docker container with Apache Airflow to ingest and process data before saving it to data warehouse in PostgreSQL. Inside Airflow, Minio is considered as Data Lake to contain Raw and processed data. Meanwhile, Spark is used as an external tool to process data. For notification, I use Slack as a free and simple method to receive message from Airflow for process minitoring.
## 2. Architecture:
![alt text](https://github.com/minhD03/stock-market/blob/c8f161fa3f4586e095c681ff317d2670da0115d5/images/Diagram%202.png)
These are the steps processed in the workflow:

+) Check if API is available: Try to get the response from the host website and notify user if there is no exist website (The website formula is https://query1.finance.yahoo.com/v8/finance/chart/"SYMBOL", where "SYMBOL" coressponding to the companies that user wants to collect data). Currently, the available companies are: AAPL, MSFT, GOOGL, AMZN, TSLA, META, NFLX, NVDA, AMD, INTC.

+) Get the stock price by collecting the json dataset from the server. Then, store the price in Local Minio Console inside the coressponding company symbol.

+) From there Spark App will transform the data by creating csv file with timestamp, close, high, low, open and volume.

+) With the formatted Dataset, it then will be saved into PostgreSQL server for later use.
## 3. Code Structure:

```
.
├── docker-compose.yaml
├── init_airflow_role.sql       # initialize airflow role
├── connection_strings (Users are required to input this in Airflow Connection Manager)
├── dag/
│   └── stock_market.py         # manage the dag flow.
└── include/
    └── scripts/                # set up minio and perform tasks

```
## 4. User Guide:
### a. Tool required:
Docker is needed to install before running the container. Furthermore, Python with separated environment is recommended to avoid conflict. For notification, The Slack API Token is obtained from your Slack Account.

For setting up slack, these are the general steps:

- Creating your account (recommend using Google as you will get email notify in here).
  
- Create new Workspace.
  
- Create a new Channel by Right Click on Channels => Create New Channel.
  
- Go to api.slack.com to Create New App, using the workspace before.

- Inside the APP, go to Setting, OAuth and Permissions => Scopes > Add Chat:write and Chat:write.public as permission => Install.

- Get the token (For later use).
  
### b. Running steps:
- First, initiate the container:

```bash
docker compose up -d --build
```

- Login to your local Airflow at (Username and password are "airflow"):
http://localhost:8080/

- Go to **Admin => Connection => Add a new record** to add the connections required in the [Connection String](https://github.com/minhD03/stock-market/blob/c8f161fa3f4586e095c681ff317d2670da0115d5/connection_strings.yml)

- Change Directory to Spark folder and run this command to build Spark APP:
```bash
docker build -t spark-app .
```
- Run and Trigger the DAG in Airflow. The datasets are stored at DataLake Minio Console http://localhost:9001/ (username, password = minio, minio123)

- Go to http://localhost:5050/ to access PostgreSQL server. Login with (username, password) as (pgadmin@localhost.com, pgadmin)

- Right Click at Server and then add a new server with General => Name (1) (localhost:5432 for example), Connection => Hostname, username, password = postgres and then save

- When using Power BI to access dataset, add as PostgreSQL source and with access link = localhost:5432 (set as above) and database name as postgres (set as above).



