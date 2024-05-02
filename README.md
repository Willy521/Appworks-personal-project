# Estate Data Hub
A Data Hub that give insights into the real estate market through visual charts. I gather a wide range of data, including news articles and potential factors affecting housing prices, and conduct correlation analyses.



[comment]: <> (![Estate Data Hub]&#40;https://img.onl/1bqVe&#41;)

## Architecture
<img width="1390" alt="截圖 2023-11-03 下午4 25 47" src="https://github.com/Willy521/Appworks-personal-project/assets/48906493/d4078f4b-1b50-444c-8a76-d75c7afd83d3">

## Data Pipeline

Extract:

* Web crawling from various websites using requests and Beautiful Soup. 
    * Sources: 國泰建設、中華民國統計資訊網、鉅亨網新聞、內政部實價登錄、好房網。
* Backing up the crawled data to AWS S3.

Transform: 

* Retrieve the backup files from AWS S3.
* Use Python for data cleaning and preprocessing.

Load: 

* Load the processed data into AWS RDS.

## Technologies

Programming Language:
* Python

DataBase:
* MySQL

Dashboard:
* Streamlit

Cloud:
* AWS S3
* AWS RDS
* AWS CloudWatch

Others:
* Github Actions/CICD
* Docker
* Nginx
* Airflow



## Features
1. **Real-Time Tracking of Key Factors in Housing Prices**

    Users can access the latest data results with high relevance to housing prices.

https://github.com/Willy521/Appworks-personal-project/assets/48906493/ef8d3f02-8080-4029-9e71-c30be56d7901

2. **News Sentiment Analysis**
    
    Users can quickly get informed on the real estate news highlights of the past month.

https://github.com/Willy521/Appworks-personal-project/assets/48906493/c8b746a5-6203-4525-bf80-6ea2ca0d7c03


3. **Regional Transaction Hotspot Analysis**
    
    Users can observe recent real estate transaction hotspots.

https://github.com/Willy521/Appworks-personal-project/assets/48906493/9e9b51c0-802b-4011-9c2d-63d733113fdb

4. **Analysis of Factors Affecting House Prices**
   
    Users can analyze current data trends related to house prices.

https://github.com/Willy521/Appworks-personal-project/assets/48906493/0246c94c-33a5-4f21-9b6d-df053bc79176




5. **Future Outlook and User Participation Model**

    Users interested in data analysis related to property prices can submit feedback via a form that sends directly to the author's email. This functionality allows for the discovery of additional key factors associated with house prices.

https://github.com/Willy521/Appworks-personal-project/assets/48906493/f94d4f5c-507b-4b5b-855a-95ca7e4cba5e


## Author
Wei-Ting, Chen: r94040119@gmail.com
