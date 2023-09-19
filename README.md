# Spotify End to End project

## Objective
This project aims to build a comprehensive data pipeline for extracting, transforming, and analyzing Spotify data. Leveraged Python, Amazon Web Services (AWS), Snowflake and powerbi by building an end-to-end data pipeline to process, transform, and visualize Spotify's rich music data. The primary objective was to extract valuable insights from Spotify's  music catalog while constructing a comprehensive data pipeline to automate the entire process.

## Technical skills and Tools
1. Python
2. SQL
3. Lambda function
4. AWS s3
5. Amazon cloudWatch
6. Snowflake
7. PowerBI

## Workflow overview:
1. The pipeline will integrate with the Spotify API to fetch relevant data and store it in an organized manner on AWS S3. The extraction 
   process will be automated by deploying code on AWS Lambda, which will run at scheduled intervals or trigger events by cloudWatch.
2. Once the raw data is extracted and loaded to s3 a trigger will be generated to invoke run lambda function. This step involves 
   transformation i.e, clean and format the data for further analysis. This function will be designed to handle various data processing 
   tasks, such as data normalization, aggregation, or filtering, based on requirements. and transformed data will be stored back to s3.
   maintaining proper file organization and structure. 
3. Now we have the traansformed data, its time to bring this data to snowflake This will allow easy access and retrieval of the processed 
   data for further analysis. with the help of snowpipe i have automated this processwhenever some newfile uploaded to s3 an auto trigger 
   generated and sent to snowpipe to bring this neew data to respective table and update table.
4. finally i have integrated snowflake with powerbi application for creating visualization of music data. created some KPIs toget deeper 
   insights of data like get artist name ot a song, duration of a song, popularity of a song etc.

## Architecture:
![Screenshot (74)](https://github.com/rockraj999/Visualizations/assets/121096737/1cf6f815-568d-4bfb-990a-d3b6f61d3f4a)

## Achievements:
1. Successfully transformed raw spotify data into an intuitive, user-friendly visualization dashboard.
2. Improved data accessibility and gain insight out of it.
3. Demonstrated data extraction, cleaning, transformation, creating fact and dimension table, data visualization and data analysis using 
   various aws applications , snowflake and powerbi. 
4. Enhanced the understanding of spotify's music collection through interactive dashboard.







 
