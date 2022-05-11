
![](https://miro.medium.com/max/1400/1*l6ukY_v43LK9LB9fjV2f0g.png)

# Project: Data Modeling with Postgres

A startup called **Sparkify** wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. 

The goal is to create a database schema and ETL pipeline for this analysis and compare the results with their expected results.

## Project Description

This project on data modeling was made with Postgres defining fact and dimension tables (star schema) for a particular analytic focus. And has an ETL pipeline using Python to transfers data from files in two local directories into these tables .

# Files

### Song Dataset

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

>song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json


And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
This dataset was distributed into songs table and artist table.

### Log Dataset

The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![](https://video.udacity-data.com/topher/2019/February/5c6c15e9_log-data/log-data.png)
This dataset was distributed into time, users, and songs_play table.

### Entity Relationship Diagram (ERD) - Star Schema

<a href="https://ibb.co/DDTg6GK"><img src="https://i.ibb.co/hRw1nX8/udacity.png" alt="udacity" border="0"></a>

 **Benefits of the Star Schema**

-   It is extremely simple to understand and build.
-   No need for complex joins when querying data.
-   Accessing data is faster (because the engine doesn’t have to join various tables to generate results).
-   Simpler to derive business insights.
-   Works well with certain tools for analytics, in particular, with OLAP systems that can create OLAP cubes from data stored.


## Run the scripts
The scripts of this project consist in create tables and run the ETL.

#### The environment:
-  Python versions from 3.6 to 3.10    
-  PostgreSQL server versions from 7.4 to 14    
-  PostgreSQL client library version from 9.1
-  Psycopg 2.9.3 documentation](https://www.psycopg.org/docs/index.html)
- Pandas lib
- Numpy lib
- OS lib
- Datetime lib
 
#### Create table
To create tables use the scripts:
- Statements in `sql_queries.py` to create each table (using DDL – Data Definition Language)
- Run `create_tables.py` at console to create your database and tables.
- Run `test.ipynb` to confirm the creation of your tables with the correct columns. 
	
#### Run the ETL
To make the ETL use the scripts:
- Statements in `sql_queries.py `to insert records into each table (using DML - Data Manipulation Language)
- Run `etl.py` at console
- Run `test.ipynb` to confirm the table's contents 
	 

#### More in this project
To understanding the files and data use the  `etl.ipynb `

### Data Analytics

Using this ERD and their tables, is an easier way to create queries to answer the single question to analytics issues like:
- which level of user  (paid/free) listen  more musics
	- SELECT  level, count(*) as count from songsplay group by level order by count desc LIMIT 1

	| level | count |
	|--|--|
	| paid | 6093 |
	
- how much songs are listening by weekday
	- SELECT  dayname, count(*) as count from time join songsplay on time.start_time = songsplay.start_time group by dayname, weekday order by weekday
	
	|Day|Count  |
	|--|--|
	| Monday | 80  |
	| Tuesday | 80 |
	| Wednesday | 80 |
	| Thursday | 92 |
	| Friday | 100 |
	| Saturday | 80 |
	| Sunday | 80 |

- how much songs are listened by users  gender
	- SELECT users.gender, count(*) from songsplay join users on songsplay.user_id = users.user_id group by gender

| gender | count  |
|--|--|
| M | 4576 |
| F | 10964 |

Or make more advanced topics like predict user churn or music recommendation.
 
