# Data Lake

This is a udacity's data engineer nano-degree project.

## About the project

The goal of this project is to construct a data lake where data analytics teams can exploit their data more flexible manner.
The original datasets are stored in S3 as json files. Here, a ETL pipeline is created to transform these json files to dimensional tables.
In this ETL pipeline, datasets are processed by using apache spark, and each tables is saved in S3 as parquet format.


## Contents
`dl.cfg`: definitions of parameters such as S3 url or AWS account.

`etl.py`: loads prepared `song-data` and `log-data` datasets from S3. Transform json data to dimensional tabels.

`README.md`: this document.

## Dataset
The raw data for this project are not included in this repository. 
They are stored in S3.

```
LOG_DATA='s3://udacity-dend/log-data'
SONG_DATA='s3://udacity-dend/song-data'
```

The `song-dataset` is a subset of the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).
The `log-dataset` is generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs.

The data are stored in the multiple files in the following layout.

```
$ tree ./data

data
├── log_data
│   └── 2018
│       └── 11
│           ├── 2018-11-01-events.json
...
│           └── 2018-11-30-events.json
└── song_data
    └── A
        ├── A
        │   ├── A
        │   │   ├── TRAAAAW128F429D538.json
...
        │   │   └── TRAAAVO128F93133D4.json
        │   ├── B
        │   │   ├── TRAABCL128F4286650.json
...
        │   │   └── TRAABYW128F4244559.json
        │   └── C
        │       ├── TRAACCG128F92E8A55.json
...
        │       └── TRAACZK128F4243829.json
        └── B
            ├── A
            │   ├── TRABACN128F425B784.json
...
            │   └── TRABAZH128F930419A.json
            ├── B
            │   ├── TRABBAM128F429D223.json
...
            │   └── TRABBZN12903CD9297.json
            └── C
                ├── TRABCAJ12903CDFCC2.json
...
                └── TRABCYE128F934CE1D.json
```

## Requirement

This project assumes that you have created IAM user who can read and write S3 buckets.

## Usage
Prepare AWS account and create IAM user. Set appropriate value in `dl.cfg`.
Instead of using this cfg file I defined environment variables outside the repository.
The `INPUT_DATA` is URL where input datasets are stored.
The `OUTPUT_DATA` is URL where outputs would be saved.

Run `etl.py` to transform json dataset to tables. Note that this script create some file into your S3 bucket indicated by `OUTPUT_DATA`.

```
python etl.py
```

## About Table Schema

### Table Definition

In this project, the following tables are defined.

#### Fact Table

* songplays - records in log data associated with song plays i.e. records with page NextSong
  - songplay_id
  - start_time
  - user_id
  - level
  - song_id
  - artist_id
  - session_id
  - location
  - user_agent

#### Dimension Tables
* users - users in the app
  - user_id
  - first_name
  - last_name
  - gender
  - level

* songs - songs in the music database
  - song_id
  - title
  - artist_id
  - year
  - duration

* artists - artists in the music database
  - artist_id
  - name
  - location
  - lattitude
  - longitude

* time - timestamps of records in songplays broken down into specific units
  - start_time
  - hour
  - day
  - week
  - month
  - year
  - weekday

#### Staging Tables

* staging_events
  - artist
  - gender
  - lastName
  - firstName
  - level
  - sessionId
  - song
  - ts
  - location
  - userAgent
  - userId

* staging_songs
  - artist_id
  - artist_latitude
  - artist_longitude
  - artist_name
  - song_id
  - title
  - duration
  - year

