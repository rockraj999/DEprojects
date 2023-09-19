use prsonal_DB

--Storage Integration
create or replace storage integration aws_s3_integration
type= external_stage
storage_provider='S3'
enabled=TRUE
storage_aws_role_arn='**'
storage_allowed_locations=('s3://spotify-etl-project-biswa')
COMMENT = 'Creating connection to S3' ;

show integrations;

DESC integration aws_s3_integration;

grant usage on integration aws_s3_integration to role accountadmin;


--Create file format object

create or replace file format csv_fileFormat
type= 'CSV'
field_delimiter=','
skip_header=1
null_if=('NULL','NULL')
empty_field_as_null=TRUE;


-- Create album stage object with integration object & file format object

create or replace stage album_csvFolder
url='s3://spotify-etl-project-biswa/transformed_data/album_data/'
storage_integration = aws_s3_integration
file_format= csv_fileFormat;

list @album_csvFolder

--create AlbumsData table 

CREATE OR REPLACE TABLE ALBUMS_DATA(
    album_id VARCHAR(30),
    name VARCHAR(1000),
    release_date DATE,
    total_tracks INT,
    url VARCHAR(1000)
    );

-- Create artist stage object with integration object & file format object

create or replace stage artist_csvFolder
url='s3://spotify-etl-project-biswa/transformed_data/artist_data/'
storage_integration = aws_s3_integration
file_format= csv_fileFormat;

list @artist_csvFolder

-- create Artist table

CREATE OR REPLACE TABLE ARTIST_DATA(	
    artist_id VARCHAR(30),
    artist_name VARCHAR(1000),
    external_url VARCHAR(1000)
    );

-- Create songs stage object with integration object & file format object 

create or replace stage song_csvFolder
url='s3://spotify-etl-project-biswa/transformed_data/songs_data/'
storage_integration = aws_s3_integration
file_format= csv_fileFormat;

list @song_csvFolder

-- create Songs table 

CREATE OR REPLACE TABLE SONGS_DATA(
    SONG_ID VARCHAR(30),
    song_name VARCHAR(1000),
    duration_ms INT,
    url VARCHAR(1000),
    popularity INT,
    song_added DATE,
    album_id VARCHAR(100),
    artist_id VARCHAR(100) 
    );

select * from artist_data

-- Load data using copy command

copy into artist_data
fom @artist_csvFolder
on_error='Continue';

copy into songs_data
from @song_csvFolder

copy into ALBUMS_DATA
from @album_csvFolder
on_error='Continue'

-- Query loaded data

select * from songs_data; 


-- Define Artist pipe

create or replace pipe artist_pipe
auto_ingest=TRUE
as
copy into artist_data
from @artist_csvFolder

create pipe song_pipe
auto_ingest=TRUE
as
copy into songs_data
from @song_csvFolder

create pipe album_pipe
auto_ingest=TRUE
as
copy into albums_data
from @album_csvFolder

show pipes


SELECT CURRENT_WAREHOUSE();




