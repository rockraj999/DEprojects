import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd 


def album(tracks):
    album_name = []
    album_id = []
    album_release_date = []
    album_total_tracks = []
    album_external_urls = []
    for i in tracks['items']:
        album_name.append(i['track']['album']['name'])
        album_id.append(i['track']['album']['id'])
        album_release_date.append(i['track']['album']['release_date'])
        album_total_tracks.append(i['track']['album']['total_tracks'])
        album_external_urls.append(i['track']['album']['external_urls']['spotify'])

    album_df = pd.DataFrame(
        {'id': album_id, 'name': album_name, 'total_tracks': album_total_tracks, 'release_date': album_release_date,
         'url': album_external_urls})
    return album_df


def artist(tracks):
    artist_id = []
    artist_name = []
    artist_url = []

    for data in tracks['items']:
        artist = []
        for j in data['track']['artists']:
            artist.append(j['name'])
        artist_name.append(','.join(artist))
        artist_id.append(data['track']['album']['artists'][0]['id'])
        artist_url.append(data['track']['href'])
        del artist

    artist_df = pd.DataFrame({'artist_name': artist_name, 'artist_id': artist_id, 'artist_url': artist_url})
    return artist_df


def songs(tracks):
    song_id = []
    song_name = []
    song_duration = []
    song_url = []
    song_popularity = []
    song_added = []
    album_id = []
    artist_id = []
    for data in tracks['items']:
        song_id.append(data['track']['id'])
        song_name.append(data['track']['name'])
        song_duration.append(data['track']['duration_ms'] / 1000)
        song_url.append(data['track']['external_urls']['spotify'])
        song_popularity.append(data['track']['popularity'])
        song_added.append(data['added_at'])
        album_id.append(data['track']['album']['id'])
        artist_id.append(data['track']['album']['artists'][0]['id'])

    song_df = pd.DataFrame(
        {'song_id': song_id, 'song_name': song_name, 'song_duration(s)': song_duration, 'song_url': song_url,
         'song_popularity': song_popularity, 'song_added': song_added, 'album_id': album_id, 'artist_id': artist_id})
        
    return song_df


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "spotify-etl-project-biswa"
    Key = "raw_data/to_processed/"
    
    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            
    for data in spotify_data:
        album_df = album(data)
        artist_df = artist(data)
        song_df = songs(data)

        album_df = album_df.drop_duplicates(subset=['album_id'])

        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        #Song Dataframe
        album_df['release_date'] = pd.to_datetime(album_df['release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        songs_key = "transformed_data/songs_data/songs_transformed_" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_key, Body=song_content)
        
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)
        
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])    
        s3_resource.Object(Bucket, key).delete()