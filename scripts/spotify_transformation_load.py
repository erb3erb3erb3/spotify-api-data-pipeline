import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO


def album(data):

    # Data is jsonObject, in dict format.
    # Parses out relevant album information and stores as a dict in album_list array
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']

        # Create album_element as a dict containing Album information and append to the album_list array
        album_element = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date, 
                         'total_tracks': album_total_tracks, 'url': album_url}
        album_list.append(album_element)

    return album_list

def artist(data):

    # Data is jsonObject, in dict format.
    # Parses out relevant artist information and stores as a dict in artist_list array
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                # To handle case of multiple artists for a single track, iterates through each 'artists' value
                for artist in value['artists']:
                    artist_id = artist['id']
                    artist_name = artist['name']
                    artist_url = artist['eternal_urls']['spotify']
                    # Create artist_element as a dict containing Artist information and append to the artist_list array
                    artist_element = {'artist_id': artist_id, 'artist_name': artist_name, 
                                      'artist_url': artist_url}
                    artist_list.append(artist_element)
                    
    return artist_list

def song(data):
    
    # Data is jsonObject, in dict format.
    # Parses out relevant song information and stores as a dict in song_list array
    song_list = []
    for row in data['items']:
        #id name duration url popularity added album_id artist_id
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        # Stores only the first, Primary Artist ID
        # Could add multiple artist fields, and coalesce with NULL in future
        artist_id = row['track']['artists'][0]['id']

        # Create song_element as a dict containing Song information and append to the song_list array
        song_element = {'song_id': song_id, 'song_name': song_name, 'duration_ms': song_duration, 'url': song_url, 'popularity': song_popularity,
                        'song_added': song_added, 'album_id': album_id, 'artist_id': artist_id}
        song_list.append(song_element)
    
    return song_list


def lambda_handler(event, context):
    # Initiate boto3 to talk with S3
    # Define Bucket and Key we are working with
    s3 = boto3.client('s3')
    Bucket=os.environ.get('gen-bucket')
    Key=os.environ.get('gen-key')
    spotify_data = []
    spotify_keys = []

    # Loop through files in the s3 bucket to be read.
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']

        # Check if it is a json file. Reads the file if it is. 
        # Uses json.loads to read json and store it as a dict inside the spotify_data array
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)

    for data in spotify_data:
        # Calls functions to extract respective data from our jsonObject(s)
        album_list = album(data)
        artist_list = artist(data)
        song_list = song(data)

        # Create Album, Artist, and Song Dataframes and drops respective duplicates.
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])

        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        song_df = pd.DataFrame.from_dict(song_list)

        album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
        song_df['song_added'] = pd.to_datetime(song_df['song_added'], errors='coerce')

        ## album buffer is a stringIO object used to temporarily store the csv file instead of saving it locally
        album_key = os.environ.get('album-key-pfx') + str(datetime.now()) + '.csv'
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, sep='|', index = False, escapechar='\\')
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)


        artist_key = os.environ.get('artist-key-pfx') + str(datetime.now()) + '.csv'
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer, sep='|', index = False, escapechar='\\')
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)


        song_key = os.environ.get('song-key-pfx') + str(datetime.now()) + '.csv'
        song_buffer=StringIO()
        song_df.to_csv(song_buffer, sep='|', index = False, escapechar='\\')
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content)

    # Uses s3 resource to clean up S3 folders
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        # key is like 'raw_data/to_processed/filename.file splitting by / and taking last is how to get just filename
        s3_resource.meta.client.copy(copy_source, Bucket, os.environ.get('fin-bucket') + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
