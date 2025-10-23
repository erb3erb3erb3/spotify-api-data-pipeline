import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime





def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(auth_manager=auth_manager)
    playlist_uri = "37i9dQZEVXcMQVfsQdEF9U"

    spotify_data = sp.playlist_tracks(playlist_uri)


    client = boto3.client('s3')

    filename = 'spotify_raw_' + str(datetime.now()) + '.json'

    ## places json file into s3
    client.put_object(
        Bucket='spotify-etl-project-erberberb',
        Key='raw_data/to_processed/'+filename,
        Body=json.dumps(spotify_data)

    )
