import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import platform

def conection_spotify():

    if platform.system() == 'Darwin':
        from dotenv import load_dotenv

        load_dotenv('/Users/axel/Documents/Portafolio/Spotify_api/env_var/varaibles_credential.env')


        client = os.getenv('SPOTIFY_CLIENT_ID')
        secret = os.getenv('SPOTIFY_CLIENT_SECRET')

    else:

        client = os.getenv('SPOTIFY_CLIENT_ID')
        secret = os.getenv('SPOTIFY_CLIENT_SECRET')


    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=client,
        client_secret=secret
    ))
    return sp
