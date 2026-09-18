import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
#from dotenv import load_dotenv

#load_dotenv('/Users/axel/Documents/Portafolio/Spotify_api/env_var/varaibles_credential.env')

def conection_spotify():
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=os.getenv('SPOTIFY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIFY_CLIENT_SECRET')
    ))
    return sp
