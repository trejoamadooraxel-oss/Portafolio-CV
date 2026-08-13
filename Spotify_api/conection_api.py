import os
import time
import requests
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from dotenv import load_dotenv
from Models import Artist, Album, Track

load_dotenv('/Users/axel/Documents/Portafolio/Spztify_api/env_var/varaibles_credential.env')

def conection_spotify():
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=os.getenv('SPOTIFY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIFY_CLIENT_SECRET')
    ))
    return sp

def all_artist_by_token():

    auth = SpotifyClientCredentials(
        client_id=os.getenv('SPOTIFY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIFY_CLIENT_SECRET')
    )

    token = auth.get_access_token(as_dict=False)

    # Request directo a la API
    headers = {'Authorization': f'Bearer {token}'}
    artist_id = '4q3ewBCX7sLwd24euuV69X'  # Bad Bunny

    response = requests.get(
        f'https://api.spotify.com/v1/artists/{artist_id}',
        headers=headers
    )

    data = response.json()
    print(data)


def identificador_artistas(sp, name_artist):
    artistas = []

    # Prueba — buscar un artista
    artist = sp.search(q=name_artist, type='artist', limit=1)
    id_spotify = artist["artists"]["items"][0]["uri"]
    artist_full = sp.artist(id_spotify)

    # Imprime el json obtenido de "artista_full"
    #print(json.dumps(artist_full, indent=4, sort_keys=True))

    #Imprime lo almacenado en la lista-"artistas"
    name_artist = artist_full["name"]
    id_spotify = artist_full["id"]

    artistas.append({
        'nombre_artista':name_artist,
        'id_spotify': id_spotify
    })

    return id_spotify, artistas


def normalizar_fecha(fecha_str):
    try:
        # Fecha completa: "2003-06-15"
        if len(fecha_str) == 10:
            return datetime.strptime(fecha_str, "%Y-%m-%d").date()
        # Solo año y mes: "2003-06"
        elif len(fecha_str) == 7:
            return datetime.strptime(fecha_str, "%Y-%m").date()
        # Solo año: "2003"
        elif len(fecha_str) == 4:
            return datetime.strptime(fecha_str, "%Y").date()
        else:
            return None
    except:
        return None

async def list_albums(sp, id_artist, name_artist):
    all_albums = []
    dicc_abums = []

    results = sp.artist_albums(id_artist, album_type='album', country='MX', limit=10)
    #print(json.dumps(results, indent=4, sort_keys=True))
    all_albums.extend(results['items'])

    while results['next']:
        results = sp.next(results)
        all_albums.extend(results['items'])
        time.sleep(1)

    #Traemos el id que corresponde al artista:
    id_artista = await Artist.id_db(name_artist)
    time.sleep(1)

    for album in all_albums:
        dicc_abums.append({
            'nombre_album': album['name'],
            'id_spotify': album['id'],
            'fecha_lanzamiento': normalizar_fecha(album['release_date']),
            'num_canciones': album['total_tracks'],
            'id_artista': id_artista
        })

    return dicc_abums


async def list_tracks(sp, dicc_abums):
    dicc_track = []
    all_albums = [album["id_spotify"] for album in dicc_abums]

    for album_id in all_albums:

        # Traemos el id que corresponde al artista:
        id_album = await Album.id_db(album_id)
        time.sleep(1)

        tracks = sp.album_tracks(album_id)
        for track in tracks["items"]:
            dicc_track.append({
                'nombre_cancion': track['name'],
                'id_spotify': track['id'],
                'num_cancion': track['track_number'],
                'duracion': track['duration_ms'],
                'id_album': id_album
            })
        time.sleep(1)

    return dicc_track

def main():
    sp = conection_spotify()

    #Conexion mediante token
    #all_artist_by_token()

    id_artista, dic_artist = identificador_artistas(sp)

    dicc_abums = list_albums(sp, id_artista)

    tracsk = list_tracks(sp, dicc_abums)

    for song in tracsk:
        print(song)

if __name__ == "__main__":

    main()
