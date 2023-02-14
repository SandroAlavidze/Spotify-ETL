import json
import pandas as pd
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

default_args = {'owner': 'sandroalavidze',
                'start_date': dt.datetime(2022, 11, 26),
                'retries': 1,
                'retry_delay': dt.timedelta(minutes=1)
                }

# Initialize spotipy.Spotify object
cid = "CID"
secret = "SECRET"
client_credentials_manager = SpotifyClientCredentials(client_id=cid,
                                                      client_secret=secret)
sp = spotipy.Spotify(auth_manager=client_credentials_manager)


def get_track_data(ti) -> None:  # Fetch this year's tracks and save results in dataframe
    artist_name = []
    track_name = []
    track_popularity = []
    artist_id = []
    track_id = []
    for i in range(0, 1000, 50):
        track_results = sp.search(
            q='year:2022', type='track', limit=50, offset=i)
        for i, t in enumerate(track_results['tracks']['items']):
            artist_name.append(t['artists'][0]['name'])
            artist_id.append(t['artists'][0]['id'])
            track_name.append(t['name'])
            track_id.append(t['id'])
            track_popularity.append(t['popularity'])
    track_df = pd.DataFrame({'artist_name': artist_name, 'track_name': track_name,
                            'track_id': track_id, 'track_popularity': track_popularity, 'artist_id': artist_id})
    ti.xcom_push(key="spotify_track_dataframe",
                 value=track_df.to_json(orient='records'))


# Fetch the corresponding artist data and join with track table
def get_artist_data(ti) -> None:
    df = ti.xcom_pull(key="spotify_track_dataframe", task_ids='get_track_data')
    if not df:
        raise ValueError('Cant find anything')

    df_converted = pd.read_json(df)
    artist_popularity = []
    artist_genres = []
    artist_followers = []
    for a_id in df_converted.artist_id:
        artist = sp.artist(a_id)
        artist_popularity.append(artist['popularity'])
        artist_genres.append(artist['genres'])
        artist_followers.append(artist['followers']['total'])
    track_df = df_converted.assign(artist_popularity=artist_popularity,
                                   artist_genres=artist_genres, artist_followers=artist_followers)
    ti.xcom_push(key="spotify_track_and_artist",
                 value=track_df.to_json(orient='records'))


# Fetch the numerical information of the tracks and join with the rest of the data creating final table
def numCharac(ti) -> None:
    df = ti.xcom_pull(key="spotify_track_and_artist",
                      task_ids='get_artist_data')
    if not df:
        raise ValueError('Cant find anything')

    df_converted = pd.read_json(df)
    track_features = []
    for t_id in df_converted['track_id']:
        af = sp.audio_features(t_id)
        track_features.append(af)
    tf_df = pd.DataFrame(columns=['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness',
                         'liveness', 'valence', 'tempo', 'type', 'id', 'uri', 'track_href', 'analysis_url', 'duration_ms', 'time_signature'])
    for item in track_features:
        for feat in item:
            tf_df = tf_df.append(feat, ignore_index=True)
    data_final = pd.merge(df_converted, tf_df,
                          left_on="track_id", right_on='id')
    ti.xcom_push(key="spotify_track_artist_and_info",
                 value=data_final.to_json(orient='records'))


def upload_to_s3(ti) -> None:  # upload data to the S3 bucket
    df = ti.xcom_pull(key="spotify_track_artist_and_info",
                      task_ids='get_numerical_charact')
    if not df:
        raise ValueError('Cant find anything')

    df_converted = pd.read_json(df)
    df_converted.to_csv('data_spotify.csv')
    hook = S3Hook('s3_conn')
    hook.load_file(filename='data_spotify.csv', key='data_spotify.csv',
                   bucket_name='airflow-spotify-bucket', replace=True)


with DAG(
    dag_id='spotify_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False


) as dag:

    task_get_track_data = PythonOperator(
        task_id='get_track_data',
        python_callable=get_track_data,
        do_xcom_push=True
    )

    task_get_artist_data = PythonOperator(
        task_id='get_artist_data',
        python_callable=get_artist_data,
        do_xcom_push=True
    )

    task_get_numerical_characteristics = PythonOperator(
        task_id='get_numerical_charact',
        python_callable=numCharac
    )

    task_upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

task_get_track_data >> task_get_artist_data >> task_get_numerical_characteristics >> task_upload_to_s3
