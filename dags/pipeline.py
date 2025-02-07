from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook 
import pendulum
import requests
import xmltodict
import os

@dag(
    dag_id = 'podcast_summary',
    schedule = '@daily',
    start_date = pendulum.datetime(2025, 1, 1),
    catchup=False,
)

def podcast_summary():

    create_database = SqliteOperator(
        task_id="create_database_sqlite",
        sqlite_conn_id="conn-podcast",
        sql="""
            CREATE TABLE IF NOT EXISTS episodes (
                link TEXT PRIMARY KEY,
                title TEXT,
                filename TEXT,
                date_published TEXT,
                description TEXT,
                transcript TEXT
                );
            """
    )

    @task()
    def get_episodes():
        url = "https://www.marketplace.org/feed/podcast/marketplace/"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()  # Check for HTTP errors.
            safe_text = resp.text.encode("utf-8").decode("utf-8")
            # safe_text = safe_text.replace("&", "&amp;")
            data_ep = xmltodict.parse(safe_text)
                        
            if "rss" in data_ep:
                episodes = data_ep["rss"]["channel"]["item"]
                print(f"{len(episodes)} episodes found.")
                return episodes
            else:
                print("Error: 'rss' key not found in response.")
                return []

        except Exception as err:
            print(f"An error occurred: {err}")
            return []


    @task()
    def load_episodes(episod_list):
        hook = SqliteHook(sqlite_conn_id="conn-podcast")
        existed = hook.get_pandas_df("select * from episodes;")
        new_episode = []
        for ep in episod_list:
            if ep["link"] not in existed["link"].values:
                filename = f"{ep["link"].split("/")[-1]}.mp3"
                new_episode.append([ep['link'], ep['title'], ep.get('pubDate', ''), \
                    ep.get('description', ''), ep.get('transcript', ''), filename])
        hook.insert_rows(table = 'episodes', rows = new_episode, \
                        target_fields = ['link', 'title', 'date_published', 'description', 'transcript', 'filename'])

    podcast_episodes = get_episodes()
    load_episodes(podcast_episodes)

    @task()
    def download_episodes(episodes):
        # Create directory if it doesn't exist
        os.makedirs("episodes", exist_ok=True)
        for ep in episodes:
            filename = f"{ep["link"].split("/")[-1]}.mp3"
            audio_path = os.path.join("episode", filename)
            if not os.path.exists(audio_path):
                print(f'Downloading {filename}')
                try:
                    audio_url = ep.get("enclosure", {}).get("@url")
                    if not audio_url:
                        print(f"No audio URL found for episode {filename}")
                        continue
                        
                    audio = requests.get(audio_url)
                    audio.raise_for_status()  # Check for HTTP errors
                    
                    with open(audio_path, 'wb') as f:
                        f.write(audio.content)  # Fixed typo: audio.connect -> audio.content
                        
                except Exception as e:
                    print(f"Failed to download {filename}: {e}")

    download_episodes(podcast_episodes)

    create_database.set_downstream(podcast_episodes)

podcast_summary()    
