import psycopg2
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from psycopg2 import OperationalError

import urllib.parse as p
import os
import pickle
import emoji

# Взаимодействие с Ютуб Апи происходит через области.
# Мы берём глобальную, чтобы взаимодействовать со всем Апи без траблов
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]


def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def youtube_authenticate():
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_service_name = "youtube"
    api_version = "v3"
    # Файл с данными для авторизации, создаётся заранее на сайте Гугл Апи
    client_secrets_file = "credentials.json"
    creds = None
    # Файл token.pickle хранит токен доступа пользователя и создаётся автоматом при первой авторизации
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # Если нет действительных учётных данных, пробуем войти в систему с данными из credentionals.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
            creds = flow.run_local_server(port=0)
        # Сохраняем учетные данные для следующего запуска
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    return build(api_service_name, api_version, credentials=creds)


def get_video_details(youtube, **kwargs):
    return youtube.videos().list(
        part="snippet,contentDetails,statistics",
        **kwargs
    ).execute()


def write_video_info(connection, video_response, publish_time):
    items = video_response.get("items")[0]
    # Получаем фрагмент, статистику и подробную информацию о контенте из ответа
    snippet = items["snippet"]
    statistics = items["statistics"]
    content_details = items["contentDetails"]
    # Получаем данные из фрагмента
    channel_title = snippet["channelTitle"]
    title = snippet["title"]
    description = snippet["description"]
    # Получаем параметры видео
    comment_count = statistics["commentCount"]
    like_count = statistics["likeCount"]
    dislike_count = 0
    view_count = statistics["viewCount"]
    duration = content_details["duration"]
    duration = str(duration).replace("PT", "")

    if "H" in duration:
        hours = duration.split("H")[0]
        minutes = duration.split("H")[1].split("M")[0]
    else:
        hours = 0
        minutes = duration.split("M")[0]

    if "M" in duration:
        seconds = duration.split("M")[1].replace("S", "")
    else:
        seconds = duration.split("S")[0]

    duration_in_seconds = int(hours) * 60 * 60 + int(minutes) * 60 + int(seconds)
    ct = calculating_content_timing(duration_in_seconds, len(str(description)))
    er = calculating_engagement_rate(view_count, channel_subscriber_count)
    cp = calculating_post_profit(view_count, like_count, dislike_count, duration_in_seconds, comment_count)
    cp_min = calculating_post_profit_min(cp, ct)

    insert_query = """INSERT INTO social_network_post
            (number_of_views, date_of_publication, number_of_comments, type_of_sn, content, CT, ER, CP, CP_MIN)
            values
            ({}, '{}', {}, 'YouTube', '{}', {}, {}, {}, {}) RETURNING id;
            """.format(view_count, publish_time, comment_count, title + "\n" + description, ct, er, cp, cp_min)

    id_video = 0
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(insert_query)
        # айдишник только что вставленного поста
        id_video = cursor.fetchone()[0]
    except OperationalError as e:
        print(f"The error '{e}' occurred")

    insert_query = (
        """INSERT INTO reaction(id_of_post, type_of_emotion, number_of_emotions) VALUES ({}, 'U+1F44D', {})""".format(id_video,
                                                                                                              like_count)
    )

    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(insert_query)
    except OperationalError as e:
        print(f"The error '{e}' occurred")

    print(emoji.emojize(f"""\
    Title: {title}
    Description: {description}
    Channel Title: {channel_title}
    Publish time: {publish_time}
    Duration: {duration}
    Duration_sec: {duration_in_seconds}
    Number of comments: {comment_count}
    Number of likes: {like_count}
    Number of views: {view_count}
    """, language='alias'))

    return id_video


def search(youtube, **kwargs):
    return youtube.search().list(
        part="snippet",
        **kwargs
    ).execute()


def parse_channel_url(url):
    """
     Эта функция принимает URL канала, чтобы проверить, содержит ли он
     идентификатор канала, идентификатор пользователя или название канала
    """
    path = p.urlparse(url).path
    id = path.split("/")[-1]
    if "/c/" in path:
        return "c", id
    elif "/channel/" in path:
        return "channel", id
    elif "/user/" in path:
        return "user", id


def get_channel_id_by_url(youtube, url):
    # Возвращает идентификатор канала заданного `id` и `method`
    method, id = parse_channel_url(url)
    if method == "channel":
        return id
    raise Exception(f"Cannot find ID:{id} with {method} method")


def get_channel_videos(youtube, **kwargs):
    return youtube.search().list(
        **kwargs
    ).execute()


def get_channel_details(youtube, **kwargs):
    return youtube.channels().list(
        part="statistics,snippet,contentDetails",
        **kwargs
    ).execute()


def get_comments(youtube, **kwargs):
    return youtube.commentThreads().list(
        part="snippet",
        **kwargs
    ).execute()


def write_comment_info(comment_response, id_video):
    author = comment_response["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
    author = str(author).replace("'", '').replace('"', '')
    comment_text = comment_response["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
    comment_text = str(comment_text).replace("'", '').replace('"', '')
    updated_at = comment_response["snippet"]["topLevelComment"]["snippet"]["updatedAt"]
    updated_at = str(updated_at).split("T")[0]
    like_count = comment_response["snippet"]["topLevelComment"]["snippet"]["likeCount"]

    insert_query = (
        """INSERT INTO comment(id_of_post, time_of_comment, author, content) VALUES ({}, '{}', '{}', '{}')""".format(
            id_video, updated_at, author, comment_text)
    )

    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(insert_query)
    except OperationalError as e:
        print(f"The error '{e}' occurred")

    print(emoji.emojize(f"""\
                Author: {author}
                Comment: {comment_text}
                Likes: {like_count}
                Updated At: {updated_at}\
                """, language='alias'))


def calculating_content_timing(video_length, description_length):
    description_timing = 0

    match description_length:
        case 1 if description_length < 90:
            description_timing = 6
        case 2 if 90 <= description_length <= 458:
            description_timing = 30
        case 3 if 459 <= description_length <= 900:
            description_timing = 60
        case 4 if 901 <= description_length <= 1800:
            description_timing = 120
        case 5 if 1801 <= description_length <= 2700:
            description_timing = 180
        case 6 if 2701 <= description_length <= 3200:
            description_timing = 240
        case 7 if 3201 <= description_length <= 4500:
            description_timing = 300
        case 8 if 4501 <= description_length <= 9000:
            description_timing = 600
        case 9 if 9001 <= description_length <= 18000:
            description_timing = 1200
        case 10 if 18001 <= description_length <= 27000:
            description_timing = 1800
        case 11 if 27001 <= description_length <= 32000:
            description_timing = 2400
        case 12 if 32001 <= description_length <= 45000:
            description_timing = 3000
        case 13 if 45001 <= description_length <= 67500:
            description_timing = 4500
        case 14 if 67501 <= description_length <= 81000:
            description_timing = 5400
        case 15 if description_length > 81000:
            description_timing = 5401

    return video_length + description_timing


def calculating_engagement_rate(number_post_views, number_subscribers):
    return int(number_post_views) / int(number_subscribers) * 100


def calculating_post_profit(number_post_views, likes_count, dislikes_count, video_length, comments_count):
    reach1 = int(number_post_views)
    ku = (int(likes_count) + int(dislikes_count)) / int(number_post_views)
    t1 = 5

    if int(video_length) > int(comments_count):
        tc = comments_count
    else:
        tc = video_length

    t2 = video_length + tc
    cps = 0.00045
    ke = calculating_audience_attention_level(video_length)
    reach2 = (int(likes_count) + int(dislikes_count)) + (
                int(number_post_views) - (int(likes_count) + int(dislikes_count))) * ku

    return reach1 * t1 * cps + reach2 * (t2 - t1) * ke * cps


def calculating_audience_attention_level(video_length):
    level = 0.0

    match video_length:
        case 1 if video_length < 10:
            level = 0.95
        case 2 if 11 <= video_length <= 30:
            level = 0.85
        case 3 if 31 <= video_length <= 60:
            level = 0.75
        case 4 if 61 <= video_length <= 120:
            level = 0.7
        case 5 if 121 <= video_length <= 180:
            level = 0.65
        case 6 if 181 <= video_length <= 240:
            level = 0.62
        case 7 if 241 <= video_length <= 300:
            level = 0.6
        case 8 if 301 <= video_length <= 600:
            level = 0.55
        case 9 if 601 <= video_length <= 1200:
            level = 0.47
        case 10 if 1201 <= video_length <= 1800:
            level = 0.38
        case 11 if 1801 <= video_length <= 2400:
            level = 0.3
        case 12 if 2401 <= video_length <= 3000:
            level = 0.28
        case 13 if 3001 <= video_length <= 4500:
            level = 0.27
        case 14 if 4501 <= video_length <= 5400:
            level = 0.25
        case 15 if 5401 <= video_length <= 7200:
            level = 0.2
        case 16 if 7201 <= video_length <= 9000:
            level = 0.17
        case 17 if 9001 <= video_length <= 10800:
            level = 0.15
        case 18 if 10801 <= video_length <= 12600:
            level = 0.13
        case 19 if 12601 <= video_length <= 14400:
            level = 0.11
        case 20 if 14401 <= video_length <= 16200:
            level = 0.08
        case 21 if 16201 <= video_length <= 18000:
            level = 0.05
        case 22 if 18001 <= video_length <= 19800:
            level = 0.02
        case 23 if video_length > 19801:
            level = 0.01

    return level


def calculating_post_profit_min(post_profit, content_timing):
    return round(post_profit / content_timing, 2)


connection = create_connection(
    "youtube_data", "postgres", "17031703", "127.0.0.1", "5432"
)

insert_query = (
    f"INSERT INTO type_reaction (code) VALUES ('U+1F44D')"
)

connection.autocommit = True
cursor = connection.cursor()
cursor.execute(insert_query)

# Аутентификация с помощью YouTube API
youtube = youtube_authenticate()

channel_url = "https://www.youtube.com/channel/UCwipTluVS2mjuhPtx2WU7eQ"
# Получаем id канала из URL
channel_id = get_channel_id_by_url(youtube, channel_url)
# Получаем статистику канала
response = get_channel_details(youtube, id=channel_id)
# Достаём инфу из ответа
snippet = response["items"][0]["snippet"]
statistics = response["items"][0]["statistics"]
channel_country = snippet["country"]
channel_description = snippet["description"]
channel_creation_date = snippet["publishedAt"]
channel_title = snippet["title"]
channel_subscriber_count = statistics["subscriberCount"]
channel_video_count = statistics["videoCount"]
channel_view_count = statistics["viewCount"]
print(f"""
Title: {channel_title}
Published At: {channel_creation_date}
Description: {channel_description}
Country: {channel_country}
Number of videos: {channel_video_count}
Number of subscribers: {channel_subscriber_count}
Total views: {channel_view_count}
""")
# Указываем количество страниц, по которым хотим пройтись.
# У Масленникова всего 33 видоса за 2 года,
# с одной страницы можем максимум 50 видосов получить,
# поэтому указываем 1
n_pages = 1
# Номер текущего видео по счёту
n_videos = 0
# Для перехода между страницами используем токены,
# их берём из ответа, если следующей страницы с контентом нет,
# то и токена тоже не будет и наоборот
next_page_video_token = None
next_page_comment_token = None
# Ютуб прикалывается периодически и отправляет дубликаты видосов в ответе,
# поэтому храним id всех спарсенных видосов на данный момент
video_ids = []
for i in range(n_pages):
    params = {
        'part': 'snippet',
        # Querry параметры
        'q': '',
        'channelId': channel_id,
        'type': 'video',
        # Количество видосов на одной странице
        'maxResults': 50,
        'publishedAfter': '2022-01-01T00:00:00Z'
    }
    # Заменяем токен для следующей итерации
    if next_page_video_token:
        params['pageToken'] = next_page_video_token
    res = get_channel_videos(youtube, **params)
    channel_videos = res.get("items")
    for video in channel_videos:
        n_videos += 1
        video_id = video["id"]["videoId"]
        # Проверка на уникальность
        if video_id not in video_ids:
            video_ids.insert(n_videos, video_id)
            # Создаём url текущего видоса для запроса инфы о нём
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            video_response = get_video_details(youtube, id=video_id)
            items = video_response.get("items")[0]
            snippet = items["snippet"]
            # Убираем из времени публикации время и часовой пояс
            publish_time = str(snippet["publishedAt"]).split("T")[0]
            if publish_time.startswith("2023") or publish_time.startswith("2022"):
                id_video_db = write_video_info(connection, video_response, publish_time)
                # Параметры для запроса комментариев
                parameters = {
                    'videoId': video_id,
                    'maxResults': 100,
                    # Сортировка по популярности
                    'order': 'relevance',
                }
                resp = get_comments(youtube, **parameters)
                items_comments = resp.get("items")
                # Если комментов нет, завершаем цикл
                if not items_comments:
                    break
                for item in items_comments:
                    write_comment_info(item, id_video_db)
                    print(100 * "*")
                if "nextPageToken" in resp:
                    # Работает также, как с видосами
                    params["pageToken"] = resp["nextPageToken"]
                else:
                    # Если нет следующей страницы, прерываем цикл
                    break
                print("*" * 70)
    print("*" * 100)
    # Проверка на существование следующей страницы
    if "nextPageToken" in res:
        next_page_video_token = res["nextPageToken"]
