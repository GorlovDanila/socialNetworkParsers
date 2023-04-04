import configparser

import psycopg2
from psycopg2 import OperationalError
from telethon import TelegramClient


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connect = None
    try:
        connect = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connect


def execute_query(connect, query):
    connect.autocommit = True
    cursor = connect.cursor()
    try:
        cursor.execute(query)
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def insert_new_post(connect, query):
    connect.autocommit = True
    cursor = connect.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchone()[0]  # айдишник только что вставленного поста
    except OperationalError as e:
        print(f"The error '{e}' occurred")


config = configparser.ConfigParser()
config.read("config.ini")

db_name = config["db"]["name"]
db_user = config["db"]["user"]
user_password = config["db"]["password"]
db_host = config["db"]["host"]
db_port = config["db"]["port"]

connection = create_connection(db_name, db_user, user_password, db_host, db_port)

phone = config["telegram"]["phone"]
api_id = config["telegram"]["api_id"]
api_hash = config["telegram"]["api_hash"]

channel_url = config["telegram"]["channel_url"]

my_client = TelegramClient(phone, api_id, api_hash)
my_client.start()

channel_subscriber_count = 1203060


# метод для проверки, есть ли уже такая реакция в таблице
def check_reaction(connect, code_reaction):
    connect.autocommit = True
    cursor = connect.cursor()
    try:
        cursor.execute(f"SELECT * FROM type_reaction WHERE code = '{code_reaction}';")
        select = cursor.fetchone()
        if select is None:
            return False
        else:
            return True
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def getTdFromLen(length) -> int:
    if length < 90:
        return 6
    if 90 <= length <= 458:
        return 30
    if 459 <= length <= 900:
        return 60
    if 901 <= length <= 1800:
        return 120
    if 1801 <= length <= 2700:
        return 180
    if 2701 <= length <= 3200:
        return 240
    if 3201 <= length <= 4500:
        return 300
    if 4501 <= length <= 9000:
        return 600
    if 9001 <= length <= 18000:
        return 1200
    if 18001 <= length <= 27000:
        return 1800
    if 27001 <= length <= 32000:
        return 2400
    if 32001 <= length <= 45000:
        return 3000
    if 45001 <= length <= 67500:
        return 4500
    if 67501 <= length <= 81000:
        return 5400
    else:
        return 5400


def calculating_engagement_rate(number_post_views, number_subscribers):
    if number_post_views is None:
        return 0
    return number_post_views / number_subscribers * 100


def calculating_post_profit_min(post_profit, content_timing):
    return round(post_profit / content_timing, 2)


# проходимся по всем сообщениям в канале, reverse=False означает, что сначала будут новые
for tg_post in my_client.iter_messages(channel_url, reverse=False):

    post_content = tg_post.message  # сам текст поста
    post_views = tg_post.views  # просмотры
    forwards = tg_post.forwards
    answers = tg_post.replies
    media = tg_post.media
    reactions = tg_post.reactions

    count_emoticons = 0


    # если к посту есть какие-то реакции, то циклом проходимся по всем
    if reactions is not None:
        for reaction in reactions.results:
            count_emoticons += reaction.count

    # количество комментариев
    if answers is not None:
        count_comments = answers.replies
    else:
        count_comments = 0

    if post_content is not None or media is not None:

        media_in_seconds = 0

        if media is not None:
            media_in_seconds = 5
            attachment = True
        else:
            attachment = False

        if post_content is not None:
            length_post = len(post_content)
        else:
            length_post = 0

        post_content = post_content.replace("'", "")
        post_content = post_content.replace('"', '')

        # сделала такую проверку, потому что бывает, что сохраняются пустые посты какие-то и без медиа, и без текста
        # если такие "мертвые" посты, то у них всегда нет реакций, поэтому пришлось такую проверку сделать
        if count_emoticons != 0:

            # Формируем query для вставки в таблицу с постом
            post_query = """INSERT INTO social_network_post (attachment, content, likes, comments, reposts) VALUES
            ({}, '{}', {}, {}, {}) returning id;
            """.format(attachment, post_content, count_emoticons, count_comments, forwards)
            # достаём айди
            post_db_id = insert_new_post(connect=connection, query=post_query)

            ct = media_in_seconds + getTdFromLen(length_post)
            er = calculating_engagement_rate(post_views, channel_subscriber_count)
            cp = post_views * 0.0082
            cp_min = calculating_post_profit_min(cp, ct)

            print('ct-', ct, ', er-', er, ',cp-', cp, ',cp_min-', cp_min)

            data_query = """
            INSERT INTO data (post_id, ct, er, cp, cp_min) VALUES
            ({}, {}, {}, {}, {})
            """.format(post_db_id, ct, er, cp, cp_min)
            execute_query(connect=connection, query=data_query)
