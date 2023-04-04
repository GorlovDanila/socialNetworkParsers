import vk
import configparser
import psycopg2
from psycopg2 import OperationalError

VERSION = 5.131
S = 1_110_564
CPV = 0.02754


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


config = configparser.ConfigParser()
config.read("config.ini")

db_name = config["db"]["name"]
db_user = config["db"]["user"]
user_password = config["db"]["password"]
db_host = config["db"]["host"]
db_port = config["db"]["port"]

client_id = int(config["vk"]["client_id"])
group_id = int(config["vk"]["group_id"])
access_token = config["vk"]["access_token"]

connection = create_connection(db_name, db_user, user_password, db_host, db_port)
vk_api = vk.API(access_token=access_token)
for i in range(0, 4 + 1):
    result = vk_api.wall.get(
        owner_id=-group_id,
        count=100 if i < 4 else 27,
        offset=i * 100,
        v=VERSION
    )  # вот именно тут мы получаем нужное кол-во постов
    for post in result["items"]:
        post_comments_count = post["comments"]["count"]
        post_reposts_count = post["reposts"]["count"]
        post_likes_count = post["likes"]["count"]
        post_content = post["text"]
        post_attachment = len(post["attachments"]) > 0

        # Да, тут тоже крашнулось
        post_content = post_content.replace("'", "")
        post_content = post_content.replace('"', '')

        # Формируем query для вставки в таблицу с постом
        post_query = """INSERT INTO social_network_post (attachment, content, likes, comments, reposts) VALUES 
        ({}, '{}', {}, {}, {}) returning id;
        """.format(post_attachment, post_content, post_likes_count, post_comments_count, post_comments_count)
        # достаём айди
        post_db_id = insert_new_post(connect=connection, query=post_query)

        # блок обработки данных

        L = post_likes_count
        C = post_comments_count
        R = post_reposts_count

        Tc = 15 if post_attachment else 0
        Td = getTdFromLen(len(post_content))
        CT = Tc + Td

        Vp = (L - Tc) * 6 + C * 20
        ER = (Vp / S) * 100

        L0 = R * 10
        V0 = R * 60
        kU = (L + L0) / (Vp + V0)
        Reach = L + L0 + ((Vp + V0) - (L + L0)) * kU
        CP = Reach * CPV

        CP_MIN = CP / CT

        data_query = """
        INSERT INTO data (post_id, ct, er, cp, cp_min) VALUES 
        ({}, {}, {}, {}, {})
        """.format(post_db_id, CT, ER, CP, CP_MIN)
        execute_query(connect=connection, query=data_query)
