import json
import psycopg2
import os
from googletrans import Translator
import emoji

import config

DB = psycopg2.connect(options=f'-c search_path={config.options}', database=config.database, user=config.user,
                      password=config.password, host=config.host, port=config.port)
MY_CURSOR = DB.cursor()

trans = Translator()


def parse():
    os.system(
        f'instagram-scraper {config.instagram_target} -u {config.instagram_user} -p {config.instagram_password} --comments')


def read_json():
    with open('json/za_dzhumabekova\\za_dzhumabekova.json', 'r', encoding='utf-8') as f:
        text = json.load(f)

    comments = []
    posts = []

    for txt in text['GraphImages']:
        d = txt['comments']

        post_id = txt['id']
        display_url = txt['display_url']
        edge_media_to_caption = txt['edge_media_to_caption']
        edges = edge_media_to_caption['edges']
        desc_post = edges[0]

        post = ({
            'id': post_id,
            'description': desc_post['node']['text'],
            'display_url': display_url
        })
        posts.append(post)

        for i in d['data']:
            comment = ({'id': i['id'],
                        'post_id': post_id,
                        'owner_id': i['owner']['id'],
                        'username': i['owner']['username'],
                        'comment_text': i['text'],
                        'created_at': i['created_at'],
                        })
            comments.append(comment)
    return posts, comments


def save_posts(posts):
    for post in posts:
        try:
            sql_formula = f'INSERT INTO post(id, description, display_url) VALUES (%s,%s,%s) ON CONFLICT (id) DO NOTHING'
            content = (
                [post['id'], post['description'], post['display_url']])

            MY_CURSOR.execute(sql_formula, content)
        except:
            pass
    DB.commit()


def save_comments(comments):
    for i in comments:
        try:
            sql_formula = f'INSERT INTO comment( id ,post_id, owner_id, username, comment_text, created_at) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING'

            content = ([i['id'], i['post_id'], i['owner_id'], i['username'], i['comment_text'], i['created_at']])
            MY_CURSOR.execute(sql_formula, content)

        except:
            pass
        DB.commit()


def find_deleted_messages(items):
    MY_CURSOR.execute(f"UPDATE comment SET deleted=TRUE")
    for i in items:
        MY_CURSOR.execute(f"UPDATE comment SET deleted=FALSE where id='{i['id']}'")
        DB.commit()


def translate():
    MY_CURSOR.execute('select * from post where description_ru is null or description_en is null')
    posts = MY_CURSOR.fetchall()
    for ps in posts:
        strin = ps[1]
        no_emoji = emoji.get_emoji_regexp().sub(u'', strin)
        description_ru = trans.translate(no_emoji, dest='ru')
        description_en = trans.translate(no_emoji, dest='en')
        dosca = description_en.text.replace("'", '"')
        MY_CURSOR.execute(
            f"update post set description_ru='{description_ru.text}', description_en='{dosca}' where id={ps[0]}")
        DB.commit()

    MY_CURSOR.execute('select * from comment where comment_text_ru is null or comment_text_en is null')
    coments = MY_CURSOR.fetchall()
    for com in coments:
        strin = com[4]
        no_emoji = emoji.get_emoji_regexp().sub(u'', strin)
        comment_text_ru = trans.translate(no_emoji, dest='ru')
        comment_text_en = trans.translate(no_emoji, dest='en')
        dosca = comment_text_en.text.replace("'", '"')
        MY_CURSOR.execute(
            f"update comment set comment_text_ru='{comment_text_ru.text}', comment_text_en='{dosca}' where id={com[0]}")
        DB.commit()


# parse()

read_json()

posts, comments = read_json()

MY_CURSOR.execute('create table if not exists post(id bigint not null constraint post_pk primary key, description text, display_url text, description_ru text default null, description_en text default null)')
save_posts(posts)

MY_CURSOR.execute('create table if not exists comment(id bigserial not null constraint comment_pkey primary key, post_id bigint not null constraint comment_post_id_fk references post, owner_id bigint not null, username varchar(30) not null, comment_text text not null, deleted boolean default false, created_at bigint not null, comment_text_ru text default null, comment_text_en text default null)')
save_comments(comments)

find_deleted_messages(comments)
translate()
