import json
import psycopg2
import os

import config


DB = psycopg2.connect(options=f'-c search_path={config.options}', database=config.database, user=config.user, password=config.password, host=config.host, port=config.port)
MY_CURSOR = DB.cursor()

def parse():
    os.system(f'instagram-scraper {config.instagram_target} -u {config.instagram_user} -p {config.instagram_password} --comments')

def read_json():
    with open(config.path_to_json, 'r', encoding='utf-8') as f:
        text = json.load(f)

    comments = []
    posts = []

    for txt in text['GraphImages']:
        d = txt['comments']

        post_id =  txt['id']
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
            content = ([post['id'], post['description'], post['display_url'] ])

            MY_CURSOR.execute(sql_formula, content)
        except:
            pass
    DB.commit()

def save_comments(comments):
    for i in comments:
        try:
            sql_formula = f'INSERT INTO comment( id ,post_id, owner_id, username, comment_text, created_at ) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING'

            content = ([i['id'], i['post_id'], i['owner_id'], i['username'], i['comment_text'], i['created_at'] ])
            MY_CURSOR.execute(sql_formula, content)

        except:
            pass
        DB.commit()

def find_deleted_messages(items):
    MY_CURSOR.execute(f"UPDATE comment SET deleted=TRUE")
    for i in items:
        MY_CURSOR.execute(f"UPDATE comment SET deleted=FALSE where id='{i['id']}'")
        DB.commit()

# parse()

# posts, comments = read_json()

MY_CURSOR.execute('create table if not exists post(id bigint not null constraint post_pk primary key, description text, display_url text)')
# save_posts(posts)
MY_CURSOR.execute('create table if not exists comment(id bigserial not null constraint comment_pkey primary key, post_id bigint not null constraint comment_post_id_fk references post, owner_id bigint not null, username varchar(30) not null, comment_text text not null, deleted boolean default false, created_at bigint not null)')
# save_comments(comments)
# find_deleted_messages(comments)
