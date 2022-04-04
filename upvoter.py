import praw
import pandas as pd
import datetime
import random
import time

def user_login(client_id, client_secret,username,password,user_agent):
    # reddit api login
    reddit = praw.Reddit(client_id=client_id,
                         client_secret=client_secret,
                         username=username,
                         password=password,
                         user_agent=user_agent)
    return reddit
from fake_useragent import UserAgent
ua = UserAgent()
reddit = user_login('7vlklG_rE5V-Al-Sk-eLiQ', 'b478Rm1-9HD8Ob7B_vVYRKAJhoqGLA','Asleep-Profile3743','lowenib105@sueshaw.com',ua.chrome)

subreddit = reddit.subreddits.search_by_name('funny')[0]
stream = subreddit.stream.submissions()
now = last_save = datetime.datetime.today()


with open('log', 'a') as log:
    log.write(f'{now} Starting\n')
    
    df = pd.read_parquet('data.parquet')
    seen = set(df.index)
    
    for post in stream: # This will run indefinitely
        if now - datetime.timedelta(hours=4) > last_save:
            last_save = now
            df.to_parquet('data.parquet')
        try:
            created, now = map(datetime.datetime.fromtimestamp, [post.created_utc, int(time.time())])

            if post.id in seen:
                log.write(f'{now} Seen post {post.id}. Skipping\n')
                continue

            treatment = random.randint(0, 1)
            df.loc[post.id, ['treatment', 'created_time', 'saved_time', 'starting_like_count',
       'starting_comments']] = [treatment, created, now, post.ups, post.num_comments]

            if treatment:
                post.upvote()

            seen.add(post.id)
        except Exception as e:
            df.to_parquet('data.parquet')
            log_msg = f'{now} {post.id} FAILED {repr(e)}\n'
            log.write(log_msg)
            print(log_msg)
                
'''
df = pd.read_parquet('data.parquet')
df = df.sort_values('saved_time')
if 'revisit_date' not in df.columns:
    df['revisit_date'] = None

i = 0
while True:
    now = datetime.datetime.today()
    while df.iloc[i, :].created_time < (now - datetime.timedelta(days=7)):
        row = df.iloc[i, :]
        i += 1
        if not pd.isnull(row.revisit_date):
            continue # Skip to where we left off

        post_id = row.name
        print(post_id)
        post = reddit.submission(post_id)
        df.loc[post_id, 'revisit_date'] = datetime.datetime.now()
        df.loc[post_id, 'end_like_count'] = post.ups
        df.loc[post_id, 'end_comments'] = post.num_comments
    df.to_parquet('data.parquet')
    time.sleep(1*60*60) # 1 hour
'''
