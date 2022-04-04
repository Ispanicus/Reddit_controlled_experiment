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
reddit2 = user_login('BnFIdn_A-SCKnctB7PuGJQ', 'R9LIgBr79-4ZesWdSfe3bZuib1ab1w', 'Pale_Fix9773', 'xabelos111@minimeq.com', ua.chrome)
reddit3 = user_login('aig1SpiMkS9uyKq390oP6g', '8pbxn7qVtrKCV7dTFJexjC0rZylgDg', 'Character-Drama-2894', 'ddttuxx@eelraodo.com', ua.chrome)
reddit4 = user_login('OM_wRQVi4-RyCjS8li2O4A', 'N-SI4VOqUgMQD9ClmWX9cwrVNlvdpQ', 'AmountAccomplished18', 'assaf@eelraodo.com', ua.chrome)

accounts = [reddit, reddit2, reddit3, reddit4]

subreddit = reddit.subreddits.search_by_name('MildlyInteresting')[0]
stream = subreddit.stream.submissions()
now = last_save = datetime.datetime.today()

df = pd.read_parquet('data.parquet')
df = df.sort_values('saved_time')
if 'revisit_date' not in df.columns:
    df['revisit_date'] = None

for post_id, row in df.iterrows():
    for idx, account in enumerate(accounts):
        post = account.submission(post_id)
        df.loc[post_id, f'revisit_date {idx}'] = datetime.datetime.now()
        df.loc[post_id, f'end_like_count {idx}'] = post.ups
        df.loc[post_id, f'end_comments {idx}'] = post.num_comments
    print(post_id)
df.to_parquet('data.parquet')