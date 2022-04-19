import praw
import pandas as pd
import datetime
import random
import time

def det_date():
    return (datetime.now().date() -datetime(2022,4,18).date()).days

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

subreddit = reddit.subreddit('all')
stream = subreddit.stream.submissions(skip_existing=True)
now = datetime.datetime.today()
#column_names = ['id','subreddit', 'treatment', 'created_time', 'visited_time', 'like_count', 'comment_count']
#df = pd.DataFrame(columns = column_names)
#df.to_parquet('data_random.parquet')

with open('log', 'a') as log:
    log.write(f'{now} Starting\n')
    
    df = pd.read_parquet('data_random.parquet')
    seen = set(df.index)
    seen_ids = set()
    counter = 0
    
    for pid in stream: # This will run for 500 posts
        post = reddit.submission(pid)
        try:
            created, now = map(datetime.datetime.fromtimestamp, [post.created_utc, int(time.time())])

            if post.id in seen:
                log.write(f'{now} Seen post {post.id}. Skipping\n')
                continue
                
            if post.score == 1 and post.num_comments == 0: # verify that it is indeed new, thanks to other bots
                treatment = random.randint(0, 1)
                df.append([post.id, post.subreddit, treatment, created, now, post.ups, post.num_comments], ignore_index = True)

                if treatment:
                    post.upvote()

                seen.add(post.id)
                counter +=1
            
        except Exception as e:
            df.to_parquet('data_random.parquet')
            log_msg = f'{now} {post.id} FAILED {repr(e)}\n'
            log.write(log_msg)
            print(log_msg)
        
        if counter == 501: # When all 500 draws are done, terminate
            log.write('DONE')
            break
               
    
    for i in range(1,min(det_date()+1,8)):
        
        if det_date()-i > 0 and det_date()-i <= 7:
            log.write(f"Getting results for day {i}. This is the {det_date()-i} time")
            
            for idx, values in df.iterrows:
                try:
                    if values[0] in seen_ids:
                        continue
                    
                    id, subreddit, treatment, created_time, _, _, _ = [value for value in values]
                    post = reddit.submission(id)
                    now = datetime.datetime.today()
                    df.append(id, subreddit, treatment, created_time, now, post.ups, post.num_comments)
                    seen_ids.add(id)
                except Exception as e:
                    df.to_parquet('data_random.parquet')
                    log_msg = f'{now} {post.id} FAILED {repr(e)}\n'
                    log.write(log_msg)
                    print(log_msg)
                    