import praw
import pandas as pd
import datetime
import random
import time
from fake_useragent import UserAgent

def user_login(client_id, client_secret,username,password,user_agent):
    # reddit api login
    reddit = praw.Reddit(client_id=client_id,
                         client_secret=client_secret,
                         username=username,
                         password=password,
                         user_agent=user_agent)
    return reddit

#column_names = ['id','subreddit', 'treatment', 'created_time', 'visited_time', 'like_count', 'comment_count']
#df = pd.DataFrame(columns = column_names)
#df.to_parquet('data_random.parquet')

while True:
    ua = UserAgent()
    reddit = user_login('7vlklG_rE5V-Al-Sk-eLiQ', 'b478Rm1-9HD8Ob7B_vVYRKAJhoqGLA','Asleep-Profile3743','lowenib105@sueshaw.com',ua.chrome)
    subreddit = reddit.subreddit('all')
    stream = subreddit.stream.submissions(skip_existing=True)
    now = start = datetime.datetime.today()
    
    with open('log', 'a') as log:
        log.write(f'{now} Starting\n')
        
        df = pd.read_parquet('data_random.parquet')
        seen = set(df.index)
        seen_ids = set()

        for idx, values in df.iterrows():
            try:
                if values[0] in seen_ids:
                    continue
                
                id, subreddit, treatment, created_time, _, _, _ = [value for value in values]
                now = datetime.datetime.today()
                
                if now - created_time < datetime.timedelta(7):
                    post = reddit.submission(id)
                    df.append(id, subreddit, treatment, created_time, now, post.ups, post.num_comments)
                    
                seen_ids.add(id)
                
            except Exception as e:
                df.to_parquet('data_random.parquet')
                log_msg = f'{now} {post.id} FAILED {repr(e)}\n'
                log.write(log_msg)
                print(log_msg)
        
        '''counter = 0
        
        for pid in stream: # This will run for 500 posts
            post = reddit.submission(pid)
            try:
                created, now = map(datetime.datetime.fromtimestamp, [post.created_utc, int(time.time())])

                if post.id in seen:
                    log.write(f'{now} Seen post {post.id}. Skipping\n')
                    continue
                    
                if post.score == 1 and post.num_comments == 0: # verify that it is indeed new, thanks to other bots
                    treatment = random.randint(0, 1)
                    if treatment:
                        post.upvote()
                    df.append([post.id, post.subreddit, treatment, created, now, post.ups, post.num_comments], ignore_index = True)
                    counter +=1
                    
                seen.add(post.id)
                    
            except Exception as e:
                df.to_parquet('data_random.parquet')
                log_msg = f'{now} {post.id} FAILED {repr(e)}\n'
                log.write(log_msg)
                print(log_msg)
            
            if counter == 501: # When all 500 draws are done, terminate
                log.write('DONE')
                break'''
                
    time_elapsed = datetime.datetime.today() - start
    time.sleep(86400 - time_elapsed.seconds) # sleep 1 day minus the elapsed time