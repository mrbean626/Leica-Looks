import praw
import time
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# load environment variables from .env file
load_dotenv()

# retrieve client id, secret, and user agent
client_id = os.getenv("REDDIT_CLIENT_ID")
client_secret = os.getenv("REDDIT_CLIENT_SECRET")
user_agent = os.getenv("REDDIT_AGENT")

# set up with praw creds
reddit = praw.Reddit(client_id=client_id,
                     client_secret=client_secret,
                     user_agent=user_agent)

# surf "Leica" subreddit
subreddit = reddit.subreddit('Leica')

# get unix timestamp
def get_post_date(created_utc):
    return datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d %H:%M:%S')

# scrape comments from each submission
def get_comments(submission):
    submission.comments.replace_more(limit=None) 
    comments_data = []
    
    for comment in submission.comments.list():
        comments_data.append({
            'comment_id': comment.id,
            'comment_author': str(comment.author) if comment.author else '[deleted]',  # check for deleted accounts
            'comment_body': comment.body,
            'comment_score': comment.score,
            'comment_created_utc': comment.created_utc,
            'comment_created_date': get_post_date(comment.created_utc),
            'comment_upvote_ratio': comment.upvote_ratio if hasattr(comment, 'upvote_ratio') else None,
            'comment_awards': comment.total_awards_received
        })
    
    return comments_data

# scrape posts and get their comments
def scrape_leica_subreddit(limit=None):
    data = []  # storing posts and details
    
    for submission in subreddit.hot(limit=limit):  
        print(f"Scraping submission: {submission.title}")
        
        # author is deleted 
        author = submission.author
        if author:
            author_name = str(author)

        else:
            author_name = '[deleted]'
            author_comment_karma = None
            author_link_karma = None

        # store posts into dictionary
        post_data = {
            'post_id': submission.id,
            'title': submission.title,
            'post_body': submission.selftext,
            'score': submission.score,
            'upvote_ratio': submission.upvote_ratio,
            'url': submission.url,
            'author': author_name,
            'created_utc': submission.created_utc,
            'created_date': get_post_date(submission.created_utc),
            'num_comments': submission.num_comments,
            'post_flair': submission.link_flair_text,
            'is_moderator_post': submission.distinguished == 'moderator',  
            'is_edited': submission.edited if submission.edited else False,  
            'awards': submission.total_awards_received,
            'gilded': submission.gilded,
            'crosspost_count': len(submission.crosspost_parent_list) if hasattr(submission, 'crosspost_parent_list') else 0,  
            'comments': get_comments(submission)
        }
        data.append(post_data)
    
    # convert dictionary information into dataframe
    df = pd.DataFrame(data)
    return df

df = scrape_leica_subreddit()

# expand comments into separate dataframe
comments_data = []
for index, row in df.iterrows():
    for comment in row['comments']:
        comment['post_id'] = row['post_id']  # reference to parent post
        comments_data.append(comment)

comments_df = pd.DataFrame(comments_data)

df = df.drop(columns=['comments'])

df.to_csv('leica_subreddit_posts.csv', index=False)
comments_df.to_csv('leica_subreddit_comments.csv', index=False)
