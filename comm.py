import streamlit as st
import cloudscraper
import pandas as pd
from datetime import datetime
import time
import logging
import pytz
import random
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logging.basicConfig(level=logging.INFO)

class AdaptiveRateLimiter:
    def __init__(self, initial_rate=1, max_rate=2, backoff_factor=2, jitter=0.1):
        self.current_rate = initial_rate
        self.max_rate = max_rate
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.last_call = 0

    def wait(self):
        now = time.time()
        time_since_last_call = now - self.last_call
        wait_time = max(0, (1 / self.current_rate) - time_since_last_call)
        wait_time += random.uniform(0, self.jitter)
        if wait_time > 0:
            time.sleep(wait_time)
        self.last_call = time.time()

    def increase_rate(self):
        self.current_rate = min(self.current_rate * self.backoff_factor, self.max_rate)

    def decrease_rate(self):
        self.current_rate /= self.backoff_factor

rate_limiter = AdaptiveRateLimiter()
scraper = cloudscraper.create_scraper()

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    retry=retry_if_exception_type((cloudscraper.exceptions.CloudflareChallengeError, Exception)),
    reraise=True
)
def fetch_with_rate_limit(url):
    rate_limiter.wait()
    try:
        response = scraper.get(url)
        response.raise_for_status()
        rate_limiter.increase_rate()
        return response.json()
    except cloudscraper.exceptions.CloudflareChallengeError:
        st.error("Cloudflare challenge detected. Unable to bypass.")
        raise
    except Exception as e:
        if response.status_code == 429:
            rate_limiter.decrease_rate()
            st.warning(f"Rate limited. Adjusting rate and retrying...")
        else:
            st.error(f"Failed to fetch data: {str(e)}")
        raise

def fetch_all_user_posts(user_id):
    offset = 0
    batch_size = 500
    all_posts = []

    while True:
        url = f"https://api.moescape.ai/v1/users/{user_id}/posts?offset={offset}&limit={batch_size}"
        try:
            data = fetch_with_rate_limit(url)
            if not data:
                break
            
            all_posts.extend(data)
            
            if len(data) < batch_size:
                break
            
            offset += batch_size
        except Exception as e:
            st.error(f"Error fetching posts: {str(e)}")
            break

    return all_posts

@st.cache_data(ttl=3600)
def fetch_post_comments(post_uuid):
    url = f"https://api.moescape.ai/v1/posts/{post_uuid}/comments?offset=0&limit=500"
    data = fetch_with_rate_limit(url)
    if data:
        return data['comments']
    else:
        st.error(f"Failed to fetch comments for post {post_uuid}")
        return []

def utc_to_eest(utc_dt):
    utc_dt = datetime.fromisoformat(utc_dt.replace('Z', '+00:00'))
    eest = pytz.timezone('Europe/Helsinki')
    return utc_dt.replace(tzinfo=pytz.UTC).astimezone(eest)

def parse_comments(comments, post_uuid, post_title):
    parsed_comments = []
    for comment in comments:
        parsed_comment = {
            'name': comment['profile']['name'],
            'comment': comment['text'],
            'date': utc_to_eest(comment['created_at']).strftime('%Y-%m-%d %H:%M:%S %Z'),
            'likes': comment['likes'],
            'post_title': post_title,
            'post_link': f"https://moescape.ai/posts/{post_uuid}"
        }
        parsed_comments.append(parsed_comment)
        
        replies = comment.get('replies') or []
        for reply in replies:
            if reply:
                parsed_reply = {
                    'name': reply['profile']['name'],
                    'comment': f"↳ {reply['text']}",
                    'date': utc_to_eest(reply['created_at']).strftime('%Y-%m-%d %H:%M:%S %Z'),
                    'likes': reply['likes'],
                    'post_title': post_title,
                    'post_link': f"https://moescape.ai/posts/{post_uuid}"
                }
                parsed_comments.append(parsed_reply)
    
    return parsed_comments

st.title('Moescape User Posts and Comments')

user_id = st.text_input('Enter User ID')
num_posts = st.number_input('Number of posts to scan (max 2000)', min_value=1, max_value=2000, value=10)
sort_order = st.radio("Select posts to analyze:", ("Oldest", "Newest"))

if user_id and num_posts:
    posts_placeholder = st.empty()
    progress_bar = st.progress(0)
    all_comments = []
    
    posts_placeholder.write("Fetching all posts...")
    all_posts = fetch_all_user_posts(user_id)
    total_posts = len(all_posts)
    posts_placeholder.write(f"Found {total_posts} posts in total")
    
    all_posts.sort(key=lambda x: x.get('created_at', ''), reverse=(sort_order == "Newest"))
    
    posts_to_analyze = all_posts[:num_posts]
    
    st.write(f"Analyzing the {sort_order.lower()} {len(posts_to_analyze)} posts")
    
    comment_progress_bar = st.progress(0)
    
    for i, post in enumerate(posts_to_analyze):
        comments = fetch_post_comments(post['uuid'])
        parsed_comments = parse_comments(comments, post['uuid'], post['title'])
        all_comments.extend(parsed_comments)
        comment_progress_bar.progress((i + 1) / len(posts_to_analyze))

    if all_comments:
        df = pd.DataFrame(all_comments)
        
        st.dataframe(
            df,
            column_config={
                "post_link": st.column_config.LinkColumn(
                    "Post Link",
                    help="Click to open the post",
                    validate="https://moescape.ai/posts/.*",
                    display_text="Open post"
                )
            },
            hide_index=True
        )
        
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download CSV",
            csv,
            "moescape_comments.csv",
            "text/csv",
            key='download-csv'
        )
    else:
        st.write("No comments found for this user's posts.")
