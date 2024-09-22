import streamlit as st
import cloudscraper
import pandas as pd
from datetime import datetime
import time
import logging
import pytz

logging.basicConfig(level=logging.INFO)

class RateLimiter:
    def __init__(self, calls_per_second=1):
        self.calls_per_second = calls_per_second
        self.last_call = 0

    def wait(self):
        now = time.time()
        time_since_last_call = now - self.last_call
        if time_since_last_call < 1 / self.calls_per_second:
            time.sleep((1 / self.calls_per_second) - time_since_last_call)
        self.last_call = time.time()

rate_limiter = RateLimiter(calls_per_second=2)
scraper = cloudscraper.create_scraper()

@st.cache_data(ttl=3600)
def fetch_with_rate_limit(url, max_retries=5, initial_delay=3):
    for attempt in range(max_retries):
        rate_limiter.wait()
        try:
            response = scraper.get(url)
            response.raise_for_status()
            return response.json()
        except cloudscraper.exceptions.CloudflareChallengeError:
            logging.error("Cloudflare challenge detected. Unable to bypass.")
            return None
        except Exception as e:
            if response.status_code == 429:
                delay = initial_delay * (2 ** attempt)
                logging.info(f"Rate limited. Waiting {delay} seconds before retry.")
                time.sleep(delay)
            else:
                logging.error(f"Failed to fetch data: {str(e)}")
                return None
    logging.error("Max retries reached. Giving up.")
    return None

@st.cache_data(ttl=3600)
def fetch_user_posts(user_id, limit=2000):
    all_posts = []
    offset = 0
    batch_size = 500  # API's maximum limit per request

    while len(all_posts) < limit:
        url = f"https://api.moescape.ai/v1/users/{user_id}/posts?offset={offset}&limit={batch_size}"
        data = fetch_with_rate_limit(url)
        
        if data is None:
            st.error(f"Failed to fetch posts at offset {offset}")
            break
        
        if not isinstance(data, list):
            st.error(f"Unexpected data format received at offset {offset}")
            break
        
        all_posts.extend(data)
        
        if len(data) < batch_size:  # No more posts to fetch
            break
        
        offset += batch_size

    return all_posts[:limit]  # Return only the requested number of posts

@st.cache_data(ttl=3600)
def fetch_post_comments(post_uuid):
    url = f"https://api.moescape.ai/v1/posts/{post_uuid}/comments?offset=0&limit=20"
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
            #'post_uuid': post_uuid,
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
                    #'post_uuid': post_uuid,
                    'post_title': post_title,
                    'post_link': f"https://moescape.ai/posts/{post_uuid}"
                }
                parsed_comments.append(parsed_reply)
    
    return parsed_comments

st.title('Moescape User Posts and Comments')

user_id = st.text_input('Enter User ID')
num_posts = st.number_input('Number of posts to scan (max 2000)', min_value=1, max_value=2000, value=10)
order = st.radio("Order of posts to analyze", ('Most Recent', 'Oldest'))

if user_id and num_posts:
    posts = fetch_user_posts(user_id, limit=num_posts)
    
    if posts:
        total_posts = len(posts)
        st.write(f"Found {total_posts} posts")
        
        posts.sort(key=lambda x: x.get('created_at', ''), reverse=(order == 'Most Recent'))
        
        st.write(f"Analyzing the {'most recent' if order == 'Most Recent' else 'oldest'} {total_posts} posts")
        
        progress_bar = st.progress(0)
        all_comments = []
        
        for i, post in enumerate(posts):
            comments = fetch_post_comments(post['uuid'])
            parsed_comments = parse_comments(comments, post['uuid'], post['title'])
            all_comments.extend(parsed_comments)
            progress_bar.progress((i + 1) / len(posts))

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
        
