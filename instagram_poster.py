import os
import requests
from datetime import datetime, timedelta
from app import app, db, ConversionLog

# Configuration for Instagram Graph API
IG_ACCESS_TOKEN = os.environ.get('IG_ACCESS_TOKEN')
IG_USER_ID = os.environ.get('IG_USER_ID') # Your Instagram Business Account ID
IMAGE_URL = os.environ.get('IG_IMAGE_URL', 'https://mp3aud.io/default-stats-image.jpg') # IG requires media

def get_daily_stats():
    """Aggregates conversions from the last 24 hours"""
    with app.app_context():
        yesterday = datetime.utcnow() - timedelta(days=1)
        recent_logs = ConversionLog.query.filter(ConversionLog.timestamp >= yesterday).all()
        
        total_tracks = sum(log.track_count for log in recent_logs)
        unique_users = len(set(log.user_id for log in recent_logs if log.user_id))
        
        return total_tracks, unique_users

def post_to_instagram(total_tracks, unique_users):
    """Publishes a media container and caption to Instagram"""
    if not IG_ACCESS_TOKEN or not IG_USER_ID:
        print("Missing Instagram credentials. Please set IG_ACCESS_TOKEN and IG_USER_ID.")
        return

    caption = (
        f"🚀 Daily MP3aud.io Stats!\n\n"
        f"In the last 24 hours, our community successfully converted {total_tracks} tracks! 🎧🔥\n\n"
        f"Join {unique_users} active users today and start converting your favorite links into high-quality MP3s & AI Summaries.\n\n"
        f"#mp3audio #audioconverter #dailystats #productivity #music"
    )

    print(f"Preparing to post:\n{caption}\n")

    # Step 1: Create a media container
    media_url = f"https://graph.facebook.com/v18.0/{IG_USER_ID}/media"
    media_payload = {
        'image_url': IMAGE_URL,
        'caption': caption,
        'access_token': IG_ACCESS_TOKEN
    }
    
    media_response = requests.post(media_url, data=media_payload)
    media_data = media_response.json()
    
    if 'id' not in media_data:
        print(f"Error creating media container: {media_data}")
        return
        
    creation_id = media_data['id']
    
    # Step 2: Publish the media container to the feed
    publish_url = f"https://graph.facebook.com/v18.0/{IG_USER_ID}/media_publish"
    publish_payload = {'creation_id': creation_id, 'access_token': IG_ACCESS_TOKEN}
    publish_response = requests.post(publish_url, data=publish_payload)
    
    print(f"Instagram API Response: {publish_response.json()}")

if __name__ == "__main__":
    tracks, users = get_daily_stats()
    if tracks > 10:
        post_to_instagram(tracks, users)
    else:
        print(f"Only {tracks} conversions in the last 24 hours (threshold is >10). Skipping post.")