# Google API 
from googleapiclient.discovery import build
import json
import s3fs

# Data Manipulation
import pandas as pd
from dateutil import parser

def youtube_data_analysis():

    # https://developers.google.com/youtube/v3/docs
    api_key = "AIzaSyApYt112vr-Iq2xxLSnNGuFJ0NtywS11oU"

    api_service_name = "youtube"
    api_version = "v3"

    # Get credentials and create an API client
    youtube = build(api_service_name, api_version, developerKey=api_key)

    playlist_id="UU_mYaQAE6-71rjSN6CeCA-g"

    def get_video_ids(youtube, playlist_id):
        
        video_ids = []
        
        request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults = 50
            )
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
            #description.append(item['snippet']['description'])
            
        next_page_token = response.get('nextPageToken')
        while next_page_token is not None:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults = 50,
                pageToken = next_page_token
            )
            response = request.execute()

            for item in response['items']:
                video_ids.append(item['contentDetails']['videoId'])
                
            next_page_token = response.get('nextPageToken')
            
        return video_ids

    video_ids = get_video_ids(youtube, playlist_id)

    def get_video_details(youtube,video_ids):
        all_video_info = []

        for i in range(0, len(video_ids),50):
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id = ','.join(video_ids[i:i+50])
            )
            response = request.execute()

            for video in response['items']:
                stats_to_keep = {'snippet': ['channelTitle','title','description','tags','publishedAt'],
                                'statistics': ['viewCount','likeCount','favoriteCount','commentCount'],
                                'contentDetails': ['duration','definition','caption']
                                }
                video_info = {}
                video_info['video_id'] = video['id']

                for k in stats_to_keep.keys():
                    for v in stats_to_keep[k]:
                        try:
                            video_info[v] = video[k][v]
                        except:
                            video_info[v] = None

                all_video_info.append(video_info)
        
        return pd.DataFrame(all_video_info)

    video_df = get_video_details(youtube,video_ids)

    numeric_cols = ['viewCount', 'likeCount', 'favoriteCount', 'commentCount']
    video_df[numeric_cols] = video_df[numeric_cols].apply(pd.to_numeric, errors = 'coerce', axis = 1)

    # add no.of tags
    video_df['tagCount'] = video_df['tags'].apply(lambda x: 0 if x is None else len(x))

    # comments and links per 1000 view ratio
    video_df['likeRatio'] = video_df['likeCount']/ video_df['viewCount'] * 1000
    video_df['commentRatio'] = video_df['commentCount']/ video_df['viewCount'] * 1000

    # Title character length
    video_df['titleLength'] = video_df['title'].apply(lambda x: len(x))

    video_df.to_csv("s3://my-airflow-youtube-bucket/Youtube_video_analysis.csv")

youtube_data_analysis()