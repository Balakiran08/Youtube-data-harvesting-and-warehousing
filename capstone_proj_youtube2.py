from googleapiclient.discovery import build

def api_connect():
    service_name = "youtube"
    version = "v3"
    api_key = "AIzaSyA9JWwH7Cfmp7a1C3FytLo7w1Dycr7isKw"
    youtube = build(service_name,version, developerKey = api_key)
    
    return youtube

youtube = api_connect()

def retrieve_channel_data(channel_id):
    
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= channel_id)
    response = request.execute()
    
    channal_name = response['items'][0]['snippet']['title']
    channel_id = response['items'][0]['id']
    channel_discripion = response['items'][0]['snippet']['description']
    publishedat= response['items'][0]['snippet']['publishedAt']
    channel_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    channel_subcount = response['items'][0]['statistics']['subscriberCount']
    channel_vidcount = response['items'][0]['statistics']['videoCount']
    channel_viewcount = response['items'][0]['statistics']['viewCount']
    
    d = {
        'channal_name': channal_name,
        'channel_id' : channel_id,
        'channel_discripion' : channel_discripion,
        'publishedat' : publishedat,
        'channel_playlist_id' : channel_playlist_id,
        'channel_subcount' : int(channel_subcount),
        'channel_vidcount' : int(channel_vidcount),
        'channel_viewcount' : int(channel_viewcount)  
        }
    return(d)

def retrieve_allvideo_ids(channel_id):
    
    allvideo_ids = []

    request = youtube.channels().list(
                            part ="contentDetails",
                            id = channel_id)
    response = request.execute()
    channel_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        request1 = youtube.playlistItems().list(
                            part ="snippet",
                            playlistId = channel_playlist_id,
                            maxResults = 50,
                            pageToken = next_page_token)
        response1 = request1.execute()

        for i in range(len(response1["items"])):
            allvideo_ids.append(response1['items'][i]['snippet']['resourceId']['videoId']) 
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return allvideo_ids

def retrieve_allvideo_ids_data(Totalvideo_ids):
    video_data = []
    try:
    
        for video_id in Totalvideo_ids:
                request = youtube.videos().list(id = video_id,
                                                part = 'snippet, contentDetails, statistics')
                response = request.execute()

                all_vid_data = dict(
                        video_id = response['items'][0]['id'],
                        video_name = response['items'][0]['snippet']['title'], 
                        video_description = response['items'][0]['snippet'].get('description'),
                        tags = response['items'][0]['snippet'].get('tags'),
                        publishedAt = response['items'][0]['snippet']['publishedAt'],
                        view_count = response['items'][0]['statistics'].get('viewCount'),
                        like_count = response['items'][0]['statistics'].get('likeCount'),
                        favorite_count = response['items'][0]['statistics'].get('favoriteCount'),
                        comment_count = response['items'][0]['statistics'].get('commentCount'),
                        duration = response['items'][0]['contentDetails']['duration'],
                        thumbnail = response['items'][0]['snippet']['thumbnails']['default']['url'],
                        caption_status = response['items'][0]['contentDetails']['caption'],
                        channel_id = response['items'][0]['snippet']['channelId'],
                        channel_name = response['items'][0]['snippet']['channelTitle'],
                        )
                video_data.append(all_vid_data)
    except:
        pass
    return video_data

def retrieve_all_comments_data(Totalvideo_ids):
    comments_data = []
    try:
        for video_id in Totalvideo_ids:
            request = youtube.commentThreads().list(part = 'snippet',
                                                    videoId = video_id,
                                                    maxResults = 50)
            response = request.execute()

            for i in response["items"]:
                all_comments_data = dict(
                    comment_id=i['snippet']['topLevelComment']['id'],
                    Video_Id=i['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_Text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published=i['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comments_data.append(all_comments_data)
    except:
        pass
    return comments_data

from pymongo import MongoClient

mongo_uri = "mongodb://localhost:27017"
client = MongoClient(mongo_uri)
db = client.youtube

def main_data(channel_id):
    all_vid_ids = retrieve_allvideo_ids(channel_id) 
    channel_data_result = retrieve_channel_data(channel_id)
    video_data = retrieve_allvideo_ids_data(all_vid_ids)
    comments_data = retrieve_all_comments_data(all_vid_ids)

    all_data = {
        'channel_data': channel_data_result,
        'video_data': video_data,
        'comments_data': comments_data
    }

    collection = db.main_data
    collection.insert_one(all_data)

    return "Process complete"

import psycopg2
import pandas as pd
mydb = psycopg2.connect(host = 'localhost',
                           user = 'postgres',
                           password = 'Liverpool08#',
                           database = 'youtube',
                           port = '5432')
cursor = mydb.cursor()


def channels_table():
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channels(
                            channal_name VARCHAR(60),
                            channel_id VARCHAR(80) PRIMARY KEY,
                            channel_discripion TEXT,
                            publishedat VARCHAR(80),
                            channel_playlist_id VARCHAR(80),
                            channel_subcount BIGINT,
                            channel_vidcount INT,
                            channel_viewcount BIGINT
                         )'''
        cursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        print(f"Error creating channels table: {e}")
        mydb.rollback()

    channel_list = []
    db = client.youtube
    collection = db.main_data
    for doc in collection.find({}, {"_id": 0, "channel_data": 1}):
        # Convert string values to integers
        doc["channel_data"]["channel_subcount"] = int(doc["channel_data"]["channel_subcount"]) 
        doc["channel_data"]["channel_vidcount"] = int(doc["channel_data"]["channel_vidcount"])
        doc["channel_data"]["channel_viewcount"] = int(doc["channel_data"]["channel_viewcount"])
        channel_list.append(doc["channel_data"])

    df = pd.DataFrame(channel_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO channels(
                            channal_name,
                            channel_id,
                            channel_discripion,
                            publishedat,
                            channel_playlist_id,
                            channel_subcount,
                            channel_vidcount,
                            channel_viewcount
                          )
                          VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                          ON CONFLICT (channel_id) DO NOTHING''' 
        values = (
            row['channal_name'],
            row['channel_id'],
            row['channel_discripion'],
            row['publishedat'],
            row['channel_playlist_id'],
            row['channel_subcount'],
            row['channel_vidcount'],
            row['channel_viewcount']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting channel data: {e}")  
            mydb.rollback()

def videos_table():
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS videos(
                video_id varchar(50) primary key,
                video_name varchar(150),
                video_description text,
                tags text,
                publishedAt varchar(80),
                view_count bigint,
                like_count bigint,
                favorite_count int,
                comment_count bigint,
                duration interval,
                thumbnail varchar(200),  
                caption_status varchar(50),
                channel_id varchar(50),
                channel_name varchar(60)
                )'''
        cursor.execute(create_query)
        mydb.commit()  
    except Exception as e:
        print(f"Error creating videos table: {e}")
        mydb.rollback()  

    vid_list = []
    db = client.youtube
    collection = db.main_data
    for vid_data in collection.find({}, {'_id': 0, 'video_data': 1}):
        for i in range(len(vid_data['video_data'])):
            vid_list.append(vid_data['video_data'][i]) 
    df1 = pd.DataFrame(vid_list)  

    for index, row in df1.iterrows():
        insert_query = '''insert into videos(
                    video_id,
                    video_name,
                    video_description,
                    tags,
                    publishedAt,
                    view_count,
                    like_count,
                    favorite_count,
                    comment_count,
                    duration,
                    thumbnail,  
                    caption_status,
                    channel_id,
                    channel_name)
                    
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (video_id) DO NOTHING;'''

        values = (
                  row['video_id'],
                  row['video_name'],
                  row['video_description'],
                  row['tags'],
                  row['publishedAt'],
                  row['view_count'],
                  row['like_count'],
                  row['favorite_count'],
                  row['comment_count'],
                  row['duration'],
                  row['thumbnail'],
                  row['caption_status'],
                  row['channel_id'],
                  row['channel_name']
                  )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()  
        except Exception as e:
            print(f"Error inserting video data: {e}")
            mydb.rollback() 

def comment_table():
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS comments(
                                    comment_id varchar(100) primary key, 
                                    Video_Id varchar(50),
                                    comment_Text text,
                                    comment_author varchar(150),
                                    comment_published timestamp
                                    )'''
        cursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        print(f"Error creating comments table: {e}")
        mydb.rollback()

    com_list = []
    db = client.youtube
    collection = db.main_data
    for com_data in collection.find({}, {'_id':0, 'comments_data':1}):
        for i in range(len(com_data['comments_data'])):
            com_list.append(com_data['comments_data'][i]) 
    df2 = pd.DataFrame(com_list)  

    for index, row in df2.iterrows():
        insert_query = '''insert into comments(comment_id, 
                                            Video_Id,
                                            comment_Text,
                                            comment_author,
                                            comment_published)
                                            values(%s,%s,%s,%s,%s)
                                            ON CONFLICT (comment_id) DO NOTHING''' 

        values = (row['comment_id'],
                  row['Video_Id'],
                  row['comment_Text'],
                  row['comment_author'],
                  row['comment_published'])
                  
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting comment data: {e}")
            mydb.rollback()

def all_tables():
    channels_table()
    videos_table()
    comment_table()
    return "tables created successfully"

import streamlit as st

def df_channel_data():
    channel_list = []
    db = client.youtube
    collection = db.main_data
    for channel_data in collection.find({}, {'_id': 0, 'channel_data': 1}):  
        channel_list.append(channel_data['channel_data'])  
    
    df_channel = st.dataframe(channel_list)
    return df_channel

def df_vidoes_data():
    vid_list = []
    db = client.youtube
    collection = db.main_data
    for vid_data in collection.find({}, {'_id':0, 'video_data':1}):
        for i in range(len(vid_data['video_data'])):
            vid_list.append(vid_data['video_data'][i]) 
    
    df_video = st.dataframe(vid_list)
    return df_video

def df_comments_data():
    com_list = []
    db = client.youtube
    collection = db.main_data
    for com_data in collection.find({}, {'_id':0, 'comments_data':1}):
        for i in range(len(com_data['comments_data'])):
            com_list.append(com_data['comments_data'][i]) 
    
    df_comments = st.dataframe(com_list) 

    return df_comments

        
def scrap_page():
    channel_id = st.text_input('channel ID')
    
    if st.button('scrap data'):
        channel_data_result = retrieve_channel_data(channel_id)
        st.write("channel:")
        st.write(channel_data_result)

        all_vid_ids = retrieve_allvideo_ids(channel_id)
        
        video_data = retrieve_allvideo_ids_data(all_vid_ids)
        st.write("Video:")
        st.write(video_data)

        comments_data = retrieve_all_comments_data(all_vid_ids)
        st.write("Comments:")
        st.write(comments_data)
        
    if st.button('Data to Mongodb'):
        ch_ids = []
        db = client.youtube
        collection = db.main_data
        for ch_data in collection.find({}, {'_id': 0, 'channel_data': 1}):  
            ch_ids.append(ch_data['channel_data']['channel_id'])  

        if channel_id in ch_ids:
            st.success('channel details of the given channel id already exists')

        else:
            insert = main_data(channel_id)
            st.success(insert)

    if st.button('Mongodb to SQL'):
        table = all_tables()
        st.success(table)

def Tables():
    radio = st.radio("SELECT ANY TABLE", ('channels', 'videos', 'comments'))

    if radio == "channels":
        df_channel_data()
    elif radio == "videos":
        df_vidoes_data()
    elif radio == "comments":
        df_comments_data()

        
def questions_page():
    mydb = psycopg2.connect(host='localhost',
                            user='postgres',
                            password='Liverpool08#',
                            database='youtube',
                            port='5432')
    cursor = mydb.cursor()        

    st.header("Analyze with any question below")
    question = st.selectbox('questions',(
                "1.What are the names of all the videos and their corresponding channels?",
                "2.Which channels have the most number of videos, and how many videos do they have?",
                "3.What are the top 10 most viewed videos and their respective channels?",
                "4.How many comments were made on each video, and what are their corresponding video names?",
                "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                "8.What are the names of all the channels that have published videos in the year 2022?",
                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))
        
    if question  == "1.What are the names of all the videos and their corresponding channels?":
        query1 = '''select video_name as videos, 
                    channel_name as channelname from videos'''
        cursor.execute(query1)
        mydb.commit()
        t1 = cursor.fetchall()
        df = pd.DataFrame(t1,columns = ['video title', 'channel name'])
        st.write(df)
    
    elif question ==  "2.Which channels have the most number of videos, and how many videos do they have?":
        query2 = '''select channal_name as channelname,
                    channel_vidcount as channel_total_vid_count from channels order by channel_vidcount desc'''
        cursor.execute(query2)
        mydb.commit()
        t2 = cursor.fetchall()
        df1 = pd.DataFrame(t2,columns = ['channelname', 'channel_total_vid_count'])
        st.write(df1)
    
    elif question ==  "3.What are the top 10 most viewed videos and their respective channels?":
        query3 = '''select view_count as views, 
                    channel_name as channelname, 
                    video_name as title from videos order by views desc limit 10'''
        cursor.execute(query3)
        mydb.commit()
        t3 = cursor.fetchall()
        df2 = pd.DataFrame(t3,columns = ['views', 'channelname','title'])
        st.write(df2)
    
    elif question == "4.How many comments were made on each video, and what are their corresponding video names?":
        query4 = '''select comment_count as no_of_comments,
                    video_name as video_title from videos where comment_count is not null'''
        cursor.execute(query4)
        mydb.commit()
        t4 = cursor.fetchall()
        df3 = pd.DataFrame(t4,columns = ['no_of_comments', 'video_title'])
        st.write(df3)
    
    elif question == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5 = '''select channel_name  as channel_name, 
                    video_name as title, 
                    like_count as likes from videos where like_count is not null order by like_count desc'''
        cursor.execute(query5)
        mydb.commit()
        t5 = cursor.fetchall()
        df4 = pd.DataFrame(t5,columns = ['channel_name','title','likes'])
        st.write(df4)
    
    elif question == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query6 = '''select video_name as video_title, 
                    like_count as likes from videos where like_count is not null'''
        cursor.execute(query6)
        mydb.commit()
        t6 = cursor.fetchall()
        df5 = pd.DataFrame(t6,columns = ['video_title','likes'])
        st.write(df5)
    
    elif question == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
        query7 = '''select channal_name as channal_name, 
                    channel_viewcount as channel_viewcount from channels'''
        cursor.execute(query7)
        mydb.commit()
        t7 = cursor.fetchall()
        df6 = pd.DataFrame(t7,columns = ['channal_name','channel_viewcount'])
        st.write(df6)
    
    elif question == "8.What are the names of all the channels that have published videos in the year 2022?":
        query8 = '''select channel_name as channel_name, 
                    video_name as title, 
                    publishedAt as published_date from videos where extract(year from publishedat::date :: date)= 2022'''
        cursor.execute(query8)
        mydb.commit()
        t8 = cursor.fetchall()
        df7 = pd.DataFrame(t8,columns = ['channel_name','title','published_date'])
        st.write(df7)
    
    elif question ==  "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9 = '''select channel_name as channel_name,
                    avg(duration) as average_duration from videos group by channel_name'''
        cursor.execute(query9)
        mydb.commit()
        t9 = cursor.fetchall()
        df8 = pd.DataFrame(t9,columns = ['channel_name','average_duration'])
        st.write(df8)

        T9 = []
        for index,row in df8.iterrows():
            channel_name = row['channel_name']
            average_duration = row['average_duration']
            average_duration_str = str(average_duration)
            T9.append(dict(channel_name = channel_name, average_duration = average_duration_str))

        
    elif question == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        query10 = '''select channel_name as channel_name,
                    video_name as title, 
                    comment_count as comments_count from videos where comment_count is not null order by comment_count desc'''
        cursor.execute(query10)
        mydb.commit()
        t10 = cursor.fetchall()
        df9 = pd.DataFrame(t10,columns = ['channel_name','title','comments_count'])
        st.write(df9)
        
        
def main():
    st.set_page_config(layout="wide")
    
    tab1, tab2, tab3, tab4 = st.tabs(["HOME", "SCRAP DATA", "TABLES", "ANALYSIS"])

    with tab1:  

        with st.container():
            st.markdown("""
            <div class="content-container">
                <h1 style="text-align:center; color:#FF0000;">YouTube Data Analytics Suite</h1>
                <p style="text-align:center; font-size:18px;">
                    Comprehensive YouTube data harvesting and analysis platform with:
                </p>
                <ul style="font-size:16px;">
                    <li>📈 Channel metadata extraction</li>
                    <li>📊 Video content analysis</li>
                    <li>💬 Comment sentiment tracking</li>
                    <li>💾 Dual database storage (MongoDB & PostgreSQL)</li>
                    <li>📉 Interactive analytics dashboard</li>
                </ul>
                <p style="text-align:center; margin-top:20px;">
                    Select a tab above to get started!
                </p>
            </div>
            """, unsafe_allow_html=True)

    with tab2:  
        scrap_page()

    with tab3: 
        Tables()

    with tab4:  
        questions_page()

if __name__ == "__main__":
    main()
        
