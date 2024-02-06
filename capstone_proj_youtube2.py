#!/usr/bin/env python
# coding: utf-8

# In[162]:


# import subprocess


# In[163]:


# subprocess.run(['pip', 'install', 'google-api-python-client'])


# In[30]:


from googleapiclient.discovery import build


# In[31]:


def api_connect():
    api_service_name = "youtube"
    api_version = "v3"
    api_key = "AIzaSyA9JWwH7Cfmp7a1C3FytLo7w1Dycr7isKw"
    youtube = build(api_service_name,api_version, developerKey = api_key)
    
    return youtube

youtube = api_connect()


# In[32]:


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
        'channel_subcount' : channel_subcount,
        'channel_vidcount' : channel_vidcount,
        'channel_viewcount' : channel_viewcount  
        }
    return(d)


# In[5]:


# channel_data = retrieve_channel_data("UCOdR79CQSet6tQrKVbfIaQg")


# In[6]:


# channel_data


# In[33]:


def get_allvideo_ids(channel_id):
    
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


# In[8]:


# Totalvideo_ids = get_allvideo_ids("UCOdR79CQSet6tQrKVbfIaQg")


# In[9]:


# Totalvideo_ids


# In[34]:


def get_allvideo_ids_data(Totalvideo_ids):
 

    video_data = []
    try:
    
        for video_id in Totalvideo_ids:
            request = youtube.videos().list(id = video_id,
                                            part = 'snippet, contentDetails, statistics')
            response = request.execute()




            all_vid_data = dict(
                        channel_name = response['items'][0]['snippet']['channelTitle'],
                        channel_id = response['items'][0]['snippet']['channelId'],
                        vid_id = response['items'][0]['id'],
                        title = response['items'][0]['snippet']['title'],
                        tags = response['items'][0]['snippet'].get('tags'),
                        thumbnail = response['items'][0]['snippet']['thumbnails']['default']['url'],
                        description = response['items'][0]['snippet'].get('description'),
                        published_date = response['items'][0]['snippet']['publishedAt'],
                        duration = response['items'][0]['contentDetails']['duration'],
                        views = response['items'][0]['statistics'].get('viewCount'),
                        likes = response['items'][0]['statistics'].get('likeCount'),
                        comments = response['items'][0]['statistics'].get('commentCount'),
                        favorite_count = response['items'][0]['statistics'].get('favoriteCount'),
                        definition = response['items'][0]['contentDetails']['definition'],
                        caption_status = response['items'][0]['contentDetails']['caption'],
                        )
            video_data.append(all_vid_data)
    except:
        pass

    return video_data


# In[11]:


# videos_data = get_allvideo_ids_data(Totalvideo_ids)


# In[12]:


# videos_data


# In[35]:


def get_all_comments_data(Totalvideo_ids):
    comments_data = []

    try:
        for video_id in Totalvideo_ids:
            request = youtube.commentThreads().list(part = 'snippet',
                                                    videoId = video_id,
                                                    maxResults = 50)
            response = request.execute()

            for item in response["items"]:
                all_comments_data = dict(
                    comment_id=item['snippet']['topLevelComment']['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comments_data.append(all_comments_data)
    except:
        pass

    return comments_data


# In[14]:


# comments_data = get_all_comments_data(Totalvideo_ids)


# In[15]:


# comments_data


# In[24]:


# subprocess.run(['pip', 'install', 'pymongo'])


# In[36]:


from pymongo import MongoClient


# In[37]:


mongo_uri = "mongodb://localhost:27017"
client = MongoClient(mongo_uri)
db = client.youtube2


# In[38]:


def main_data2(channel_id):
    channel_data_result = retrieve_channel_data(channel_id)
    all_vid_ids = get_allvideo_ids(channel_id)
    video_data = get_allvideo_ids_data(all_vid_ids)
    comments_data = get_all_comments_data(all_vid_ids)
    
    coll_2  = db.main_data2
    coll_2.insert_one(
                    {
                        'channel_data_result' : channel_data_result,
                        'video_data' : video_data,
                        'comments_data' : comments_data
                        
                    }
                    )
    return "process complete"


# In[39]:


# main_data = main_data2("UCcf1t1tH-Pp5bmu4hbAytwA")


# In[40]:


# main_data


# In[19]:


# !pip install psycopg2


# In[41]:


import psycopg2


# In[42]:


import pandas as pd


# In[43]:


def channels_table():

    mydb = psycopg2.connect(host = 'localhost',
                           user = 'postgres',
                           password = 'Liverpool08#',
                           database = 'youtube2',
                           port = '5432')
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()


    try:

        create_query = '''create table if not exists channels(channal_name varchar(60),
                                         channel_id varchar(80) primary key,
                                         channel_discripion text,
                                         publishedat varchar(80),
                                         channel_playlist_id varchar(80),
                                         channel_subcount bigint,
                                         channel_vidcount int,
                                         channel_viewcount bigint)'''

        cursor.execute(create_query)
        mydb.commit()

    except:
        print("channels table already created")


    channel_list = []
    db = client.youtube2
    coll_2 = db.main_data2
    for channel_data in coll_2.find({}, {'_id':0, 'channel_data_result':1}):
        channel_list.append(channel_data['channel_data_result'])

    df = pd.DataFrame(channel_list)



    for index, row in df.iterrows():
        insert_query = '''insert into channels(channal_name,
                                                channel_id,
                                                channel_discripion,
                                                publishedat,
                                                channel_playlist_id,
                                                channel_subcount,
                                                channel_vidcount,
                                                channel_viewcount)

                                                values(%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['channal_name'],
                    row['channel_id'],
                    row['channel_discripion'],
                    row['publishedat'],
                    row['channel_playlist_id'],
                    row['channel_subcount'],
                    row['channel_vidcount'],
                    row['channel_viewcount'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print('datas of channels are inserted')



# In[23]:


# df


# In[44]:


def videos_table():
    mydb = psycopg2.connect(host='localhost',
                    user='postgres',
                    password='Liverpool08#',
                    database='youtube2',
                    port='5432')
    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS videos(
            channel_name varchar(90),
            channel_id varchar(100),
            vid_id varchar(50) primary key,
            title varchar(150),
            tags text,
            thumbnail varchar(200),
            description text,
            published_date varchar(80),
            duration interval,
            views bigint,
            likes bigint,
            comments bigint,
            favorite_count int,
            definition varchar(20),
            caption_status varchar(50)
            )'''
    cursor.execute(create_query)
    mydb.commit()



    vid_list = []
    db = client.youtube2
    coll_2 = db.main_data2
    for vid_data in coll_2.find({}, {'_id':0, 'video_data':1}):
        for i in range(len(vid_data['video_data'])):
            vid_list.append(vid_data['video_data'][i]) 
            df1 = pd.DataFrame(vid_list)  

    for index, row in df1.iterrows():
        insert_query = '''insert into videos(channel_name,
                    channel_id,
                    vid_id,
                    title,
                    tags,
                    thumbnail,
                    description,
                    published_date,
                    duration,
                    views,
                    likes,
                    comments,
                    favorite_count,
                    definition,
                    caption_status)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (vid_id) DO NOTHING;'''

        values = (row['channel_name'],
                  row['channel_id'],
                  row['vid_id'],
                  row['title'],
                  row['tags'],
                  row['thumbnail'],
                  row['description'],
                  row['published_date'],
                  row['duration'],
                  row['views'],
                  row['likes'],
                  row['comments'],
                  row['favorite_count'],
                  row['definition'],
                  row['caption_status']
                  )


        cursor.execute(insert_query, values)
        mydb.commit()



# In[94]:





# In[95]:





# In[43]:


# df1


# In[45]:


def comment_table():

    mydb = psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Liverpool08#',
                                database='youtube2',
                                port='5432')
    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS comments(
                                    comment_id varchar(100) primary key, 
                                    Video_Id varchar(50),
                                    comment_Text text,
                                    comment_author varchar(150),
                                    comment_published timestamp
                                    )'''

    cursor.execute(create_query)
    mydb.commit()


    com_list = []
    db = client.youtube2
    coll_2 = db.main_data2
    for com_data in coll_2.find({}, {'_id':0, 'comments_data':1}):
        for i in range(len(com_data['comments_data'])):
           com_list.append(com_data['comments_data'][i]) 
    df2 = pd.DataFrame(com_list)  


    for index, row in df2.iterrows():
        insert_query = '''insert into comments(comment_id, 
                            Video_Id,
                            comment_Text,
                            comment_author,
                            comment_published)
                    values(%s,%s,%s,%s,%s)'''

        values = (row['comment_id'],
                  row['Video_Id'],
                  row['comment_Text'],
                  row['comment_author'],
                  row['comment_published']
                  )


        cursor.execute(insert_query, values)
        mydb.commit()






# In[102]:






# In[103]:





# In[27]:


# df2


# In[46]:


def all_tables():
    channels_table()
    videos_table()
    comment_table()
    
    return "tables created successfully"


# In[47]:


# all_tables = all_tables()


# In[48]:


# all_tables


# In[49]:


import streamlit as st


# In[55]:


def streamlit_channel_data():
    channel_list = []
    db = client.youtube2
    coll_2 = db.main_data2
    for channel_data in coll_2.find({}, {'_id':0, 'channel_data_result':1}):
        channel_list.append(channel_data['channel_data_result'])
    df = st.dataframe(channel_list)

    return df


# In[26]:


# df


# In[56]:


def streamlit_vidoes_data():
    vid_list = []
    db = client.youtube2
    coll_2 = db.main_data2
    for vid_data in coll_2.find({}, {'_id':0, 'video_data':1}):
        for i in range(len(vid_data['video_data'])):
            vid_list.append(vid_data['video_data'][i]) 
        df1 = st.dataframe(vid_list)

    return df1


# In[117]:


# df1


# In[57]:


def streamlit_comments_data():
    com_list = []
    db = client.youtube2
    coll_2 = db.main_data2
    for com_data in coll_2.find({}, {'_id':0, 'comments_data':1}):
        for i in range(len(com_data['comments_data'])):
            com_list.append(com_data['comments_data'][i]) 
    df2 = st.dataframe(com_list) 

    return df2


# In[121]:


# df2


# In[58]:


with st.sidebar:
    st.title(":red [YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header('skill take aways')
    st.caption('Python scripting')
    st.caption('Data collection')
    st.caption('MongoDB')
    st.caption('API Integration')
    st.caption('data Management using MongoDB and SQL')
    
    
channel_id = st.text_input('Enter the channel Id')



# In[59]:


if st.button('collect and store data'):
    ch_ids = []
    db = client.youtube2
    coll_2 = db.main_data2
    for ch_data in coll_2.find({}, {'_id': 0 , 'channel_data_result': 1}):
        ch_ids.append(ch_data['channel_data_result']['channel_id'])

    if channel_id in ch_ids:
        st.success('channel details of the given channel id already exists')

    else:
        insert = main_data2(channel_id)
        st.success(insert)
        
if st.button('Migrate to SQL'):
    table = all_tables()
    st.success(table)
    
show_table = st.selectbox("SELECT THE TABLE FOR VIEW", ('channels', 'videos', 'comments'))

if show_table == "channels":
    streamlit_channel_data()
    
elif show_table == "videos":
    streamlit_vidoes_data()
    
elif show_table == "comments":
    streamlit_comments_data()
    
mydb = psycopg2.connect(host='localhost',
                        user='postgres',
                        password='Liverpool08#',
                        database='youtube2',
                        port='5432')
cursor = mydb.cursor()        
        
question = st.selectbox('questions',(
            '1.All the videos and the channel name',
            "2.Channels with most number of videos",
            '3.10 most viewed videos',
            '4.No of comments in each videos with video names',
            '5.highest likes of the videos and their channel name',
            '6.no of likes and dislikes for each video, corresponding video names',
            '7.no of views for each channel and channel names',
            '8.all the channels that have published videos in the year 2022',
            '9. average duration of all videos in each channel, their corresponding channel names',
            '10. highest number of comment and corresponding channel names'))


# In[61]:


if question  == '1.All the videos and the channel name':
    query1 = '''select title as videos, channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1,columns = ['video title', 'channel name'])
    st.write(df)
#     df


# In[63]:


elif question == '2.Channels with most number of videos':
    query2 = '''select channal_name as channelname,channel_vidcount as channel_total_vid_count from channels order by channel_vidcount desc'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df1 = pd.DataFrame(t2,columns = ['channelname', 'channel_total_vid_count'])
    st.write(df1)
#     df1


# In[65]:


elif question == '3.10 most viewed videos':
    query3 = '''select views as views, channel_name as channelname, title as title from videos order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df2 = pd.DataFrame(t3,columns = ['views', 'channelname','title'])
    st.write(df2)
#     df2


# In[68]:


elif question == '4.No of comments in each videos with video names':
    query4 = '''select comments as no_of_comments, title as video_title from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df3 = pd.DataFrame(t4,columns = ['comments', 'title'])
    st.write(df3)
#     df3


# In[ ]:


elif question == '5.highest likes of the videos and their channel name':
    query5 = '''select channel_name  as channel_name, title as title, likes as likes from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df4 = pd.DataFrame(t5,columns = ['channel_name','title','likes'])
    st.write(df4)
#     df4


# In[ ]:


elif question == '6.no of likes and dislikes for each video, corresponding video names':
    query6 = '''select title as title, likes as likes from videos where likes is not null'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df5 = pd.DataFrame(t6,columns = ['title','likes'])
    st.write(df5)
#     df5


# In[ ]:


elif question == '7.no of views for each channel and channel names':
    query7 = '''select channal_name as channal_name, channel_viewcount as channel_viewcount from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df6 = pd.DataFrame(t7,columns = ['channal_name','channel_viewcount'])
    st.write(df6)
#     df6


# In[ ]:


elif question == '8.all the channels that have published videos in the year 2022':
    query8 = '''select channel_name as channel_name, title as title, published_date as published_date from videos where extract(year from published_date :: date)= 2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df7 = pd.DataFrame(t8,columns = ['channel_name','title','published_date'])
    st.write(df7)
#     df7


# In[ ]:


elif question == '9. average duration of all videos in each channel, their corresponding channel names':
    query9 = '''select channel_name as channel_name,avg(duration) as average_duration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df8 = pd.DataFrame(t9,columns = ['channel_name','average_duration'])
    st.write(df8)
#     df8

    T9 = []
    for index,row in df8.iterrows():
        channel_name = row['channel_name']
        average_duration = row['average_duration']
        average_duration_str = str(average_duration)
        T9.append(dict(channel_name = channel_name, average_duration = average_duration_str))


# In[ ]:


elif question == '10. highest number of comment and corresponding channel names':
    query10 = '''select channel_name as channel_name,title as title, comments as comments from videos where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df9 = pd.DataFrame(t10,columns = ['channel_name','tile','comments'])
    st.write(df9)
#     df9

