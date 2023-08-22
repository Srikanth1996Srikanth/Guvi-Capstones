import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build


st.subheader('Youtube Data Harvesting and Warehousing')


st.set_page_config(page_title= None,
                   page_icon= None,
                   layout= "centered",
                   initial_sidebar_state= "auto",
                   menu_items=None)

#menu

tab1, tab2, tab3 = st.tabs(['Main page','pymongo Extract','Sql extract'])

st.header('Main page')

st.header('pymongo')

st.header('sql transform')


# pymongo
client = pymongo.MongoClient("localhost:27017")
db = client.youtube


# sql database
mydb = sql.connect(
    host= 'localhost',
    user='root',
    password= '9965543275@Sri',
auth_plugin='mysql_native_password'
)
mycursor= mydb.cursor()

mycursor.execute('USE newdb1')

# youtube api
api_key = "AIzaSyBYNMjJf6T_tZpQmCbnig5QCorqA11K7Fk" 

api_service_name = "youtube"
api_version = "v3"

youtube = build('youtube','v3',developerKey=api_key)


# Channel details
def channeldetails(channel_id):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(
                    Channel_name = response['items'][i]['snippet']['title'],
                    Channelid = channel_id[i],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    subscribers = response['items'][i]['statistics']['subscriberCount'],
                    views = response['items'][i]['statistics']['viewCount'],
                    all_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    )
        ch_data.append(data)
    return ch_data


# videos
def channelvideos(channel_id):
    videoids = []
    a = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = a['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        a = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=20,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(a['items'])):
            videoids.append(a['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = a.get('nextPageToken')
        
        if next_page_token is None:
            break
    return videoids


# video details
def videodetails(v_ids):
    videostatus = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            videostatus.append(video_details)
    return videostatus


#for comments
def commentsdetails(v_id):
    commentsdata = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                commentsdata.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return commentsdata


# youtube channel names from mongodb
def channelnames():   
    chname = []
    for i in db.channel_details.find():
        chname.append(i['Channel_name'])
    return chname


# Main Page
if select == "Main page":
    st.subheader('YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit')
    st.subheader('Domain : Social Media')
    st.subheader('Skills used in this project : python,streamlit,MongoDB,SQL' )
    
# data extraction
                 
if selected == "pymongo Extract":
    tab1,tab2 = st.tabs()
    
    # extraction tab
    with tab1:
        st.write("TypeYouTube Channel ID below")
        ch_id = st.text_input().split('')

        if ch_id and st.button("Data Extraction"):
            ch_details = channeldetails(ch_id)
            st.table(ch_details)

        if st.button("Upload MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = channeldetails(ch_id)
                v_ids = channelvideos(ch_id)
                vid_details = videodetails(v_ids)
                
                def comments():
                    comm = []
                    for i in v_ids:
                        comm+= commentsdetails(i)
                    return comm
                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                st.success("Upload to MongoDB successful !!")
      
    # TRANSFORM TAB
    with tab2:     
            ch_names = channelnames()  
            user_inp = st.selectbox("Select channel",options= ch_names)
        
    def insertintochannels():
                    collections = db.channel_details
                    query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                
                    for i in collections.find({"channel_name" : user_inp},{'_id' : 0}):
                        mycursor.execute(query,tuple(i.values()))
                    mydb.commit()
                
    def insertintovideos():
            collections1 = db.video_details
            query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in collections1.find({"channel_name" : user_inp},{'_id' : 0}):
                values = [str(val).replace("'", "''").replace('"', '""') if isinstance(val, str) else val for val in i.values()]
                mycursor.execute(query1, tuple(values))
                mydb.commit()

    def insertintocomments():
            collections1 = db.video_details
            collections2 = db.comments_details
            query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in collections1.find({"channel_name" : user_inp},{'_id' : 0}):
                for i in collections2.find({'Video_id': vid['Video_id']},{'_id' : 0}):
                    mycursor.execute(query2,tuple(i.values()))
                    mydb.commit()

    if st.button("Submit"):
        try:
            insertintovideos()
            insertintochannels()
            insertintocomments()
            st.success("Transformation to MySQL Successful !!")
        except:
            st.error("Channel details already transformed !!")
            
        
        
    