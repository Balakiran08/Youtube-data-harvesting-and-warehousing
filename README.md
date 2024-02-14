Introduction:

This Python script collects data from the YouTube API, stores it in a MongoDB database, and then migrates the data to a PostgreSQL database. The collected data includes information about YouTube channels, videos, and comments.

Requirements:

Python

MongoDB

PostgreSQL

Google API key

Streamlit (for visualization)


Use the Streamlit app to interact with the collected data, visualize it, and execute SQL queries.

Code Structure:

main_data2: Connects to the YouTube API and retrieves channel data, video IDs, video data, and comments data.
channels_table: Creates a PostgreSQL table for channel data and inserts the collected data.

videos_table: Creates a PostgreSQL table for video data and inserts the collected data.

comment_table: Creates a PostgreSQL table for comments data and inserts the collected data.

all_tables: Calls the above functions to create and populate all tables in the PostgreSQL database.

Streamlit functions: streamlit_channel_data, streamlit_videos_data, and streamlit_comments_data display the data in Streamlit.

Streamlit Usage:

Enter a YouTube channel ID.
Click the "Collect and Store Data" button to collect data from the YouTube API and store it in MongoDB.
Click the "Migrate to SQL" button to migrate data from MongoDB to PostgreSQL.
Choose a table (Channels, Videos, or Comments) to display using the Streamlit app.
Select a predefined SQL query question to view the results.
