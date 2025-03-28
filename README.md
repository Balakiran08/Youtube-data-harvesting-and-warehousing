# YouTube Data Harvesting and Analysis Tool

A comprehensive tool to harvest YouTube channel data, store it in databases, and perform analytical queries for insights.

##  Overview

This project enables users to:
1. **Retrieve YouTube Channel Data** via YouTube API (channel details, videos, comments).
2. **Store Data** in MongoDB (NoSQL) for flexible storage.
3. **Migrate to PostgreSQL** (SQL) for structured analysis.
4. **Analyze Data** through 10 predefined business questions using Streamlit.

##  Features

- **YouTube API Integration**: Fetch channel, video, and comment data.
- **MongoDB Storage**: Temporarily store raw JSON data.
- **PostgreSQL Migration**: Structured data for SQL queries.
- **Streamlit UI**: User-friendly interface for data collection and analysis.
- **Predefined Analytics**: Answer key questions about channels/videos.

## 🔧 Tools & Technologies Used

- **YouTube Data API v3**: For data retrieval.
- **Python**: Core scripting language.
- **MongoDB**: NoSQL database for raw data storage.
- **PostgreSQL**: SQL database for structured analysis.
- **Streamlit**: Web interface for user interaction.
- **Libraries**:
  - `googleapiclient`: YouTube API access.
  - `pymongo`: MongoDB operations.
  - `psycopg2`: PostgreSQL connectivity.
  - `pandas`: Data manipulation.
  - `streamlit`: UI components.

## 📊 Analysis Questions

The tool answers these 10 analytical questions:
1. List all videos and their channels.
2. Channels with most videos.
3. Top 10 most viewed videos.
4. Video comment counts.
5. Most-liked videos.
6. Total likes per video.
7. Total views per channel.
8. Channels publishing videos in 2022.
9. Average video duration per channel.
10. Most-commented videos.


