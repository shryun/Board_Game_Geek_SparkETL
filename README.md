# Board Game Geek Data Pipeline

## Overview
This project demonstrates a **Spark-based ETL pipeline** that scrapes board game ranking data from [BoardGameGeek](https://boardgamegeek.com/browse/boardgame) and stores it in a **partitioned Parquet table** for analytics. The dataset includes key attributes like rank, title, year published, geek rating, average rating, and number of voters.

---

## Features
- **Web Scraping:** Extracts data from HTML tables using **BeautifulSoup**.  
- **Data Transformation:** Cleans and transforms raw data using **PySpark**, including:
  - Removing duplicates
  - Extracting the year from game titles
  - Handling missing values
- **Data Storage:** Stores cleaned data as a **partitioned Parquet table** (`games.board_games`) with the current date as the partition key.  
- **Analytics:** Enables querying and analysis of board game rankings using **Spark SQL**.  

---

## Table Schema
| Column         | Type    | Description                             |
|----------------|---------|-----------------------------------------|
| Rank           | Integer | Board game ranking                       |
| ImageURL       | String  | URL of the game image                    |
| Title          | String  | Board game title                         |
| YearPublished  | Integer | Year the game was published              |
| GeekRating     | Float   | Geek rating score                        |
| AvgRating      | Float   | Average rating score                      |
| NumVoters      | Integer | Number of voters                         |
| currentdate    | Date    | Date the record was processed/partition |

---

## Prerequisites
- Python 3.x  
- PySpark  
- pandas  
- BeautifulSoup4  
- Jupyter Notebook or Databricks environment  

---

## Usage
1. Clone the repository:
```bash
git clone <repository-url>
```
