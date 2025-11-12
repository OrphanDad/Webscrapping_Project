Overview:

This project scrapes quotes from quotes.toscrape.com and builds a small ETL pipeline 
that loads the scraped data into a PostgreSQL database using PySpark.
The scraper collects the quote, author, tags, page number, and timestamp, saving them as JSON files.

Structure

Webscraping_project/
│
├── scraper.py                 
├── processing/
│   ├── process_data.py        
│   ├── load_to_postgres.py    
│
├── data/
│   ├── raw/                   
│   ├── processed/             
│
├── config/config.ini          
├── main.py                    
├── requirements.txt           
└── README.md           

Output:

JSON files are stored in: data/raw
Processed files are saved in: data/processed
Final data is loaded into PostgreSQL tables: authors, tags, quotes_stage, and quotes
