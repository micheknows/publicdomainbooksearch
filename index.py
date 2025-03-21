from fastapi import FastAPI, Request
import asyncio
from typing import Optional
import requests
import psycopg2
import os
import logging

app = FastAPI()

logging.basicConfig(level=logging.INFO)

@app.get("/")
async def home():
    return "Hello, World!"


# Connect to the database
def get_db_connection():
    DATABASE_URL = os.getenv('PUBLICDOMAINBOOKS_DATABASE_URL')
    if not DATABASE_URL:
        raise ValueError("Database URL not found in environment variables")

    return psycopg2.connect(DATABASE_URL, sslmode='require')


# Initialize database: Create `books` table if not exists
def initialize_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            gutenberg_id INTEGER UNIQUE,
            title TEXT,
            author TEXT,
            subjects TEXT[],
            bookshelves TEXT[],
            language TEXT[],
            download_count INTEGER,
            link TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


# Fetch books from Gutenberg and store in database
@app.get("/fetch_books")
def fetch_books():
    try:

        response = requests.get("https://gutendex.com/books/")
        if response.status_code != 200:
                logging.error("Failed to fetch books: %s", response.status_code)
                return {"error": "Failed to fetch books"}

        books = response.json().get("results", [])

        conn = get_db_connection()
        cur = conn.cursor()

        for book in books:
            gutenberg_id = book.get("id")
            title = book.get("title")
            author = book["authors"][0]["name"] if book["authors"] else "Unknown"
            subjects = book.get("subjects", [])
            bookshelves = book.get("bookshelves", [])
            language = book.get("language", [])
            download_count = book.get("download_count", 0)
            gutenberg_link = book["formats"].get("text/html", "")

            # Insert data into the database
            cur.execute("""
                INSERT INTO books (gutenberg_id, title, author, subjects, bookshelves, language, download_count, link)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (gutenberg_id) DO NOTHING
            """, (gutenberg_id, title, author, subjects, bookshelves, language, download_count, gutenberg_link))

        conn.commit()
        cur.close()
        conn.close()
        
    except requests.exceptions.RequestException as req_err:
        logging.error("HTTP request failed: %s", req_err)
        return {"error": "External API request failed"}

    except psycopg2.Error as db_err:
        logging.error("Database error: %s", db_err)
        return {"error": "Database error occurred"}

    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
        return {"error": str(e)}

    return {"message": "Books successfully stored in the database"}


# Initialize database on startup
initialize_db()

if __name__ == "__main__":
    initialize_db()  # Ensure DB is set up before running
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)