from fastapi import FastAPI, Request
import asyncio
from typing import Optional
import requests
import psycopg2
import os

app = FastAPI()



@app.get("/")
async def home():
    return "Hello, World!"


# Connect to the database
def get_db_connection():
    return psycopg2.connect(os.getenv('PUBLICDOMAINBOOKS_DATABASE_URL'), sslmode='require')


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
            languages TEXT[],
            download_count INTEGER,
            gutenberg_link TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


# Fetch books from Gutenberg and store in database
@app.get("/fetch_books")
def fetch_books():
    response = requests.get("https://gutendex.com/books/")
    if response.status_code != 200:
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
        languages = book.get("languages", [])
        download_count = book.get("download_count", 0)
        gutenberg_link = book["formats"].get("text/html", "")

        # Insert data into the database
        cur.execute("""
            INSERT INTO books (gutenberg_id, title, author, subjects, bookshelves, languages, download_count, gutenberg_link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (gutenberg_id) DO NOTHING
        """, (gutenberg_id, title, author, subjects, bookshelves, languages, download_count, gutenberg_link))

    conn.commit()
    cur.close()
    conn.close()

    return {"message": "Books successfully stored in the database"}


# Initialize database on startup
initialize_db()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app)