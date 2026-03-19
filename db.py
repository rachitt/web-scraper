import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from models import Comment, PainPoint, Post, Tweet


def _get_connection(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str) -> None:
    conn = _get_connection(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            subreddit TEXT NOT NULL,
            title TEXT NOT NULL,
            selftext TEXT,
            author TEXT,
            score INTEGER,
            num_comments INTEGER,
            url TEXT,
            created_utc REAL,
            scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            post_id TEXT NOT NULL,
            parent_id TEXT NOT NULL,
            body TEXT,
            author TEXT,
            score INTEGER,
            depth INTEGER,
            created_utc REAL,
            scraped_at TEXT,
            FOREIGN KEY (post_id) REFERENCES posts(id)
        );

        CREATE TABLE IF NOT EXISTS tweets (
            id TEXT PRIMARY KEY,
            text TEXT,
            author TEXT,
            likes INTEGER,
            retweets INTEGER,
            replies INTEGER,
            url TEXT,
            created_at TEXT,
            scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS pain_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT,
            category TEXT,
            severity REAL,
            frequency REAL,
            market_size TEXT,
            source_platform TEXT,
            source_type TEXT,
            source_id TEXT,
            source_text TEXT,
            subreddit TEXT,
            confidence REAL,
            cross_platform_validated BOOLEAN DEFAULT 0,
            cluster_id INTEGER,
            created_at TEXT,
            tags TEXT
        );

        CREATE TABLE IF NOT EXISTS clusters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            description TEXT,
            pain_point_count INTEGER DEFAULT 0,
            avg_severity REAL,
            created_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id);
        CREATE INDEX IF NOT EXISTS idx_pain_points_category ON pain_points(category);
        CREATE INDEX IF NOT EXISTS idx_pain_points_source ON pain_points(source_platform, source_type);
        CREATE INDEX IF NOT EXISTS idx_pain_points_cluster ON pain_points(cluster_id);
    """)
    conn.close()


# --- Posts ---

def insert_post(db_path: str, post: Post) -> bool:
    conn = _get_connection(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO posts (id, subreddit, title, selftext, author, score, num_comments, url, created_utc, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (post.id, post.subreddit, post.title, post.selftext, post.author,
             post.score, post.num_comments, post.url, post.created_utc,
             post.scraped_at or datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return conn.total_changes > 0
    finally:
        conn.close()


def post_exists(db_path: str, post_id: str) -> bool:
    conn = _get_connection(db_path)
    try:
        row = conn.execute("SELECT 1 FROM posts WHERE id = ?", (post_id,)).fetchone()
        return row is not None
    finally:
        conn.close()


def get_posts(db_path: str, subreddit: Optional[str] = None, limit: int = 100) -> list[Post]:
    conn = _get_connection(db_path)
    try:
        if subreddit:
            rows = conn.execute(
                "SELECT * FROM posts WHERE subreddit = ? ORDER BY created_utc DESC LIMIT ?",
                (subreddit, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM posts ORDER BY created_utc DESC LIMIT ?", (limit,)
            ).fetchall()
        return [Post(**dict(r)) for r in rows]
    finally:
        conn.close()


# --- Comments ---

def insert_comment(db_path: str, comment: Comment) -> bool:
    conn = _get_connection(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO comments (id, post_id, parent_id, body, author, score, depth, created_utc, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (comment.id, comment.post_id, comment.parent_id, comment.body,
             comment.author, comment.score, comment.depth, comment.created_utc,
             comment.scraped_at or datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return conn.total_changes > 0
    finally:
        conn.close()


def get_comments(db_path: str, post_id: str) -> list[Comment]:
    conn = _get_connection(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM comments WHERE post_id = ? ORDER BY depth, created_utc", (post_id,)
        ).fetchall()
        return [Comment(**dict(r)) for r in rows]
    finally:
        conn.close()


# --- Tweets ---

def insert_tweet(db_path: str, tweet: Tweet) -> bool:
    conn = _get_connection(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO tweets (id, text, author, likes, retweets, replies, url, created_at, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (tweet.id, tweet.text, tweet.author, tweet.likes, tweet.retweets,
             tweet.replies, tweet.url, tweet.created_at,
             tweet.scraped_at or datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        return conn.total_changes > 0
    finally:
        conn.close()


def get_tweets(db_path: str, limit: int = 100) -> list[Tweet]:
    conn = _get_connection(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM tweets ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [Tweet(**dict(r)) for r in rows]
    finally:
        conn.close()


# --- Pain Points ---

def insert_pain_point(db_path: str, pp: PainPoint) -> int:
    conn = _get_connection(db_path)
    try:
        cursor = conn.execute(
            "INSERT INTO pain_points (description, category, severity, frequency, market_size, source_platform, source_type, source_id, source_text, subreddit, confidence, cross_platform_validated, cluster_id, created_at, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (pp.description, pp.category, pp.severity, pp.frequency,
             pp.market_size, pp.source_platform, pp.source_type, pp.source_id,
             pp.source_text, pp.subreddit, pp.confidence,
             pp.cross_platform_validated, pp.cluster_id,
             pp.created_at or datetime.now(timezone.utc).isoformat(),
             ",".join(pp.tags)),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def get_pain_points(
    db_path: str,
    category: Optional[str] = None,
    min_score: float = 0.0,
    validated_only: bool = False,
    limit: int = 100,
) -> list[PainPoint]:
    conn = _get_connection(db_path)
    try:
        query = "SELECT * FROM pain_points WHERE severity >= ?"
        params: list = [min_score]
        if category:
            query += " AND category = ?"
            params.append(category)
        if validated_only:
            query += " AND cross_platform_validated = 1"
        query += " ORDER BY severity DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            d["cross_platform_validated"] = bool(d["cross_platform_validated"])
            d["tags"] = d["tags"].split(",") if d["tags"] else []
            results.append(PainPoint(**d))
        return results
    finally:
        conn.close()


def update_pain_point_cluster(db_path: str, pain_point_id: int, cluster_id: int) -> None:
    conn = _get_connection(db_path)
    try:
        conn.execute(
            "UPDATE pain_points SET cluster_id = ? WHERE id = ?",
            (cluster_id, pain_point_id),
        )
        conn.commit()
    finally:
        conn.close()


def update_pain_point_validated(db_path: str, pain_point_id: int, validated: bool) -> None:
    conn = _get_connection(db_path)
    try:
        conn.execute(
            "UPDATE pain_points SET cross_platform_validated = ? WHERE id = ?",
            (validated, pain_point_id),
        )
        conn.commit()
    finally:
        conn.close()


# --- Stats ---

def get_stats(db_path: str) -> dict:
    conn = _get_connection(db_path)
    try:
        posts = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        comments = conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
        tweets = conn.execute("SELECT COUNT(*) FROM tweets").fetchone()[0]
        pain_points = conn.execute("SELECT COUNT(*) FROM pain_points").fetchone()[0]
        validated = conn.execute(
            "SELECT COUNT(*) FROM pain_points WHERE cross_platform_validated = 1"
        ).fetchone()[0]
        subreddits = conn.execute("SELECT DISTINCT subreddit FROM posts").fetchall()
        return {
            "posts": posts,
            "comments": comments,
            "tweets": tweets,
            "pain_points": pain_points,
            "validated_pain_points": validated,
            "subreddits": [r[0] for r in subreddits],
        }
    finally:
        conn.close()
