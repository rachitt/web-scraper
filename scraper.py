"""Reddit scraper using public .json endpoints — no API key needed."""

import time
import httpx
from datetime import datetime, timezone

import db
from models import Comment, Post

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
BASE_URL = "https://old.reddit.com"
DELAY_BETWEEN_REQUESTS = 2.0  # be polite


def _get_client() -> httpx.Client:
    return httpx.Client(
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        },
        follow_redirects=True,
        timeout=30.0,
    )


def _fetch_json(client: httpx.Client, url: str) -> dict:
    """Fetch a Reddit .json endpoint with retry on 429."""
    for attempt in range(3):
        resp = client.get(url)
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Failed to fetch {url} after retries")


def _fetch_posts(client: httpx.Client, subreddit: str, sort: str, time_filter: str, limit: int, after: str | None = None) -> tuple[list[dict], str | None]:
    """Fetch a page of posts from a subreddit."""
    url = f"{BASE_URL}/r/{subreddit}/{sort}.json?limit={min(limit, 100)}&t={time_filter}&raw_json=1"
    if after:
        url += f"&after={after}"

    data = _fetch_json(client, url)
    posts = data.get("data", {}).get("children", [])
    next_after = data.get("data", {}).get("after")
    return posts, next_after


def _fetch_comments(client: httpx.Client, subreddit: str, post_id: str, max_depth: int) -> list[dict]:
    """Fetch comments for a post."""
    url = f"{BASE_URL}/r/{subreddit}/comments/{post_id}.json?limit=200&depth={max_depth}&raw_json=1"
    data = _fetch_json(client, url)

    if not isinstance(data, list) or len(data) < 2:
        return []

    comments = []
    _flatten_comments(data[1].get("data", {}).get("children", []), comments, max_depth, depth=0)
    return comments


def _flatten_comments(children: list[dict], out: list[dict], max_depth: int, depth: int) -> None:
    """Recursively flatten comment tree."""
    for child in children:
        if child.get("kind") != "t1":
            continue
        cdata = child.get("data", {})
        out.append({
            "id": cdata.get("id", ""),
            "parent_id": cdata.get("parent_id", ""),
            "body": cdata.get("body", ""),
            "author": cdata.get("author", "[deleted]"),
            "score": cdata.get("score", 0),
            "depth": depth,
            "created_utc": cdata.get("created_utc", 0),
        })
        # Recurse into replies
        replies = cdata.get("replies")
        if isinstance(replies, dict) and depth < max_depth:
            reply_children = replies.get("data", {}).get("children", [])
            _flatten_comments(reply_children, out, max_depth, depth + 1)


def scrape_subreddit(
    config: dict,
    db_path: str,
    subreddit_name: str,
    limit: int | None = None,
) -> dict:
    reddit_cfg = config.get("reddit", {})
    sort = reddit_cfg.get("sort", "hot")
    time_filter = reddit_cfg.get("time_filter", "week")
    limit = limit or reddit_cfg.get("limit", 50)
    comments_cfg = reddit_cfg.get("comments", {})
    max_depth = comments_cfg.get("max_depth", 10)
    fetch_comments = comments_cfg.get("enabled", True)

    stats = {"posts_new": 0, "posts_skipped": 0, "comments_new": 0}
    now = datetime.now(timezone.utc).isoformat()
    fetched = 0
    after = None

    client = _get_client()
    try:
        while fetched < limit:
            remaining = limit - fetched
            posts, after = _fetch_posts(client, subreddit_name, sort, time_filter, remaining, after)

            if not posts:
                break

            for post_data in posts:
                pdata = post_data.get("data", {})
                post_id = pdata.get("id", "")

                if db.post_exists(db_path, post_id):
                    stats["posts_skipped"] += 1
                    fetched += 1
                    continue

                post = Post(
                    id=post_id,
                    subreddit=subreddit_name,
                    title=pdata.get("title", ""),
                    selftext=pdata.get("selftext", ""),
                    author=pdata.get("author", "[deleted]"),
                    score=pdata.get("score", 0),
                    num_comments=pdata.get("num_comments", 0),
                    url=pdata.get("url", ""),
                    created_utc=pdata.get("created_utc", 0),
                    scraped_at=now,
                )
                db.insert_post(db_path, post)
                stats["posts_new"] += 1
                fetched += 1

                # Fetch comments for this post
                if fetch_comments:
                    time.sleep(DELAY_BETWEEN_REQUESTS)
                    raw_comments = _fetch_comments(client, subreddit_name, post_id, max_depth)
                    for cdata in raw_comments:
                        comment = Comment(
                            id=cdata["id"],
                            post_id=post_id,
                            parent_id=cdata["parent_id"],
                            body=cdata["body"],
                            author=cdata["author"],
                            score=cdata["score"],
                            depth=cdata["depth"],
                            created_utc=cdata["created_utc"],
                            scraped_at=now,
                        )
                        if db.insert_comment(db_path, comment):
                            stats["comments_new"] += 1

            if not after:
                break

            time.sleep(DELAY_BETWEEN_REQUESTS)
    finally:
        client.close()

    return stats


def scrape_all(config: dict, db_path: str) -> dict:
    all_stats = {}
    for subreddit in config["reddit"]["subreddits"]:
        print(f"Scraping r/{subreddit}...")
        stats = scrape_subreddit(config, db_path, subreddit)
        all_stats[subreddit] = stats
        print(f"  {stats['posts_new']} new posts, {stats['posts_skipped']} skipped, {stats['comments_new']} new comments")
    return all_stats
