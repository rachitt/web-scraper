import praw
from datetime import datetime, timezone

import db
from models import Comment, Post


def _build_client(config: dict) -> praw.Reddit:
    return praw.Reddit(
        client_id=config["reddit"]["client_id"],
        client_secret=config["reddit"]["client_secret"],
        user_agent=config["reddit"]["user_agent"],
    )


def scrape_subreddit(
    config: dict,
    db_path: str,
    subreddit_name: str,
    limit: int | None = None,
) -> dict:
    reddit = _build_client(config)
    sub = reddit.subreddit(subreddit_name)
    sort = config["reddit"].get("sort", "hot")
    time_filter = config["reddit"].get("time_filter", "week")
    limit = limit or config["reddit"].get("limit", 50)
    max_depth = config["reddit"]["comments"].get("max_depth", 10)
    replace_more_limit = config["reddit"]["comments"].get("replace_more_limit", 0)

    if sort == "top":
        submissions = sub.top(time_filter=time_filter, limit=limit)
    elif sort == "new":
        submissions = sub.new(limit=limit)
    else:
        submissions = sub.hot(limit=limit)

    stats = {"posts_new": 0, "posts_skipped": 0, "comments_new": 0}
    now = datetime.now(timezone.utc).isoformat()

    for submission in submissions:
        if db.post_exists(db_path, submission.id):
            stats["posts_skipped"] += 1
            continue

        post = Post(
            id=submission.id,
            subreddit=subreddit_name,
            title=submission.title,
            selftext=submission.selftext or "",
            author=str(submission.author) if submission.author else "[deleted]",
            score=submission.score,
            num_comments=submission.num_comments,
            url=submission.url,
            created_utc=submission.created_utc,
            scraped_at=now,
        )
        db.insert_post(db_path, post)
        stats["posts_new"] += 1

        # Fetch comments
        submission.comments.replace_more(limit=replace_more_limit)
        comment_list = submission.comments.list()
        for c in comment_list:
            if not hasattr(c, "body"):
                continue
            depth = _get_comment_depth(c)
            if depth > max_depth:
                continue
            comment = Comment(
                id=c.id,
                post_id=submission.id,
                parent_id=c.parent_id,
                body=c.body,
                author=str(c.author) if c.author else "[deleted]",
                score=c.score,
                depth=depth,
                created_utc=c.created_utc,
                scraped_at=now,
            )
            if db.insert_comment(db_path, comment):
                stats["comments_new"] += 1

    return stats


def _get_comment_depth(comment) -> int:
    depth = 0
    c = comment
    while not c.is_root:
        depth += 1
        c = c.parent()
    return depth


def scrape_all(config: dict, db_path: str) -> dict:
    all_stats = {}
    for subreddit in config["reddit"]["subreddits"]:
        print(f"Scraping r/{subreddit}...")
        stats = scrape_subreddit(config, db_path, subreddit)
        all_stats[subreddit] = stats
        print(f"  {stats['posts_new']} new posts, {stats['posts_skipped']} skipped, {stats['comments_new']} new comments")
    return all_stats
