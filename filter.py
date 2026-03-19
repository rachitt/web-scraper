"""Pre-filter posts, comments, and tweets for pain points before LLM analysis."""

import re
from db import get_db

DEFAULT_PAIN_KEYWORDS = [
    "frustrated",
    "frustrating",
    "wish there was",
    "why can't",
    "why isn't there",
    "hate when",
    "struggling with",
    "so tired of",
    "drives me crazy",
    "there has to be a better way",
    "sick of",
    "fed up",
    "annoying",
    "pain point",
    "deal breaker",
    "waste of time",
    "broken",
    "unusable",
    "terrible experience",
    "nightmare",
    "impossible to",
    "no good way to",
    "can't believe there's no",
    "someone needs to build",
    "would pay for",
    "shut up and take my money",
    "is there an app",
    "any tool that",
    "how do you deal with",
    "am i the only one",
]


def _count_keyword_matches(text: str, keywords: list[str]) -> int:
    """Count how many distinct pain keywords appear in text."""
    text_lower = text.lower()
    return sum(1 for kw in keywords if kw.lower() in text_lower)


def filter_posts(config: dict) -> dict:
    """Filter posts by keyword matches and engagement thresholds."""
    db = get_db()
    filter_cfg = config.get("filter", {})
    keywords = filter_cfg.get("pain_keywords", DEFAULT_PAIN_KEYWORDS)
    thresholds = filter_cfg.get("thresholds", {})
    keyword_threshold = thresholds.get("posts", 1)
    post_min_score = config.get("post_min_score", 5)
    post_min_comments = config.get("post_min_comments", 3)

    posts = db.execute(
        "SELECT id, title, selftext, score, num_comments FROM posts WHERE is_pain_point IS NULL"
    ).fetchall()

    passed = 0
    failed = 0

    for post in posts:
        text = f"{post['title'] or ''} {post['selftext'] or ''}"
        matches = _count_keyword_matches(text, keywords)
        score_ok = (post["score"] or 0) >= post_min_score
        comments_ok = (post["num_comments"] or 0) >= post_min_comments

        if matches >= keyword_threshold and score_ok and comments_ok:
            db.execute("UPDATE posts SET is_pain_point = 1 WHERE id = ?", (post["id"],))
            passed += 1
        else:
            db.execute("UPDATE posts SET is_pain_point = 0 WHERE id = ?", (post["id"],))
            failed += 1

    db.commit()
    db.close()
    return {"passed": passed, "failed": failed, "total": passed + failed}


def filter_comments(config: dict) -> dict:
    """Filter comments by keyword matches and engagement thresholds."""
    db = get_db()
    filter_cfg = config.get("filter", {})
    keywords = filter_cfg.get("pain_keywords", DEFAULT_PAIN_KEYWORDS)
    thresholds = filter_cfg.get("thresholds", {})
    keyword_threshold = thresholds.get("comments", 1)
    comment_min_score = config.get("comment_min_score", 2)

    comments = db.execute(
        "SELECT id, body, score FROM comments WHERE is_pain_point IS NULL"
    ).fetchall()

    passed = 0
    failed = 0

    for comment in comments:
        text = comment["body"] or ""
        matches = _count_keyword_matches(text, keywords)
        score_ok = (comment["score"] or 0) >= comment_min_score

        if matches >= keyword_threshold and score_ok:
            db.execute("UPDATE comments SET is_pain_point = 1 WHERE id = ?", (comment["id"],))
            passed += 1
        else:
            db.execute("UPDATE comments SET is_pain_point = 0 WHERE id = ?", (comment["id"],))
            failed += 1

    db.commit()
    db.close()
    return {"passed": passed, "failed": failed, "total": passed + failed}


def filter_tweets(config: dict) -> dict:
    """Filter tweets by keyword matches and engagement thresholds."""
    db = get_db()
    filter_cfg = config.get("filter", {})
    keywords = filter_cfg.get("pain_keywords", DEFAULT_PAIN_KEYWORDS)
    thresholds = filter_cfg.get("thresholds", {})
    keyword_threshold = thresholds.get("tweets", 1)
    tweet_min_likes = config.get("tweet_min_likes", 3)

    tweets = db.execute(
        "SELECT id, text, likes FROM tweets WHERE is_pain_point IS NULL"
    ).fetchall()

    passed = 0
    failed = 0

    for tweet in tweets:
        text = tweet["text"] or ""
        matches = _count_keyword_matches(text, keywords)
        likes_ok = (tweet["likes"] or 0) >= tweet_min_likes

        if matches >= keyword_threshold and likes_ok:
            db.execute("UPDATE tweets SET is_pain_point = 1 WHERE id = ?", (tweet["id"],))
            passed += 1
        else:
            db.execute("UPDATE tweets SET is_pain_point = 0 WHERE id = ?", (tweet["id"],))
            failed += 1

    db.commit()
    db.close()
    return {"passed": passed, "failed": failed, "total": passed + failed}


def run_all_filters(config: dict) -> dict:
    """Run filters on all content types. Returns combined results."""
    results = {
        "posts": filter_posts(config),
        "comments": filter_comments(config),
        "tweets": filter_tweets(config),
    }
    total_passed = sum(r["passed"] for r in results.values())
    total = sum(r["total"] for r in results.values())
    filter_rate = (total - total_passed) / total * 100 if total > 0 else 0
    results["summary"] = {
        "total_processed": total,
        "total_passed": total_passed,
        "total_filtered": total - total_passed,
        "filter_rate_pct": round(filter_rate, 1),
    }
    return results
