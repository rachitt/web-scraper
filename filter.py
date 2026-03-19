"""Pre-filter posts and comments for pain points before LLM analysis."""

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
    "pain point",
    "deal breaker",
    "waste of time",
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
    "am i the only one",
    "how do you deal with",
    "there's got to be",
    "i can't find",
    "nothing works",
    "every time i try",
    "so expensive",
    "rip off",
    "overpriced",
    "no alternative",
    "why is it so hard",
]

# Title patterns to skip (promotion threads, weekly megathreads, etc.)
BLACKLIST_PATTERNS = [
    r"(?i)promote your business",
    r"(?i)marketplace\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)",
    r"(?i)weekly\s+(self[- ]?promotion|promo|thread|discussion)",
    r"(?i)share your (startup|project|business)",
    r"(?i)feedback friday",
    r"(?i)quick questions",
]


def _is_blacklisted(title: str) -> bool:
    """Check if a post title matches a blacklisted pattern."""
    return any(re.search(pat, title) for pat in BLACKLIST_PATTERNS)


def _count_keyword_matches(text: str, keywords: list[str]) -> int:
    """Count how many distinct pain keywords appear in text."""
    text_lower = text.lower()
    return sum(1 for kw in keywords if kw.lower() in text_lower)


def filter_posts(config: dict) -> dict:
    """Filter posts by keyword matches and engagement thresholds.

    Posts pass if:
    - Not from a blacklisted thread
    - At least 1 pain keyword match
    - Score >= 5 AND comments >= 3
    OR
    - High engagement (score >= 20) even with 0 keyword matches (the post itself IS the complaint)
    """
    db = get_db()
    filter_cfg = config.get("filter", {})
    keywords = filter_cfg.get("pain_keywords", DEFAULT_PAIN_KEYWORDS)
    thresholds = filter_cfg.get("thresholds", {})
    keyword_threshold = thresholds.get("posts", 1)
    post_min_score = filter_cfg.get("post_min_score", 5)
    post_min_comments = filter_cfg.get("post_min_comments", 3)
    post_high_engagement = filter_cfg.get("post_high_engagement", 20)

    posts = db.execute(
        "SELECT id, title, selftext, score, num_comments FROM posts WHERE is_pain_point IS NULL"
    ).fetchall()

    passed = 0
    failed = 0

    for post in posts:
        title = post["title"] or ""

        # Skip promotion/megathreads
        if _is_blacklisted(title):
            db.execute("UPDATE posts SET is_pain_point = 0 WHERE id = ?", (post["id"],))
            failed += 1
            continue

        text = f"{title} {post['selftext'] or ''}"
        matches = _count_keyword_matches(text, keywords)
        score = post["score"] or 0
        comments = post["num_comments"] or 0

        # Pass: keyword match + decent engagement
        keyword_pass = matches >= keyword_threshold and score >= post_min_score and comments >= post_min_comments
        # Pass: high engagement even without keywords (the post IS the complaint)
        engagement_pass = score >= post_high_engagement and comments >= post_min_comments

        if keyword_pass or engagement_pass:
            db.execute("UPDATE posts SET is_pain_point = 1 WHERE id = ?", (post["id"],))
            passed += 1
        else:
            db.execute("UPDATE posts SET is_pain_point = 0 WHERE id = ?", (post["id"],))
            failed += 1

    db.commit()
    db.close()
    return {"passed": passed, "failed": failed, "total": passed + failed}


def filter_comments(config: dict) -> dict:
    """Filter comments by keyword matches and engagement.

    Comments from blacklisted parent posts are skipped.
    Minimum score raised to 10 to filter noise.
    """
    db = get_db()
    filter_cfg = config.get("filter", {})
    keywords = filter_cfg.get("pain_keywords", DEFAULT_PAIN_KEYWORDS)
    thresholds = filter_cfg.get("thresholds", {})
    keyword_threshold = thresholds.get("comments", 1)
    comment_min_score = filter_cfg.get("comment_min_score", 10)

    comments = db.execute(
        """SELECT c.id, c.body, c.score, p.title as post_title
           FROM comments c
           LEFT JOIN posts p ON c.post_id = p.id
           WHERE c.is_pain_point IS NULL"""
    ).fetchall()

    passed = 0
    failed = 0

    for comment in comments:
        post_title = comment["post_title"] or ""

        # Skip comments from blacklisted threads
        if _is_blacklisted(post_title):
            db.execute("UPDATE comments SET is_pain_point = 0 WHERE id = ?", (comment["id"],))
            failed += 1
            continue

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


def run_all_filters(config: dict) -> dict:
    """Run filters on all content types. Returns combined results."""
    results = {
        "posts": filter_posts(config),
        "comments": filter_comments(config),
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
