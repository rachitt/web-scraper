"""Cross-platform validation of pain points between Reddit and X."""

from db import get_db

DEFAULT_VALIDATION_BOOST = 1.5
MIN_KEYWORD_OVERLAP = 0.3


def _extract_keywords(text: str, min_length: int = 4) -> set[str]:
    """Extract meaningful keywords from text."""
    stop_words = {
        "this", "that", "with", "from", "have", "been", "they", "their",
        "there", "what", "when", "where", "which", "would", "could", "should",
        "about", "just", "like", "really", "very", "some", "than", "them",
        "then", "into", "only", "also", "more", "most", "such", "each",
        "other", "every", "does", "doing", "being", "having",
    }
    words = set()
    for word in text.lower().split():
        cleaned = "".join(c for c in word if c.isalnum())
        if len(cleaned) >= min_length and cleaned not in stop_words:
            words.add(cleaned)
    return words


def _text_similarity(text_a: str, text_b: str) -> float:
    """Compute keyword overlap ratio between two texts."""
    keywords_a = _extract_keywords(text_a)
    keywords_b = _extract_keywords(text_b)

    if not keywords_a or not keywords_b:
        return 0.0

    overlap = keywords_a & keywords_b
    smaller = min(len(keywords_a), len(keywords_b))
    return len(overlap) / smaller if smaller > 0 else 0.0


def _get_reddit_content(db) -> list[dict]:
    """Get original content text for Reddit-sourced pain points."""
    rows = db.execute(
        """SELECT pp.id, pp.problem_summary, pp.source_id, pp.source_type,
                  CASE
                      WHEN pp.source_type = 'post' THEN p.title || ' ' || COALESCE(p.selftext, '')
                      WHEN pp.source_type = 'comment' THEN c.body
                  END as original_text
           FROM pain_points pp
           LEFT JOIN posts p ON pp.source_id = p.id AND pp.source_type = 'post'
           LEFT JOIN comments c ON pp.source_id = c.id AND pp.source_type = 'comment'
           WHERE pp.source_platform = 'reddit'"""
    ).fetchall()
    return [dict(r) for r in rows]


def _get_x_content(db) -> list[dict]:
    """Get original content text for X-sourced pain points."""
    rows = db.execute(
        """SELECT pp.id, pp.problem_summary, pp.source_id, pp.source_type,
                  t.text as original_text
           FROM pain_points pp
           LEFT JOIN tweets t ON pp.source_id = t.id AND pp.source_type = 'tweet'
           WHERE pp.source_platform = 'x'"""
    ).fetchall()
    return [dict(r) for r in rows]


def _search_tweets_for_match(db, keywords: set[str], min_overlap: float) -> bool:
    """Search tweets table for content matching the given keywords."""
    tweets = db.execute("SELECT text FROM tweets WHERE is_pain_point = 1").fetchall()
    for tweet in tweets:
        tweet_keywords = _extract_keywords(tweet["text"] or "")
        if not tweet_keywords:
            continue
        overlap = keywords & tweet_keywords
        smaller = min(len(keywords), len(tweet_keywords))
        if smaller > 0 and len(overlap) / smaller >= min_overlap:
            return True
    return False


def _search_reddit_for_match(db, keywords: set[str], min_overlap: float) -> bool:
    """Search posts and comments tables for content matching keywords."""
    posts = db.execute(
        "SELECT title, selftext FROM posts WHERE is_pain_point = 1"
    ).fetchall()
    for post in posts:
        text = f"{post['title'] or ''} {post['selftext'] or ''}"
        post_keywords = _extract_keywords(text)
        if not post_keywords:
            continue
        overlap = keywords & post_keywords
        smaller = min(len(keywords), len(post_keywords))
        if smaller > 0 and len(overlap) / smaller >= min_overlap:
            return True

    comments = db.execute(
        "SELECT body FROM comments WHERE is_pain_point = 1"
    ).fetchall()
    for comment in comments:
        comment_keywords = _extract_keywords(comment["body"] or "")
        if not comment_keywords:
            continue
        overlap = keywords & comment_keywords
        smaller = min(len(keywords), len(comment_keywords))
        if smaller > 0 and len(overlap) / smaller >= min_overlap:
            return True

    return False


def validate_cross_platform(config: dict) -> dict:
    """Validate pain points across platforms and boost scores."""
    db = get_db()
    validation_cfg = config.get("validation", {})
    boost = validation_cfg.get("cross_platform_score_boost", DEFAULT_VALIDATION_BOOST)
    min_overlap = validation_cfg.get("min_overlap", MIN_KEYWORD_OVERLAP)

    reddit_items = _get_reddit_content(db)
    x_items = _get_x_content(db)

    validated_count = 0
    already_validated = 0

    # Validate Reddit pain points against X content
    for item in reddit_items:
        combined_text = f"{item['problem_summary'] or ''} {item['original_text'] or ''}"
        keywords = _extract_keywords(combined_text)

        if _search_tweets_for_match(db, keywords, min_overlap):
            current = db.execute(
                "SELECT cross_platform_validated, opportunity_score FROM pain_points WHERE id = ?",
                (item["id"],),
            ).fetchone()

            if current["cross_platform_validated"]:
                already_validated += 1
                continue

            boosted_score = round(current["opportunity_score"] * boost, 2)
            db.execute(
                """UPDATE pain_points
                   SET cross_platform_validated = 1, opportunity_score = ?
                   WHERE id = ?""",
                (boosted_score, item["id"]),
            )
            validated_count += 1

    # Validate X pain points against Reddit content
    for item in x_items:
        combined_text = f"{item['problem_summary'] or ''} {item['original_text'] or ''}"
        keywords = _extract_keywords(combined_text)

        if _search_reddit_for_match(db, keywords, min_overlap):
            current = db.execute(
                "SELECT cross_platform_validated, opportunity_score FROM pain_points WHERE id = ?",
                (item["id"],),
            ).fetchone()

            if current["cross_platform_validated"]:
                already_validated += 1
                continue

            boosted_score = round(current["opportunity_score"] * boost, 2)
            db.execute(
                """UPDATE pain_points
                   SET cross_platform_validated = 1, opportunity_score = ?
                   WHERE id = ?""",
                (boosted_score, item["id"]),
            )
            validated_count += 1

    db.commit()
    db.close()

    return {
        "newly_validated": validated_count,
        "already_validated": already_validated,
        "reddit_checked": len(reddit_items),
        "x_checked": len(x_items),
        "boost_multiplier": boost,
    }
