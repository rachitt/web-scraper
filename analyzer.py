"""Claude API batch analysis of pre-filtered pain point content."""

import json
import sys
import time
from datetime import date

import anthropic

from db import get_db

MODEL = "claude-sonnet-4-20250514"
BATCH_SIZE = 8
CATEGORIES = [
    "productivity",
    "finance",
    "health",
    "communication",
    "developer-tools",
    "education",
    "other",
]

ANALYSIS_PROMPT = """You are analyzing social media content to identify software product opportunities from user pain points.

For each item below, determine if it expresses a genuine pain point that could be solved with a software product. Return a JSON array with one object per item, in the same order.

Each object must have these fields:
- "item_index": the index number from the input
- "is_valid_pain_point": boolean — false if it's just venting with no actionable problem
- "problem_summary": concise 1-2 sentence summary of the core problem
- "category": one of {categories}
- "frustration_level": 1-10 (how frustrated is the user)
- "solvability_score": 1-10 (how feasible is a software solution)
- "market_size_score": 1-10 (how many people likely have this problem)
- "app_idea": one sentence describing a potential app/tool to solve this

Return ONLY the JSON array, no other text.

Items to analyze:
{items}"""


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token."""
    return len(text) // 4


def _estimate_cost(input_tokens: int, output_tokens: int) -> float:
    """Estimate cost in USD for claude-sonnet-4-20250514."""
    # Sonnet pricing: $3/M input, $15/M output
    return (input_tokens * 3 + output_tokens * 15) / 1_000_000


def _get_api_calls_today(db) -> int:
    """Count API calls made today."""
    today = date.today().isoformat()
    row = db.execute(
        "SELECT COUNT(*) as cnt FROM pain_points WHERE DATE(created_at) = ?", (today,)
    ).fetchone()
    return row["cnt"] if row else 0


def _fetch_unanalyzed_posts(db, limit: int) -> list[dict]:
    rows = db.execute(
        """SELECT p.id, p.title, p.selftext, p.score, p.subreddit
           FROM posts p
           LEFT JOIN pain_points pp ON pp.source_id = p.id AND pp.source_type = 'post'
           WHERE p.is_pain_point = 1 AND pp.id IS NULL
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [
        {
            "source_id": r["id"],
            "source_type": "post",
            "source_platform": "reddit",
            "text": f"[r/{r['subreddit'] or '?'}] {r['title'] or ''}\n{r['selftext'] or ''}".strip(),
            "score": r["score"],
        }
        for r in rows
    ]


def _fetch_unanalyzed_comments(db, limit: int) -> list[dict]:
    rows = db.execute(
        """SELECT c.id, c.body, c.score, p.title as post_title, p.selftext as post_body, p.subreddit
           FROM comments c
           LEFT JOIN posts p ON c.post_id = p.id
           LEFT JOIN pain_points pp ON pp.source_id = c.id AND pp.source_type = 'comment'
           WHERE c.is_pain_point = 1 AND pp.id IS NULL
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [
        {
            "source_id": r["id"],
            "source_type": "comment",
            "source_platform": "reddit",
            "text": (
                f"[Parent post in r/{r['subreddit'] or '?'}: {r['post_title'] or 'N/A'}]\n"
                f"Context: {(r['post_body'] or '')[:200]}\n\n"
                f"Comment: {r['body'] or ''}"
            ).strip(),
            "score": r["score"],
        }
        for r in rows
    ]


def _fetch_unanalyzed_tweets(db, limit: int) -> list[dict]:
    rows = db.execute(
        """SELECT t.id, t.text, t.likes, t.search_query
           FROM tweets t
           LEFT JOIN pain_points pp ON pp.source_id = t.id AND pp.source_type = 'tweet'
           WHERE t.is_pain_point = 1 AND pp.id IS NULL
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [
        {
            "source_id": r["id"],
            "source_type": "tweet",
            "source_platform": "x",
            "text": (
                f"[Found via search: {r['search_query'] or 'N/A'}]\n"
                f"Tweet: {r['text'] or ''}"
            ).strip(),
            "score": r["likes"],
        }
        for r in rows
    ]


def _compute_frequency_score(db, problem_summary: str, source_platform: str) -> int:
    """Score 1-10 based on how many similar pain points exist."""
    keywords = [w for w in problem_summary.lower().split() if len(w) > 3]
    if not keywords:
        return 1

    all_summaries = db.execute("SELECT problem_summary FROM pain_points").fetchall()
    similar_count = 0
    for row in all_summaries:
        existing = (row["problem_summary"] or "").lower()
        overlap = sum(1 for kw in keywords if kw in existing)
        if overlap >= len(keywords) * 0.3:
            similar_count += 1

    if similar_count <= 1:
        return 1
    elif similar_count <= 3:
        return 3
    elif similar_count <= 5:
        return 5
    elif similar_count <= 10:
        return 7
    else:
        return min(10, similar_count)


def _compute_opportunity_score(
    frustration: int, solvability: int, market_size: int, frequency: int
) -> float:
    """Composite opportunity score as average of components."""
    return round((frustration + solvability + market_size + frequency) / 4, 2)


def analyze_batch(items: list[dict], config: dict, dry_run: bool = False) -> list[dict]:
    """Send a batch of items to Claude for analysis."""
    if not items:
        return []

    items_text = ""
    for i, item in enumerate(items):
        items_text += f"\n--- Item {i} ({item['source_type']} from {item['source_platform']}) ---\n"
        items_text += item["text"] + "\n"

    prompt = ANALYSIS_PROMPT.format(
        categories=", ".join(CATEGORIES),
        items=items_text,
    )

    input_tokens = _estimate_tokens(prompt)
    output_tokens = len(items) * 100
    estimated_cost = _estimate_cost(input_tokens, output_tokens)

    if dry_run:
        print(f"\n[DRY RUN] Would send {len(items)} items to {MODEL}")
        print(f"  Estimated input tokens:  {input_tokens:,}")
        print(f"  Estimated output tokens: {output_tokens:,}")
        print(f"  Estimated cost:          ${estimated_cost:.4f}")
        print(f"\n  Items:")
        for item in items:
            preview = item["text"][:80].replace("\n", " ")
            print(f"    - [{item['source_type']}/{item['source_platform']}] {preview}...")
        return []

    client = anthropic.Anthropic()
    response = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = response.content[0].text
    response_text = response_text.strip()
    if response_text.startswith("```"):
        response_text = response_text.split("\n", 1)[1]
        response_text = response_text.rsplit("```", 1)[0]

    results = json.loads(response_text)
    return results


def run_analysis(config: dict, dry_run: bool = False) -> dict:
    """Run full analysis pipeline on all unanalyzed items."""
    db = get_db()
    claude_cfg = config.get("claude", {})
    daily_cap = claude_cfg.get("daily_call_cap", 50)
    batch_size = claude_cfg.get("batch_size", BATCH_SIZE)

    calls_today = _get_api_calls_today(db)
    remaining_budget = max(0, daily_cap - calls_today)

    if remaining_budget == 0 and not dry_run:
        print(f"Daily API cap reached ({daily_cap} calls). Skipping analysis.")
        db.close()
        return {"analyzed": 0, "skipped_cap": True}

    # Gather all unanalyzed items
    all_items = []
    all_items.extend(_fetch_unanalyzed_posts(db, remaining_budget))
    all_items.extend(_fetch_unanalyzed_comments(db, remaining_budget - len(all_items)))
    all_items.extend(_fetch_unanalyzed_tweets(db, remaining_budget - len(all_items)))

    if not all_items:
        print("No unanalyzed items found.")
        db.close()
        return {"analyzed": 0, "skipped_cap": False}

    all_items = all_items[:remaining_budget]

    total_analyzed = 0

    for batch_start in range(0, len(all_items), batch_size):
        batch = all_items[batch_start : batch_start + batch_size]
        results = analyze_batch(batch, config, dry_run=dry_run)

        if dry_run:
            continue

        for result in results:
            idx = result.get("item_index", 0)
            if idx >= len(batch):
                continue

            item = batch[idx]

            if not result.get("is_valid_pain_point", True):
                table = (
                    "posts"
                    if item["source_type"] == "post"
                    else "comments"
                    if item["source_type"] == "comment"
                    else "tweets"
                )
                db.execute(
                    f"UPDATE {table} SET is_pain_point = 0 WHERE id = ?",
                    (item["source_id"],),
                )
                continue

            frequency = _compute_frequency_score(
                db, result["problem_summary"], item["source_platform"]
            )
            opportunity = _compute_opportunity_score(
                result["frustration_level"],
                result["solvability_score"],
                result["market_size_score"],
                frequency,
            )

            db.execute(
                """INSERT INTO pain_points
                   (source_id, source_type, source_platform, problem_summary, category,
                    frustration_level, solvability_score, market_size_score,
                    frequency_score, opportunity_score, app_idea, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
                (
                    item["source_id"],
                    item["source_type"],
                    item["source_platform"],
                    result["problem_summary"],
                    result["category"],
                    result["frustration_level"],
                    result["solvability_score"],
                    result["market_size_score"],
                    frequency,
                    opportunity,
                    result["app_idea"],
                ),
            )
            total_analyzed += 1

        db.commit()

    db.close()
    return {
        "analyzed": total_analyzed,
        "total_items": len(all_items),
        "skipped_cap": False,
        "dry_run": dry_run,
    }
