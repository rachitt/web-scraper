"""Export filtered items for Claude Code analysis and import results back."""

import json
import os
from datetime import datetime, timezone

from db import get_db

CATEGORIES = [
    "productivity",
    "finance",
    "health",
    "communication",
    "developer-tools",
    "education",
    "other",
]

EXPORT_PATH = "data/analyze_input.json"
IMPORT_PATH = "data/analyze_results.json"


def _fetch_unanalyzed_posts(db, limit: int) -> list[dict]:
    """Fetch posts sorted by engagement (score * comments)."""
    rows = db.execute(
        """SELECT p.id, p.title, p.selftext, p.score, p.subreddit, p.num_comments
           FROM posts p
           LEFT JOIN pain_points pp ON pp.source_id = p.id AND pp.source_type = 'post'
           WHERE p.is_pain_point = 1 AND pp.id IS NULL
           ORDER BY p.score * p.num_comments DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [
        {
            "item_index": None,
            "source_id": r["id"],
            "source_type": "post",
            "source_platform": "reddit",
            "subreddit": r["subreddit"],
            "text": f"[r/{r['subreddit'] or '?'}] {r['title'] or ''}\n{r['selftext'] or ''}".strip(),
            "score": r["score"],
            "engagement": r["score"] * (r["num_comments"] or 1),
        }
        for r in rows
    ]


def _fetch_unanalyzed_comments(db, limit: int) -> list[dict]:
    """Fetch comments sorted by score (highest upvoted = strongest signal)."""
    rows = db.execute(
        """SELECT c.id, c.body, c.score, p.title as post_title, p.selftext as post_body, p.subreddit
           FROM comments c
           LEFT JOIN posts p ON c.post_id = p.id
           LEFT JOIN pain_points pp ON pp.source_id = c.id AND pp.source_type = 'comment'
           WHERE c.is_pain_point = 1 AND pp.id IS NULL
           ORDER BY c.score DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [
        {
            "item_index": None,
            "source_id": r["id"],
            "source_type": "comment",
            "source_platform": "reddit",
            "subreddit": r["subreddit"],
            "text": (
                f"[Parent post in r/{r['subreddit'] or '?'}: {r['post_title'] or 'N/A'}]\n"
                f"Context: {(r['post_body'] or '')[:200]}\n\n"
                f"Comment: {r['body'] or ''}"
            ).strip(),
            "score": r["score"],
            "engagement": r["score"],
        }
        for r in rows
    ]


def export_for_analysis(config: dict, output_path: str | None = None) -> dict:
    """Export unanalyzed items sorted by engagement for Claude Code to analyze."""
    db = get_db()
    limit = config.get("analysis", {}).get("batch_size", 100)
    output_path = output_path or EXPORT_PATH

    # Fetch both, then merge and sort by engagement
    posts = _fetch_unanalyzed_posts(db, limit)
    comments = _fetch_unanalyzed_comments(db, limit)
    db.close()

    all_items = posts + comments
    all_items.sort(key=lambda x: x.get("engagement", 0), reverse=True)
    all_items = all_items[:limit]

    if not all_items:
        print("No unanalyzed items found.")
        return {"exported": 0, "path": output_path}

    for i, item in enumerate(all_items):
        item["item_index"] = i

    export_data = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "total_items": len(all_items),
        "categories": CATEGORIES,
        "items": all_items,
        "instructions": (
            "Analyze each item. For each, return a JSON object with: "
            "item_index, is_valid_pain_point (bool), problem_summary (1-2 sentences), "
            f"category (one of: {', '.join(CATEGORIES)}), "
            "frustration_level (1-10), solvability_score (1-10), "
            "market_size_score (1-10), app_idea (one sentence). "
            "Save the results array to data/analyze_results.json."
        ),
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(export_data, f, indent=2)

    print(f"Exported {len(all_items)} items to {output_path}")
    return {"exported": len(all_items), "path": output_path}


def _compute_frequency_score(db, problem_summary: str) -> int:
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


def import_results(config: dict, input_path: str | None = None) -> dict:
    """Import Claude Code analysis results from JSON back into the database."""
    db = get_db()
    input_path = input_path or IMPORT_PATH

    with open(EXPORT_PATH) as f:
        export_data = json.load(f)
    items_by_index = {item["item_index"]: item for item in export_data["items"]}

    with open(input_path) as f:
        results = json.load(f)

    if isinstance(results, dict):
        results = results.get("results", results.get("items", []))

    total_imported = 0
    total_rejected = 0

    for result in results:
        idx = result.get("item_index")
        item = items_by_index.get(idx)
        if item is None:
            print(f"Warning: item_index {idx} not found in export, skipping")
            continue

        if not result.get("is_valid_pain_point", True):
            table = {"post": "posts", "comment": "comments"}[item["source_type"]]
            db.execute(
                f"UPDATE {table} SET is_pain_point = 0 WHERE id = ?",
                (item["source_id"],),
            )
            total_rejected += 1
            continue

        frequency = _compute_frequency_score(db, result["problem_summary"])
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
        total_imported += 1

    db.commit()
    db.close()

    print(f"Imported {total_imported} pain points, rejected {total_rejected}")
    return {
        "imported": total_imported,
        "rejected": total_rejected,
        "total": total_imported + total_rejected,
    }
