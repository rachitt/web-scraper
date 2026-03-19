"""Reporting and output for pain point analysis results."""

import csv

from tabulate import tabulate

from db import get_db


def get_pain_points(
    category: str | None = None,
    min_score: float | None = None,
    validated_only: bool = False,
    platform: str = "all",
    limit: int = 50,
) -> list[dict]:
    """Query pain points with optional filters."""
    db = get_db()

    conditions = []
    params = []

    if category:
        conditions.append("pp.category = ?")
        params.append(category)

    if min_score is not None:
        conditions.append("pp.opportunity_score >= ?")
        params.append(min_score)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
        SELECT pp.id, pp.opportunity_score, pp.problem_summary, pp.category,
               pp.source_platform, pp.source_type,
               pp.app_idea, pp.frustration_level, pp.solvability_score,
               pp.market_size_score, pp.frequency_score
        FROM pain_points pp
        {where}
        ORDER BY pp.opportunity_score DESC
        LIMIT ?
    """
    params.append(limit)

    results = [dict(r) for r in db.execute(query, params).fetchall()]
    db.close()
    return results


def print_table(pain_points: list[dict]) -> None:
    """Print pain points as a formatted terminal table."""
    if not pain_points:
        print("No pain points found matching filters.")
        return

    headers = ["#", "Score", "Problem", "Category", "Type", "App Idea"]
    rows = []

    for i, pp in enumerate(pain_points, 1):
        summary = pp["problem_summary"] or ""
        if len(summary) > 60:
            summary = summary[:57] + "..."

        idea = pp["app_idea"] or ""
        if len(idea) > 45:
            idea = idea[:42] + "..."

        rows.append([
            i,
            pp["opportunity_score"],
            summary,
            pp["category"],
            pp["source_type"],
            idea,
        ])

    print(tabulate(rows, headers=headers, tablefmt="simple", floatfmt=".2f"))
    print(f"\n{len(pain_points)} results shown")


def export_csv(pain_points: list[dict], output_path: str) -> None:
    """Export pain points to CSV file."""
    if not pain_points:
        print("No pain points to export.")
        return

    fieldnames = [
        "rank",
        "opportunity_score",
        "problem_summary",
        "category",
        "source_type",
        "frustration_level",
        "solvability_score",
        "market_size_score",
        "frequency_score",
        "app_idea",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i, pp in enumerate(pain_points, 1):
            row = {k: pp.get(k, "") for k in fieldnames}
            row["rank"] = i
            writer.writerow(row)

    print(f"Exported {len(pain_points)} pain points to {output_path}")


def print_detailed_stats() -> None:
    """Print summary statistics across all data."""
    db = get_db()

    print("=" * 60)
    print("PIPELINE STATS")
    print("=" * 60)

    post_stats = db.execute(
        """SELECT
            COUNT(*) as total,
            SUM(CASE WHEN is_pain_point IS NULL THEN 1 ELSE 0 END) as unprocessed,
            SUM(CASE WHEN is_pain_point = 0 THEN 1 ELSE 0 END) as filtered,
            SUM(CASE WHEN is_pain_point = 1 THEN 1 ELSE 0 END) as passed
           FROM posts"""
    ).fetchone()

    comment_stats = db.execute(
        """SELECT
            COUNT(*) as total,
            SUM(CASE WHEN is_pain_point IS NULL THEN 1 ELSE 0 END) as unprocessed,
            SUM(CASE WHEN is_pain_point = 0 THEN 1 ELSE 0 END) as filtered,
            SUM(CASE WHEN is_pain_point = 1 THEN 1 ELSE 0 END) as passed
           FROM comments"""
    ).fetchone()

    content_rows = [
        ["Posts", post_stats["total"], post_stats["unprocessed"], post_stats["filtered"], post_stats["passed"]],
        ["Comments", comment_stats["total"], comment_stats["unprocessed"], comment_stats["filtered"], comment_stats["passed"]],
    ]
    print("\nContent Pipeline:")
    print(tabulate(content_rows, headers=["Type", "Total", "Unprocessed", "Filtered", "Passed"], tablefmt="simple"))

    category_stats = db.execute(
        """SELECT category, COUNT(*) as count,
                  ROUND(AVG(opportunity_score), 2) as avg_score
           FROM pain_points GROUP BY category ORDER BY count DESC"""
    ).fetchall()

    if category_stats:
        print("\nPain Points by Category:")
        cat_rows = [[r["category"], r["count"], r["avg_score"]] for r in category_stats]
        print(tabulate(cat_rows, headers=["Category", "Count", "Avg Score"], tablefmt="simple"))

    sub_stats = db.execute(
        """SELECT p.subreddit, COUNT(*) as count
           FROM pain_points pp
           JOIN posts p ON pp.source_id = p.id AND pp.source_type = 'post'
           GROUP BY p.subreddit ORDER BY count DESC LIMIT 10"""
    ).fetchall()

    if sub_stats:
        print("\nTop Subreddits:")
        sub_rows = [[r["subreddit"], r["count"]] for r in sub_stats]
        print(tabulate(sub_rows, headers=["Subreddit", "Pain Points"], tablefmt="simple"))

    print()
    db.close()
