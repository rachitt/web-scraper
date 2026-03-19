#!/usr/bin/env python3
import argparse
import os
import re
import sys

import yaml
from tabulate import tabulate

import db
import scraper


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        raw = f.read()
    # Substitute ${ENV_VAR} with environment variable values
    def _env_sub(match):
        var = match.group(1)
        val = os.environ.get(var, "")
        if not val:
            print(f"Warning: environment variable {var} is not set", file=sys.stderr)
        return val
    raw = re.sub(r"\$\{(\w+)\}", _env_sub, raw)
    return yaml.safe_load(raw)


def cmd_scrape(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    subreddit = args.subreddit
    limit = args.limit
    if not subreddit:
        print("Error: --subreddit is required for 'scrape'", file=sys.stderr)
        sys.exit(1)
    print(f"Scraping r/{subreddit} (limit={limit or config['reddit']['limit']})...")
    stats = scraper.scrape_subreddit(config, db_path, subreddit, limit)
    print(f"Done: {stats['posts_new']} new posts, {stats['posts_skipped']} skipped, {stats['comments_new']} new comments")


def cmd_scrape_x(args, config):
    print("Not yet implemented: X/Twitter scraping")


def cmd_scrape_all(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    print("Scraping all configured subreddits...")
    all_stats = scraper.scrape_all(config, db_path)
    print("\nSummary:")
    table = []
    for sub, stats in all_stats.items():
        table.append([sub, stats["posts_new"], stats["posts_skipped"], stats["comments_new"]])
    print(tabulate(table, headers=["Subreddit", "New Posts", "Skipped", "New Comments"], tablefmt="simple"))


def cmd_filter(args, config):
    print("Not yet implemented: filtering")


def cmd_analyze(args, config):
    if args.dry_run:
        print("Not yet implemented: analyze --dry-run")
    else:
        print("Not yet implemented: Claude analysis")


def cmd_validate(args, config):
    print("Not yet implemented: cross-platform validation")


def cmd_pipeline(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    print("=== Step 1: Scrape Reddit ===")
    scraper.scrape_all(config, db_path)
    print("\n=== Step 2: Scrape X ===")
    print("Not yet implemented: X/Twitter scraping")
    print("\n=== Step 3: Filter ===")
    print("Not yet implemented: filtering")
    print("\n=== Step 4: Analyze ===")
    print("Not yet implemented: Claude analysis")
    print("\n=== Step 5: Validate ===")
    print("Not yet implemented: cross-platform validation")
    print("\nPipeline complete (partial — only Reddit scraping active).")


def cmd_report(args, config):
    print("Not yet implemented: reporting")


def cmd_stats(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    stats = db.get_stats(db_path)
    print("Database Statistics:")
    print(f"  Posts:            {stats['posts']}")
    print(f"  Comments:         {stats['comments']}")
    print(f"  Tweets:           {stats['tweets']}")
    print(f"  Pain Points:      {stats['pain_points']}")
    print(f"  Validated:        {stats['validated_pain_points']}")
    if stats["subreddits"]:
        print(f"  Subreddits:       {', '.join(stats['subreddits'])}")


def main():
    parser = argparse.ArgumentParser(description="Reddit + X Pain Point Scraper")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # scrape
    p_scrape = subparsers.add_parser("scrape", help="Scrape a single subreddit")
    p_scrape.add_argument("--subreddit", "-s", help="Subreddit name")
    p_scrape.add_argument("--limit", "-l", type=int, default=None, help="Max posts to fetch")

    # scrape-x
    p_scrape_x = subparsers.add_parser("scrape-x", help="Scrape X/Twitter (stub)")
    p_scrape_x.add_argument("--query", "-q", help="Search query")
    p_scrape_x.add_argument("--limit", "-l", type=int, default=None, help="Max tweets")

    # scrape-all
    subparsers.add_parser("scrape-all", help="Scrape all configured subreddits")

    # filter
    subparsers.add_parser("filter", help="Filter scraped content for pain points (stub)")

    # analyze
    p_analyze = subparsers.add_parser("analyze", help="Analyze with Claude (stub)")
    p_analyze.add_argument("--dry-run", action="store_true", help="Preview without API calls")

    # validate
    subparsers.add_parser("validate", help="Cross-platform validation (stub)")

    # pipeline
    subparsers.add_parser("pipeline", help="Run full scrape → filter → analyze → validate pipeline")

    # report
    p_report = subparsers.add_parser("report", help="Generate reports (stub)")
    p_report.add_argument("--category", help="Filter by category")
    p_report.add_argument("--min-score", type=float, default=0.0, help="Minimum severity score")
    p_report.add_argument("--validated-only", action="store_true", help="Only validated pain points")
    p_report.add_argument("--csv", action="store_true", help="Output as CSV")

    # stats
    subparsers.add_parser("stats", help="Show database statistics")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    config = load_config(args.config)

    commands = {
        "scrape": cmd_scrape,
        "scrape-x": cmd_scrape_x,
        "scrape-all": cmd_scrape_all,
        "filter": cmd_filter,
        "analyze": cmd_analyze,
        "validate": cmd_validate,
        "pipeline": cmd_pipeline,
        "report": cmd_report,
        "stats": cmd_stats,
    }
    commands[args.command](args, config)


if __name__ == "__main__":
    main()
