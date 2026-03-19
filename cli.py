#!/usr/bin/env python3
import argparse
import asyncio
import os
import re
import sys

import yaml
from tabulate import tabulate

import db
import scraper
import filter as pain_filter
import analyzer
import reporter
import validator
from x_scraper import XScraper, ScraperConfig
from models import Tweet


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        raw = f.read()
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
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)

    x_cfg = config.get("x", {})
    queries = [args.query] if args.query else x_cfg.get("search_queries", [])
    if not queries:
        print("Error: no search queries configured. Use --query or set x.search_queries in config.yaml", file=sys.stderr)
        sys.exit(1)

    limit = args.limit or x_cfg.get("max_results", 100)
    max_pages = max(1, limit // 20)

    scraper_config = ScraperConfig(
        query_id=x_cfg.get("graphql_query_ids", {}).get("search", ScraperConfig.query_id),
        delay_between_requests=x_cfg.get("delay_seconds", 2.0),
        max_pages=max_pages,
    )

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    total_tweets = 0

    def store_tweet(tweet_data: dict) -> None:
        nonlocal total_tweets
        t = Tweet(
            id=tweet_data["tweet_id"],
            text=tweet_data["body"],
            author=tweet_data["author_handle"],
            likes=tweet_data["like_count"],
            retweets=tweet_data["retweet_count"],
            replies=tweet_data["reply_count"],
            url=tweet_data["url"],
            created_at=tweet_data["created_at"],
            scraped_at=now,
            search_query=current_query,
        )
        if db.insert_tweet(db_path, t):
            total_tweets += 1

    async def run():
        async with XScraper(config=scraper_config, store_tweet=store_tweet) as xs:
            await xs.authenticate()
            for query in queries:
                nonlocal current_query
                current_query = query
                print(f"Searching X for: '{query}'...")
                tweets = await xs.search(query, max_pages=max_pages)
                print(f"  Found {len(tweets)} tweets")

    current_query = ""
    try:
        asyncio.run(run())
        print(f"\nDone: {total_tweets} new tweets stored")
    except Exception as e:
        print(f"X scraper error: {e}", file=sys.stderr)
        print("Reddit pipeline unaffected.")


def cmd_scrape_all(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    print("=== Reddit ===")
    all_stats = scraper.scrape_all(config, db_path)
    print("\nReddit Summary:")
    table = []
    for sub, stats in all_stats.items():
        table.append([sub, stats["posts_new"], stats["posts_skipped"], stats["comments_new"]])
    print(tabulate(table, headers=["Subreddit", "New Posts", "Skipped", "New Comments"], tablefmt="simple"))

    print("\n=== X/Twitter ===")
    cmd_scrape_x(args, config)


def cmd_filter(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    print("Running pre-filters on all content...")
    results = pain_filter.run_all_filters(config)
    print(f"Posts:    {results['posts']['passed']} passed / {results['posts']['failed']} filtered")
    print(f"Comments: {results['comments']['passed']} passed / {results['comments']['failed']} filtered")
    print(f"Tweets:   {results['tweets']['passed']} passed / {results['tweets']['failed']} filtered")
    print(f"Overall:  {results['summary']['filter_rate_pct']}% filtered out")


def cmd_analyze(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    result = analyzer.run_analysis(config, dry_run=args.dry_run)
    if args.dry_run:
        print(f"\n[DRY RUN] Total items that would be analyzed: {result.get('total_items', 0)}")
    elif result.get("skipped_cap"):
        print("Daily API cap reached.")
    else:
        print(f"Analyzed {result['analyzed']} items out of {result.get('total_items', 0)} queued.")


def cmd_validate(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    print("Running cross-platform validation...")
    result = validator.validate_cross_platform(config)
    print(f"Newly validated:      {result['newly_validated']}")
    print(f"Already validated:    {result['already_validated']}")
    print(f"Reddit items checked: {result['reddit_checked']}")
    print(f"X items checked:      {result['x_checked']}")
    print(f"Boost multiplier:     {result['boost_multiplier']}x")


def cmd_pipeline(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)

    print("=== Step 1: Scrape Reddit ===")
    scraper.scrape_all(config, db_path)

    print("\n=== Step 2: Scrape X ===")
    cmd_scrape_x(args, config)

    print("\n=== Step 3: Filter ===")
    results = pain_filter.run_all_filters(config)
    print(f"Passed: {results['summary']['total_passed']} / {results['summary']['total_processed']} ({results['summary']['filter_rate_pct']}% filtered)")

    print("\n=== Step 4: Analyze ===")
    result = analyzer.run_analysis(config)
    print(f"Analyzed: {result['analyzed']} items")

    print("\n=== Step 5: Validate ===")
    result = validator.validate_cross_platform(config)
    print(f"Validated: {result['newly_validated']} pain points across platforms")

    print("\nPipeline complete.")


def cmd_report(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    results = reporter.get_pain_points(
        category=args.category,
        min_score=args.min_score,
        validated_only=args.validated_only,
        limit=50,
    )
    if args.csv:
        reporter.export_csv(results, args.csv)
    else:
        reporter.print_table(results)


def cmd_stats(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    reporter.print_detailed_stats()


def main():
    parser = argparse.ArgumentParser(description="Reddit + X Pain Point Scraper")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # scrape
    p_scrape = subparsers.add_parser("scrape", help="Scrape a single subreddit")
    p_scrape.add_argument("--subreddit", "-s", help="Subreddit name")
    p_scrape.add_argument("--limit", "-l", type=int, default=None, help="Max posts to fetch")

    # scrape-x
    p_scrape_x = subparsers.add_parser("scrape-x", help="Scrape X/Twitter")
    p_scrape_x.add_argument("--query", "-q", help="Search query")
    p_scrape_x.add_argument("--limit", "-l", type=int, default=None, help="Max tweets")

    # scrape-all
    subparsers.add_parser("scrape-all", help="Scrape all configured subreddits + X")

    # filter
    subparsers.add_parser("filter", help="Filter scraped content for pain points")

    # analyze
    p_analyze = subparsers.add_parser("analyze", help="Analyze with Claude")
    p_analyze.add_argument("--dry-run", action="store_true", help="Preview without API calls")

    # validate
    subparsers.add_parser("validate", help="Cross-platform validation")

    # pipeline
    subparsers.add_parser("pipeline", help="Run full scrape -> filter -> analyze -> validate pipeline")

    # report
    p_report = subparsers.add_parser("report", help="Generate reports")
    p_report.add_argument("--category", help="Filter by category")
    p_report.add_argument("--min-score", type=float, default=0.0, help="Minimum opportunity score")
    p_report.add_argument("--validated-only", action="store_true", help="Only validated pain points")
    p_report.add_argument("--csv", metavar="FILE", help="Export to CSV file")

    # stats
    subparsers.add_parser("stats", help="Show pipeline statistics")

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
