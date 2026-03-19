#!/usr/bin/env python3
import argparse
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


def cmd_scrape_all(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    all_stats = scraper.scrape_all(config, db_path)
    print("\nSummary:")
    table = []
    for sub, stats in all_stats.items():
        table.append([sub, stats["posts_new"], stats["posts_skipped"], stats["comments_new"]])
    print(tabulate(table, headers=["Subreddit", "New Posts", "Skipped", "New Comments"], tablefmt="simple"))


def cmd_filter(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    print("Running pre-filters on all content...")
    results = pain_filter.run_all_filters(config)
    print(f"Posts:    {results['posts']['passed']} passed / {results['posts']['failed']} filtered")
    print(f"Comments: {results['comments']['passed']} passed / {results['comments']['failed']} filtered")
    print(f"Overall:  {results['summary']['filter_rate_pct']}% filtered out")


def cmd_analyze(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)
    if args.import_file:
        result = analyzer.import_results(config, args.import_file)
    else:
        result = analyzer.export_for_analysis(config, args.output)
        if result["exported"] > 0:
            print(f"\nNow have Claude Code read {result['path']} and analyze the items.")
            print(f"Save results to data/analyze_results.json, then run:")
            print(f"  python3 cli.py analyze --import data/analyze_results.json")


def cmd_pipeline(args, config):
    db_path = config["storage"]["db_path"]
    db.init_db(db_path)

    print("=== Step 1: Scrape Reddit ===")
    scraper.scrape_all(config, db_path)

    print("\n=== Step 2: Filter ===")
    results = pain_filter.run_all_filters(config)
    print(f"Passed: {results['summary']['total_passed']} / {results['summary']['total_processed']} ({results['summary']['filter_rate_pct']}% filtered)")

    print("\n=== Step 3: Export for Analysis ===")
    result = analyzer.export_for_analysis(config)
    if result["exported"] > 0:
        print(f"\nPipeline paused. Have Claude Code analyze {result['path']}, then run:")
        print(f"  python3 cli.py analyze --import data/analyze_results.json")
        print(f"  python3 cli.py report")
    else:
        print("\nNo items to analyze. Pipeline complete.")


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
    parser = argparse.ArgumentParser(description="Reddit Pain Point Scraper")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # scrape
    p_scrape = subparsers.add_parser("scrape", help="Scrape a single subreddit")
    p_scrape.add_argument("--subreddit", "-s", help="Subreddit name")
    p_scrape.add_argument("--limit", "-l", type=int, default=None, help="Max posts to fetch")

    # scrape-all
    subparsers.add_parser("scrape-all", help="Scrape all configured subreddits")

    # filter
    subparsers.add_parser("filter", help="Filter scraped content for pain points")

    # analyze
    p_analyze = subparsers.add_parser("analyze", help="Export items for Claude Code analysis / import results")
    p_analyze.add_argument("--output", "-o", metavar="FILE", help="Export path (default: data/analyze_input.json)")
    p_analyze.add_argument("--import", dest="import_file", metavar="FILE", help="Import results JSON from Claude Code")

    # pipeline
    subparsers.add_parser("pipeline", help="Run full scrape -> filter -> export pipeline")

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
        "scrape-all": cmd_scrape_all,
        "filter": cmd_filter,
        "analyze": cmd_analyze,
        "pipeline": cmd_pipeline,
        "report": cmd_report,
        "stats": cmd_stats,
    }
    commands[args.command](args, config)


if __name__ == "__main__":
    main()
