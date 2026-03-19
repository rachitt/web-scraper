"""
X/Twitter search scraper using the internal SearchTimeline GraphQL API.

Uses authenticated sessions from x_auth to search tweets and extract
structured data with pagination, rate limiting, and retry logic.
"""

import asyncio
import json
import logging
import random
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Callable

import httpx
import yaml

from x_auth import XAuth

logger = logging.getLogger(__name__)

# Default query ID for SearchTimeline — rotates with X deploys
DEFAULT_QUERY_ID = "flaR-PUMshxFWZWPNpq4zA"

GRAPHQL_BASE = "https://x.com/i/api/graphql"

# Features required by the SearchTimeline endpoint
SEARCH_FEATURES = {
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "tweetypie_unmention_optimization_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "rweb_video_timestamps_enabled": True,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "responsive_web_media_download_video_enabled": False,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_enhance_cards_enabled": False,
}

FIELD_TOGGLES = {"withArticleRichContentState": False}


@dataclass
class ScraperConfig:
    """Configuration for the X scraper."""

    query_id: str = DEFAULT_QUERY_ID
    count: int = 20
    product: str = "Latest"  # "Latest" or "Top"
    delay_between_requests: float = 2.0
    max_retries: int = 3
    backoff_base: float = 5.0
    max_pages: int = 10
    cookie_path: str = "data/x_cookies.json"

    @classmethod
    def from_yaml(cls, path: str = "config.yaml") -> "ScraperConfig":
        """Load config from the 'x' section of a YAML file."""
        try:
            with open(path) as f:
                cfg = yaml.safe_load(f) or {}
            x_cfg = cfg.get("x", {})
            return cls(
                query_id=x_cfg.get("query_id", DEFAULT_QUERY_ID),
                count=x_cfg.get("count", 20),
                product=x_cfg.get("product", "Latest"),
                delay_between_requests=x_cfg.get("delay_between_requests", 2.0),
                max_retries=x_cfg.get("max_retries", 3),
                backoff_base=x_cfg.get("backoff_base", 5.0),
                max_pages=x_cfg.get("max_pages", 10),
                cookie_path=x_cfg.get("cookie_path", "data/x_cookies.json"),
            )
        except FileNotFoundError:
            logger.info("config.yaml not found, using defaults")
            return cls()


def parse_tweet(entry: dict) -> dict | None:
    """Extract structured tweet data from a timeline entry.

    Returns None if the entry is not a valid tweet (e.g., cursor, promotion).
    """
    try:
        content = entry.get("content", {})
        item_content = content.get("itemContent", {})
        tweet_results = item_content.get("tweet_results", {})
        result = tweet_results.get("result", {})

        # Handle "TweetWithVisibilityResults" wrapper
        if result.get("__typename") == "TweetWithVisibilityResults":
            result = result.get("tweet", {})

        if result.get("__typename") != "Tweet":
            return None

        legacy = result.get("legacy", {})
        user_legacy = (
            result.get("core", {})
            .get("user_results", {})
            .get("result", {})
            .get("legacy", {})
        )

        tweet_id = result.get("rest_id", "")
        author_handle = user_legacy.get("screen_name", "")

        return {
            "tweet_id": tweet_id,
            "author_handle": author_handle,
            "author_name": user_legacy.get("name", ""),
            "body": legacy.get("full_text", ""),
            "retweet_count": legacy.get("retweet_count", 0),
            "like_count": legacy.get("favorite_count", 0),
            "reply_count": legacy.get("reply_count", 0),
            "url": f"https://x.com/{author_handle}/status/{tweet_id}",
            "created_at": legacy.get("created_at", ""),
        }
    except (KeyError, TypeError, AttributeError):
        logger.debug("Failed to parse tweet entry: %s", entry.get("entryId", "unknown"))
        return None


def extract_entries_and_cursor(
    data: dict,
) -> tuple[list[dict], str | None]:
    """Parse the SearchTimeline response into tweet entries and next cursor."""
    instructions = (
        data.get("data", {})
        .get("search_by_raw_query", {})
        .get("search_timeline", {})
        .get("timeline", {})
        .get("instructions", [])
    )

    entries = []
    cursor = None

    for instruction in instructions:
        if instruction.get("type") == "TimelineAddEntries":
            for entry in instruction.get("entries", []):
                entry_id = entry.get("entryId", "")
                if entry_id.startswith("tweet-"):
                    entries.append(entry)
                elif entry_id.startswith("cursor-bottom"):
                    cursor = (
                        entry.get("content", {}).get("value")
                        or entry.get("content", {}).get("itemContent", {}).get("value")
                    )
        elif instruction.get("type") == "TimelineReplaceEntry":
            entry = instruction.get("entry", {})
            entry_id = entry.get("entryId", "")
            if entry_id.startswith("cursor-bottom"):
                cursor = entry.get("content", {}).get("value")

    return entries, cursor


class XScraper:
    """Async X/Twitter search scraper using the SearchTimeline GraphQL API."""

    def __init__(
        self,
        config: ScraperConfig | None = None,
        store_tweet: Callable[[dict], Any] | None = None,
    ):
        self.config = config or ScraperConfig()
        self.store_tweet = store_tweet or self._default_store
        self._auth: XAuth | None = None
        self._headers: dict[str, str] = {}
        self._cookies: dict[str, str] = {}
        self._client: httpx.AsyncClient | None = None

    @staticmethod
    def _default_store(tweet: dict) -> None:
        logger.info("Tweet @%s: %s", tweet["author_handle"], tweet["body"][:80])

    async def authenticate(self) -> None:
        """Authenticate using XAuth and set up session."""
        self._auth = XAuth(cookie_path=self.config.cookie_path)
        self._headers = self._auth.get_session_headers()
        self._cookies = self._auth.get_cookies()
        self._client = httpx.AsyncClient(
            headers=self._headers,
            cookies=self._cookies,
            follow_redirects=True,
            timeout=30.0,
        )

    def _build_search_url(self, query: str, cursor: str | None = None) -> str:
        """Build the SearchTimeline URL with encoded parameters."""
        variables: dict[str, Any] = {
            "rawQuery": query,
            "count": self.config.count,
            "querySource": "typed_query",
            "product": self.config.product,
        }
        if cursor:
            variables["cursor"] = cursor

        params = {
            "variables": json.dumps(variables, separators=(",", ":")),
            "features": json.dumps(SEARCH_FEATURES, separators=(",", ":")),
            "fieldToggles": json.dumps(FIELD_TOGGLES, separators=(",", ":")),
        }
        query_string = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        return f"{GRAPHQL_BASE}/{self.config.query_id}/SearchTimeline?{query_string}"

    async def _request_with_retry(self, url: str) -> dict:
        """Make a GET request with retry logic for transient errors."""
        for attempt in range(self.config.max_retries + 1):
            resp = await self._client.get(url)

            if resp.status_code == 200:
                # Sync ct0 if rotated
                for cookie in resp.cookies.jar:
                    if cookie.name == "ct0":
                        self._client.headers["x-csrf-token"] = cookie.value
                return resp.json()

            if resp.status_code in (429, 503):
                if attempt == self.config.max_retries:
                    raise httpx.HTTPStatusError(
                        f"Failed after {self.config.max_retries} retries",
                        request=resp.request,
                        response=resp,
                    )
                wait = self.config.backoff_base * (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    "Got %d, retrying in %.1fs (attempt %d/%d)",
                    resp.status_code, wait, attempt + 1, self.config.max_retries,
                )
                await asyncio.sleep(wait)
                continue

            if resp.status_code in (400, 404):
                logger.error(
                    "Got %d — the SearchTimeline endpoint may have changed. "
                    "Check if the query_id '%s' needs updating. Response: %s",
                    resp.status_code, self.config.query_id, resp.text[:500],
                )
                raise httpx.HTTPStatusError(
                    f"Endpoint error {resp.status_code}: query_id may be stale",
                    request=resp.request,
                    response=resp,
                )

            resp.raise_for_status()

        raise RuntimeError("Unreachable")

    async def search(self, query: str, max_pages: int | None = None) -> list[dict]:
        """Search tweets and return all collected results.

        Paginates through results up to max_pages (default from config).
        Calls store_tweet() for each tweet found.
        """
        if not self._client:
            raise RuntimeError("Call authenticate() before search()")

        max_pages = max_pages or self.config.max_pages
        all_tweets: list[dict] = []
        cursor: str | None = None

        for page in range(max_pages):
            url = self._build_search_url(query, cursor)
            logger.info("Fetching page %d for query '%s'", page + 1, query)

            data = await self._request_with_retry(url)
            entries, next_cursor = extract_entries_and_cursor(data)

            if not entries:
                logger.info("No more results on page %d", page + 1)
                break

            page_tweets = []
            for entry in entries:
                tweet = parse_tweet(entry)
                if tweet:
                    self.store_tweet(tweet)
                    page_tweets.append(tweet)

            all_tweets.extend(page_tweets)
            logger.info("Page %d: %d tweets collected", page + 1, len(page_tweets))

            if not next_cursor:
                logger.info("No cursor for next page, stopping")
                break
            cursor = next_cursor

            if page < max_pages - 1:
                delay = self.config.delay_between_requests + random.uniform(0, 1)
                await asyncio.sleep(delay)

        logger.info("Total tweets collected: %d", len(all_tweets))
        return all_tweets

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
        if self._auth:
            self._auth.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()


# ── CLI test ────────────────────────────────────────────────────

async def main():
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    query = sys.argv[1] if len(sys.argv) > 1 else "python programming"
    config = ScraperConfig.from_yaml()

    collected = []

    def store(tweet: dict) -> None:
        collected.append(tweet)
        print(f"[@{tweet['author_handle']}] {tweet['body'][:100]}")

    async with XScraper(config=config, store_tweet=store) as scraper:
        await scraper.authenticate()
        tweets = await scraper.search(query, max_pages=2)

    print(f"\n--- Collected {len(tweets)} tweets ---")
    for t in tweets[:5]:
        print(f"  {t['url']}")
        print(f"  {t['body'][:120]}")
        print(f"  likes={t['like_count']} rt={t['retweet_count']} replies={t['reply_count']}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
