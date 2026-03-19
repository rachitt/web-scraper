from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Post:
    id: str
    subreddit: str
    title: str
    selftext: str
    author: str
    score: int
    num_comments: int
    url: str
    created_utc: float
    scraped_at: Optional[str] = None

    @property
    def full_text(self) -> str:
        return f"{self.title} {self.selftext}"


@dataclass
class Comment:
    id: str
    post_id: str
    parent_id: str
    body: str
    author: str
    score: int
    depth: int
    created_utc: float
    scraped_at: Optional[str] = None

    @property
    def full_text(self) -> str:
        return self.body


@dataclass
class Tweet:
    id: str
    text: str
    author: str
    likes: int
    retweets: int
    replies: int
    url: str
    created_at: str
    scraped_at: Optional[str] = None

    @property
    def full_text(self) -> str:
        return self.text


@dataclass
class PainPoint:
    id: Optional[int] = None
    description: str = ""
    category: str = ""
    severity: float = 0.0
    frequency: float = 0.0
    market_size: str = ""
    source_platform: str = ""       # reddit, x
    source_type: str = ""           # post, comment, tweet
    source_id: str = ""             # references post/comment/tweet id
    source_text: str = ""
    subreddit: Optional[str] = None
    confidence: float = 0.0
    cross_platform_validated: bool = False
    cluster_id: Optional[int] = None
    created_at: Optional[str] = None
    tags: list[str] = field(default_factory=list)
