from dataclasses import dataclass
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
    is_pain_point: Optional[int] = None

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
    is_pain_point: Optional[int] = None

    @property
    def full_text(self) -> str:
        return self.body


@dataclass
class PainPoint:
    id: Optional[int] = None
    source_id: str = ""
    source_type: str = ""
    source_platform: str = ""
    problem_summary: str = ""
    category: str = ""
    frustration_level: float = 0.0
    solvability_score: float = 0.0
    market_size_score: float = 0.0
    frequency_score: float = 0.0
    opportunity_score: float = 0.0
    app_idea: str = ""
    created_at: Optional[str] = None
