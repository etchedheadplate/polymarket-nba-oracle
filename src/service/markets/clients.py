from src.core.api import BasePolymarketGammaAPIClient


class ArchiveNBAMarketsClient(BasePolymarketGammaAPIClient):
    """Client for markets before 2025/2026 NBA season (search by market slug)"""

    @property
    def endpoint(self) -> str:
        return "series?slug=nba"

    @property
    def filename(self) -> str:
        return "archive_markets_dump.json"


class CurrentNBAMarketsClient(BasePolymarketGammaAPIClient):
    """Client for 2025/2026 and later NBA seasons markets (search by series id)"""

    @property
    def endpoint(self) -> str:
        return "series/10345"

    @property
    def filename(self) -> str:
        return "current_markets_dump.json"
