from src.core.api import PolymarketGammaAPIClient


class NBAGamesBySlugClient(PolymarketGammaAPIClient):
    """NBA games fetched via series slug (legacy pre-2025/26 seasons)"""

    limit_requests = 350  # https://docs.polymarket.com/quickstart/introduction/rate-limits#gamma-api-rate-limits

    def __init__(
        self,
        endpoint: str = "series",
        filename: str = "games/games_by_slug_dump.json",
        params: dict[str, str] | None = None,
    ) -> None:
        if params is None:
            params = {"slug": "nba"}

        super().__init__(endpoint=endpoint, filename=filename, params=params)


class NBAGamesBySeriesIdClient(PolymarketGammaAPIClient):
    """NBA games fetched via series id (seasons 2025/26 and later)"""

    limit_requests = 350  # https://docs.polymarket.com/quickstart/introduction/rate-limits#gamma-api-rate-limits

    def __init__(
        self,
        endpoint: str = "series/10345",
        filename: str = "games/games_by_series_dump.json",
        params: dict[str, str] | None = None,
    ) -> None:
        super().__init__(endpoint=endpoint, filename=filename, params=params)
