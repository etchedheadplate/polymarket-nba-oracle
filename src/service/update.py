from src.core.conflicts import UpdateNonNullFields
from src.core.updaters import BaseUpdater
from src.database.models import NBAGamesModel, NBAMarketsModel, NBAPricesModel
from src.service.clients import NBAGamesClient, NBAMarketsClient, NBAPricesClient
from src.service.parsers import NBAGamesParser, NBAMarketsParser, NBAPricesParser
from src.service.tasks import construct_game_dates, construct_market_endpoints, construct_prices_payload


class GamesUpdater(BaseUpdater):
    _client_cls = NBAGamesClient
    _parser_cls = NBAGamesParser
    _alchemy_model = NBAGamesModel
    _conflict_strategy = UpdateNonNullFields(
        index_elements=["event_slug"], fields_to_update=["guest_score", "host_score", "game_period", "game_status"]
    )


class MarketsUpdater(BaseUpdater):
    _client_cls = NBAMarketsClient
    _parser_cls = NBAMarketsParser
    _alchemy_model = NBAMarketsModel
    _conflict_strategy = UpdateNonNullFields(
        index_elements=["event_id", "market_question"], fields_to_update=["market_end"]
    )


class PricesUpdater(BaseUpdater):
    _client_cls = NBAPricesClient
    _parser_cls = NBAPricesParser
    _alchemy_model = NBAPricesModel
    _conflict_strategy = UpdateNonNullFields(
        index_elements=["market_id", "timestamp"], fields_to_update=["price_guest_buy", "price_host_buy"]
    )


async def update_database():
    start_date, end_date = await construct_game_dates()
    for by_slug in (True, False):
        await GamesUpdater().run(
            client_kwargs={"by_slug": by_slug},
            parser_kwargs={"start_date": start_date, "end_date": end_date, "by_slug": by_slug},
        )

    endpoints = await construct_market_endpoints()
    await MarketsUpdater().run(client_kwargs={"endpoints": endpoints}, parser_kwargs=None)

    payload_guest, payload_host, token_market_map = await construct_prices_payload()
    price_config = ((True, payload_guest), (False, payload_host))
    for config in price_config:
        is_guest, params = config
        await PricesUpdater().run(
            client_kwargs={"params": params}, parser_kwargs={"token_market_map": token_market_map, "is_guest": is_guest}
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(update_database())
