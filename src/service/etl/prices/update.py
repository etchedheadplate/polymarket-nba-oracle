from typing import Any

from src.core.conflicts import UpdateNonNullFields
from src.core.updater import BaseUpdater
from src.database.connection import async_session_maker
from src.database.models import NBAPricesModel
from src.service.etl.prices.client import NBAPricesClient
from src.service.etl.prices.parser import NBAPricesParser
from src.service.repos import NBAMarketsRepo


class PricesUpdater(BaseUpdater):
    _client_cls = NBAPricesClient
    _parser_cls = NBAPricesParser
    _alchemy_model = NBAPricesModel
    _conflict_strategy = UpdateNonNullFields(
        index_elements=["market_id", "timestamp"], fields_to_update=["price_guest_buy", "price_host_buy"]
    )


async def construct_prices_payload() -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[int, int]]:
    async with async_session_maker() as session:
        markets = await NBAMarketsRepo().get_markets_without_prices(session)

    payload_guest: list[dict[str, Any]] = []
    payload_host: list[dict[str, Any]] = []
    token_market_map: dict[int, int] = {}

    for market_id, start, end, guest_token, host_token in markets:
        if not end:
            continue

        params_guest = {"market": guest_token, "startTs": int(start), "endTs": int(end)}
        params_host = {"market": host_token, "startTs": int(start), "endTs": int(end)}

        payload_guest.append(params_guest)
        payload_host.append(params_host)

        token_market_map[int(guest_token)] = market_id
        token_market_map[int(host_token)] = market_id

    return payload_guest, payload_host, token_market_map


async def update_prices():
    payload_guest, payload_host, token_market_map = await construct_prices_payload()
    price_config = ((True, payload_guest), (False, payload_host))
    for config in price_config:
        is_guest, params = config
        await PricesUpdater().run(
            client_kwargs={"params": params}, parser_kwargs={"token_market_map": token_market_map, "is_guest": is_guest}
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(update_prices())
