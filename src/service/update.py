from src.service.games.updater import update_games
from src.service.markets.updater import update_markets
from src.service.prices.updater import update_prices


async def run_update():
    await update_games()
    await update_markets()
    await update_prices()
