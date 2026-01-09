from src.service.games.updater import update_games
from src.service.markets.updater import update_markets


async def run_update():
    await update_games()
    await update_markets()
