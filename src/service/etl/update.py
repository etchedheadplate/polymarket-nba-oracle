import asyncio

from src.service.etl.games import update_games
from src.service.etl.markets import update_markets
from src.service.etl.prices import update_prices

from typing import cast
import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete-dumps", action="store_true")
    return parser.parse_args()


async def update_database(delete_dumps: bool = False) -> dict[str, int]:
    results = await asyncio.gather(
        update_games(),
        update_markets(),
        update_prices(),
        return_exceptions=True,
    )

    errors = [r for r in results if isinstance(r, BaseException)]
    if errors:
        raise Exception(f"Update failed with errors: {errors}")

    games, markets, prices = cast(tuple[int, int, int], tuple(results))

    if delete_dumps:
        import shutil
        from src.config import settings

        dumps_dir = settings.DUMPS_DIR
        for path in dumps_dir.iterdir():
            if path.is_file() or path.is_symlink():
                path.unlink()
            else:
                shutil.rmtree(path)

    return {
        "games": games,
        "markets": markets,
        "prices": prices,
    }


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(update_database(delete_dumps=args.delete_dumps))
