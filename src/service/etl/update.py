import asyncio
import argparse

from src.service.etl.games import update_games
from src.service.etl.markets import update_markets
from src.service.etl.prices import update_prices


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-dumps", action="store_true")
    return parser.parse_args()


async def update_database(keep_dumps: bool = False) -> dict[str, int | None]:
    results: dict[str, int | None] = {}
    for name, coro in (
        ("games", update_games()),
        ("markets", update_markets()),
        ("prices", update_prices()),
    ):
        try:
            results[name] = await coro
        except BaseException:
            results[name] = None

    if not keep_dumps:
        import shutil
        from src.config import settings

        dumps_dir = settings.DUMPS_DIR
        for path in dumps_dir.iterdir():
            if path.is_file() or path.is_symlink():
                path.unlink()
            else:
                shutil.rmtree(path)

    return results


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(update_database(keep_dumps=args.keep_dumps))
