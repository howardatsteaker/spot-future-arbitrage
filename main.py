from argparse import ArgumentParser
import asyncio
from src.common import Config
from src.script.main_process import MainProcess


async def main():
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", required=True, help="config file path")
    args = parser.parse_args()
    config_path = args.config
    config = Config.from_yaml(config_path)
    main_process = MainProcess(config)
    await main_process.run()


if __name__ == "__main__":
    asyncio.run(main())
