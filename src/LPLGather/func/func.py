"""
Autor: Maze
Data: 29/10/2023
Descrição: Mapeia os name dos champions.
"""

from functools import lru_cache
import asyncio
import aiosonic
import numpy
import re


RarityPattern = re.compile(r"<rarityLegendary>(.*?)</rarityLegendary>")


def filter_gangplank_item_names(name):
    match = RarityPattern.search(name)
    return match.group(1) if match else name


class IDGetter:
    def __init__(self):
        connector = aiosonic.TCPConnector(
            pool_size=5, timeouts=aiosonic.Timeouts(5, 5, 5, 10)
        )
        self.client = aiosonic.HTTPClient(connector=connector)

    async def patch(self):
        r = await self.client.get(
            "https://ddragon.leagueoflegends.com/api/versions.json", http2=True
        )
        return (await r.json())[0]

    async def close(self):
        await self.client.connector.cleanup()

    async def fetch_json(self, url, type, use_http2):
        res = await self.client.get(url, http2=use_http2)
        return await res.json(), type

    async def id_to_name_dict(self, patch):
        urls = [
            (
                f"http://ddragon.leagueoflegends.com/cdn/{patch}/data/en_US/champion.json",
                "champion",
                True,
            ),
            (
                f"http://ddragon.leagueoflegends.com/cdn/{patch}/data/en_US/item.json",
                "item",
                True,
            ),
            (
                f"http://ddragon.leagueoflegends.com/cdn/{patch}/data/en_US/summoner.json",
                "summoner",
                True,
            ),
            (
                f"http://ddragon.leagueoflegends.com/cdn/{patch}/data/en_US/runesReforged.json",
                "runes",
                True,
            ),
            (
                "https://raw.communitydragon.org/latest/plugins/rcp-be-lol-game-data/global/default/v1/perks.json",
                "perks",
                False,
            ),
        ]

        responses = await asyncio.gather(
            *(self.fetch_json(url, type, http2) for url, type, http2 in urls)
        )

        combined_dict = {
            "champion": {},
            "item": {},
            "summoner": {},
            "runes": {},
        }

        for (response, _), (_, url_type, _) in zip(responses, urls):
            if url_type in ["champion", "summoner"]:
                combined_dict[url_type].update(
                    {
                        numpy.int16(details["key"]): details["name"]
                        for _, details in response["data"].items()
                    }
                )
            elif url_type == "item":
                combined_dict[url_type].update(
                    {
                        numpy.int16(item_id): filter_gangplank_item_names(
                            details["name"]
                        )
                        for item_id, details in response["data"].items()
                    }
                )
            elif url_type == "perks":
                combined_dict["runes"].update(
                    {
                        numpy.int16(rune["id"]): rune["name"]
                        for rune in response
                        if 5000 < rune["id"] < 5010
                    }
                )

            elif url_type == "runes":
                combined_dict[url_type].update(
                    {
                        numpy.int16(rune["id"]): rune["name"]
                        for tree in response
                        for slot in tree["slots"]
                        for rune in slot["runes"]
                    }
                )
        return combined_dict


@lru_cache(maxsize=1)
def generate_id_name_dict():
    async def _generate():
        getter = IDGetter()
        patch = await getter.patch()
        id_to_name = await getter.id_to_name_dict(patch)
        await getter.close()
        return id_to_name

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_generate())

IN_GAME_DICT = generate_id_name_dict()

RUNES_MAP = IN_GAME_DICT["runes"]
CHAMPION_MAP = IN_GAME_DICT["champion"]
ITEM_MAP = IN_GAME_DICT["item"]
SUMMONER_MAP = IN_GAME_DICT["summoner"]

def map_hero_name(hero_id):
    return CHAMPION_MAP.get(hero_id, "Unknown")