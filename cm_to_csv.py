"""
Note to self: fair amount of blocking I/O in here (e.g. mkdir).
Mostly low-cost, except for perhaps moving files (shutil.move).
"""

import asyncio
import itertools
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import aiofiles
import aiohttp
from aiocsv import AsyncDictWriter
from bs4 import BeautifulSoup
from loguru import logger
from pydantic import BaseModel, Field, PrivateAttr, ValidationError, field_validator

from common.currency import fetch_eur_to_usd_rate
from common.logging import init_logger


class ScriptSettings(BaseModel):
    """
    Path should resolve windows paths correctly, but haven't tested it
    """

    completed_dir: Path = Field(default=Path("./data/completed"))
    input_dir: Path = Field(default=Path("./data/input"))
    output_dir: Path = Field(default=Path("./data/records"))
    input_glob: str = Field(
        default="*.html",
        description="files to include. Ensure only HTML files are included.",
    )
    eur_to_usd_multiplier: float = Field(
        default=1.16,
        description="Updated to latest available if able to",
    )

    def create_dirs(self):
        self.completed_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)


settings = ScriptSettings()


class ArticleAttributes(BaseModel):
    quantity: int = Field(default=1, alias="data-amount")
    name: str = Field(alias="data-name")
    condition: str | None = Field(default=None, alias="data-condition")
    price: float | None = Field(default=None, alias="data-price")
    cardmarket_id: str = Field(alias="data-product-id")
    finish: str


class ScryfallData(BaseModel):
    """
    A bunch of stuff can be added to this model, as long as the header is updated.
    """

    scryfall_id: str = Field(alias="id")
    language: str = Field(alias="lang")


class ArticleRecord(ArticleAttributes, ScryfallData):
    _fpath: str = PrivateAttr()

    @property
    def fpath(self):
        return self._fpath

    def set_fpath(self, fpath: str | Path):
        self._fpath = str(fpath)

        return self

    @classmethod
    def csv_header(cls):
        return [
            "quantity",
            "name",
            "finish",
            "condition",
            "cardmarket_id",  # ignored by Aarchidekt
            "language",
            "price",
            "scryfall_id",
        ]

        ## programmatic way to find header keys. Way more clunky to import
        # schema = cls.model_json_schema(by_alias=False)
        # props: dict[str, Any] | None = schema.get("properties")

        # if not isinstance(props, dict):
        #     raise TypeError(props)

        # return list(props)

    @classmethod
    async def from_article_soup(cls, art_soup: Any, fpath: Path):
        try:
            finish = (
                "Foil" if art_soup.find("span", attrs={"title": "Foil"}) else "Normal"
            )

            article_attrs = ArticleAttributes.model_validate(
                {
                    **art_soup.attrs,
                    "finish": finish,
                },
            )

            scryfall_data = await fetch_scryfall_data(article_attrs.cardmarket_id)

            return cls.model_validate(
                {
                    **article_attrs.model_dump(by_alias=True),
                    **scryfall_data.model_dump(by_alias=True),
                },
            ).set_fpath(fpath)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                f"{exc.__class__.__name__} during processing of article from {fpath}",
            )
            if isinstance(exc, ValidationError):
                logger.debug(exc.json())
            else:
                logger.error(f"Error: {exc}, article sourced from file = '{fpath}'")
            return None

    @field_validator("condition", mode="after")
    @classmethod
    def cardmarket_numeric_condition_to_str(cls, condition: str | None):
        match condition:
            case "1":
                return "M"
            case "2":
                return "NM"
            case "3":
                return "LP"
            case "4":
                return "MP"
            case "5":
                return "MP"
            case "6":
                return "HP"
            case "7":
                return "D"
            case _:
                return None

    @field_validator("price", mode="after")
    @classmethod
    def convert_eur_to_usd(cls, price: float | None):
        if price is None:
            return None

        return round(settings.eur_to_usd_multiplier * price, 2)

    def get_nonefield_keys(self):
        return [k for k, v in self.__dict__.items() if v is None]

    def has_nonefield(self):
        return any(v is None for v in self.__dict__.values())


async def fetch_scryfall_data(cardmarket_id: str):
    """
    Docs: https://scryfall.com/docs/api/cards/cardmarket
    """

    url = f"https://api.scryfall.com/cards/cardmarket/{cardmarket_id}"

    async with (
        aiohttp.ClientSession() as session,
        session.get(url) as resp,
    ):
        if not resp.ok:
            logger.error(
                f"Failed to find product id {cardmarket_id} using scryfall API. "
                f"Response status = {resp.status}, text = {await resp.text()}",
            )
            raise ValueError(f"Could not find a card with this ID: {cardmarket_id}")

        content = await resp.json()
        logger.debug(json.dumps(content, indent=2))

        return ScryfallData.model_validate(content)


async def process_order(html_path: Path):
    """
    Given a path to a HTML file, parses articles in the list.
    """
    async with aiofiles.open(html_path) as infile:
        contents = await infile.read()
        soup = BeautifulSoup(contents, "html.parser")
        html_articles = soup.find_all("tr", attrs={"data-article-id": True})
        tasks = (
            asyncio.create_task(ArticleRecord.from_article_soup(art, html_path))
            for art in html_articles
        )
        results = await asyncio.gather(*tasks)

        return [res for res in results if res is not None]


async def write_results_csv(outfile_name: str, articles: list[ArticleRecord]):
    if not articles:
        return

    outpath = settings.output_dir.joinpath(f"{outfile_name}")

    async with aiofiles.open(outpath, "w") as outfile:
        writer = AsyncDictWriter(outfile, fieldnames=ArticleRecord.csv_header())
        await writer.writeheader()

        for record in articles:
            nonefields = record.get_nonefield_keys()

            if nonefields:
                logger.info(
                    f"Needs manual input: card '{record.name}' (from {record.fpath})\n"
                    f"> missing n={len(nonefields)} values: {', '.join(nonefields)}",
                )

            await writer.writerow(json.loads(record.model_dump_json()))


async def process_article_results(
    article_results: list[ArticleRecord],
):
    completed: list[ArticleRecord] = []
    partial: list[ArticleRecord] = []

    for record in article_results:
        if record.has_nonefield():
            partial.append(record)
        else:
            completed.append(record)

    now_str = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

    await asyncio.gather(
        *(
            write_results_csv(f"out-{now_str}.csv", completed),
            write_results_csv(f"out-incomplete-{now_str}.csv", partial),
        ),
    )

    if completed:
        logger.info(f"-- wrote {len(completed)} complete records")

    if partial:
        logger.warning(
            f"-- wrote {len(partial)} incomplete records that might need manual input",
        )

    return partial


async def main():
    settings.create_dirs()
    init_logger()

    try:
        eur_usd_rate = await fetch_eur_to_usd_rate()
        settings.eur_to_usd_multiplier = eur_usd_rate
        logger.info(f"Using exchange rate from ECB: 1 EUR <=> {eur_usd_rate} USD")
    except Exception as exc:  # noqa: BLE001
        logger.error(exc)
        logger.info(
            f"Using default exchange rate: 1 EUR <=> {settings.eur_to_usd_multiplier} USD",
        )

    fpaths = [Path(p) for p in settings.input_dir.glob(settings.input_glob)]

    if not fpaths:
        logger.error(
            f"No files found in {settings.input_dir} given glob = {settings.input_glob}",
        )
        return False

    results = await asyncio.gather(
        *(asyncio.create_task(process_order(fpath)) for fpath in fpaths),
    )
    results_list = list(itertools.chain.from_iterable(results))
    partial_results = await process_article_results(results_list)

    logger.info(
        f"Moving {len(fpaths)} files from {settings.input_dir} to {settings.completed_dir}",
    )

    for fpath in fpaths:
        shutil.move(fpath, settings.completed_dir)

    return bool(partial_results)


if __name__ == "__main__":
    asyncio.run(main())
