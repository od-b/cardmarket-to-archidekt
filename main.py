import asyncio
import sys
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

import aiofiles
import aiofiles.os
import aiohttp
from aiocsv import AsyncDictWriter
from bs4 import BeautifulSoup
from loguru import logger
from pydantic import BaseModel, Field, ValidationError, field_validator

from common.currency import fetch_eur_to_usd_rate
from common.logging import init_logger
from common.util import async_move_and_mkdir


class ScriptSettings(BaseModel):
    """
    Path should resolve windows paths correctly, but haven't tested it
    """

    completed_dir: Path = Field(default=Path("./data/processed/completed"))
    partial_dir: Path = Field(default=Path("./data/processed/completed_partial"))
    completed_none_dir: Path = Field(default=Path("./data/processed/failed"))
    input_dir: Path = Field(default=Path("./data/input"))
    output_csv_dir: Path = Field(default=Path("./data/records"))
    input_glob: str = Field(
        default="*.html",
        description="files to include. Ensure only HTML files are included.",
    )
    eur_to_usd_multiplier: float = Field(
        default=1.16,
        description="This default value is used if the exchange rate cannot be fetched from ECB.",
    )
    default_lang: str | None = Field(
        default="en",
        description="Default language for cards where scryfall data is not available.",
    )
    skip_by_name: list[str] | None = Field(
        default=["token", " emblem"],
        description="Skip articles with any of the given strings in their name",
    )


settings = ScriptSettings()


class ArticleCSVRecordBase(BaseModel):
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


class ArticleAttributes(BaseModel):
    name: str = Field(
        alias="data-name",
        description="Overwritten by Scryfall data if available",
    )
    quantity: int = Field(alias="data-amount")
    condition: str = Field(alias="data-condition")
    price: float = Field(alias="data-price")
    cardmarket_id: str = Field(alias="data-product-id")
    finish: str = Field(
        description="""
        Note that there is no data-id for finish, so just looks for the icon with `title: "Foil"`.
        If the cardmarket site layout changes at some point, this might silently break (by always defaulting).
        """,
    )


class PartialArticleAttributes(BaseModel):
    quantity: int | None = Field(default=None, alias="data-amount")
    name: str | None = Field(default=None, alias="data-name")
    condition: str | None = Field(default=None, alias="data-condition")
    price: float | None = Field(default=None, alias="data-price")
    cardmarket_id: str | None = Field(default=None, alias="data-product-id")
    finish: str | None = Field(default=None)


class ScryfallData(BaseModel):
    """
    Fields from the scryfall response that are used in the article csv records.
    """

    scryfall_id: str = Field(alias="id")
    language: str = Field(alias="lang")


class ScryfallResponse(ScryfallData):
    """
    Raw response from Scryfall API for a cardmarket product ID.
    Lots of fields are available, but only a few are used in the article csv records.
    """

    name: str = Field(alias="name")
    set: str = Field(alias="set")
    collector_number: str = Field(alias="collector_number")
    scryfall_uri: str = Field(alias="scryfall_uri")
    layout: str = Field(alias="layout")

    @classmethod
    async def fetch_from_cardmarket_id(cls, cardmarket_id: str):
        """
        Docs: https://scryfall.com/docs/api/cards/cardmarket
        """

        url = f"https://api.scryfall.com/cards/cardmarket/{cardmarket_id}"

        async with (
            aiohttp.ClientSession() as session,
            session.get(url) as resp,
        ):
            if not resp.ok:
                raise ValueError(
                    f"Failed to find product id {cardmarket_id} using scryfall API."
                    f" Response status = {resp.status}, text = {await resp.text()}",
                )

            content = await resp.json()

            return cls.model_validate(content)


class PartialScryfallData(BaseModel):
    """
    A bunch of stuff can be added to this model, as long as the header is updated.
    """

    scryfall_id: str | None = Field(default=None, alias="id")
    language: str | None = Field(default=settings.default_lang, alias="lang")


class ArticleRecordBase(ArticleCSVRecordBase):
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
                if isinstance(condition, str):
                    logger.critical(f"Unknown condition: {condition}")
                return None

    def get_nonefield_keys(self):
        return [k for k, v in self.__dict__.items() if v is None]

    def has_nonefield(self):
        return any(v is None for v in self.__dict__.values())


class PartialArticleRecord(
    ArticleRecordBase,
    PartialArticleAttributes,
    PartialScryfallData,
):
    @field_validator("condition", mode="after")
    @classmethod
    def format_condition_str(cls, condition: str | None):
        return cls.cardmarket_numeric_condition_to_str(condition=condition)

    @field_validator("price", mode="after")
    @classmethod
    def convert_eur_to_usd(cls, price: float | None):
        if isinstance(price, float):
            return round(settings.eur_to_usd_multiplier * price, 2)
        return None


class ArticleRecord(ArticleRecordBase, ArticleAttributes, ScryfallData):
    @field_validator("condition", mode="after")
    @classmethod
    def format_condition_str(cls, condition: str):
        return cls.cardmarket_numeric_condition_to_str(condition=condition)

    @field_validator("price", mode="after")
    @classmethod
    def convert_eur_to_usd(cls, price: float):
        return round(settings.eur_to_usd_multiplier * price, 2)

    @classmethod
    async def from_article_soup(cls, art_soup: Any, fpath: Path):
        finish = "Foil" if art_soup.find("span", attrs={"title": "Foil"}) else "Normal"

        try:
            article_attrs = ArticleAttributes.model_validate(
                {
                    **art_soup.attrs,
                    "finish": finish,
                },
            )
        except ValidationError as exc:
            logger.error(
                f"Failed to scrape one or more fields from an articles stemming from file '{fpath.name}'. Details: {exc.json(indent=2)}."
                " Continuing with partial data (if any)",
            )
            article_attrs = PartialArticleAttributes.model_validate(
                {
                    **art_soup.attrs,
                    "finish": finish if finish == "Foil" else None,
                },
            )

        if (
            settings.skip_by_name
            and isinstance(article_attrs.name, str)
            and any(
                pat.lower() in article_attrs.name.lower()
                for pat in settings.skip_by_name
            )
        ):
            logger.info(
                f"Skipping article with name '{article_attrs.name}'.",
            )
            return None

        scryfall_data: ScryfallData | None = None

        if article_attrs.cardmarket_id is not None:
            try:
                # fetch scryfall data for card with the given cardmarket ID (fails if not a card)
                scryfall_response = await ScryfallResponse.fetch_from_cardmarket_id(
                    article_attrs.cardmarket_id,
                )
                # overwrite certain fields with the data from Scryfall
                article_attrs.name = scryfall_response.name

                scryfall_data = ScryfallData.model_validate(
                    {
                        **scryfall_response.model_dump(by_alias=True),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    f"Failed to fetch scryfall data for card with name '{article_attrs.name}', sourced from '{fpath.name}'. Details: {exc}.",
                )

        return_cls = (
            PartialArticleRecord
            if (
                type(article_attrs) is PartialArticleAttributes or scryfall_data is None
            )
            else cls
        )

        scryfall_data_dump = (
            {} if scryfall_data is None else scryfall_data.model_dump(by_alias=True)
        )

        return return_cls.model_validate(
            {
                **article_attrs.model_dump(by_alias=True),
                **scryfall_data_dump,
            },
        )


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

        maybe_results = await asyncio.gather(*tasks, return_exceptions=True)
        maybe_results = [res for res in maybe_results if res is not None]  # pyright: ignore[reportUnnecessaryComparison]

        return html_path, maybe_results


async def write_results_csv(
    fpath: Path,
    articles: Iterable[ArticleRecord | PartialArticleRecord],
):
    articles = tuple(articles)

    async with aiofiles.open(fpath, "w") as outfile:
        writer = AsyncDictWriter(
            outfile,
            fieldnames=ArticleRecord.csv_header(),
        )
        await writer.writeheader()

        partial_records = [art for art in articles if type(art) is PartialArticleRecord]
        complete_records = [art for art in articles if type(art) is ArticleRecord]

        logger.info(f"Writing n={len(articles)} records to '{fpath}'")

        if partial_records:
            logger.warning(
                f"n={len(partial_records)} of the records are partial, and need manual input. They are all found at the top of the CSV.",
            )

        # write incompleted records first
        for record in partial_records:
            record_data = record.model_dump(mode="json")
            for k, v in record.__dict__.items():
                if v is None:
                    record_data[k] = f"__{k.upper()}__"

            await writer.writerow(record_data)

        for record in complete_records:
            record_data = record.model_dump(mode="json")
            await writer.writerow(record_data)


async def cardmarket_to_csv():
    fpaths = [Path(p) for p in settings.input_dir.glob(settings.input_glob)]

    if not fpaths:
        logger.error(
            f"No files found in {settings.input_dir} given glob = {settings.input_glob}",
        )
        return 1

    try:
        eur_usd_rate = await fetch_eur_to_usd_rate()
        settings.eur_to_usd_multiplier = eur_usd_rate
        logger.info(f"Using exchange rate from ECB: 1 EUR <=> {eur_usd_rate} USD")
    except Exception as exc:  # noqa: BLE001
        logger.exception(exc)
        logger.info(
            f"Using default exchange rate: 1 EUR <=> {settings.eur_to_usd_multiplier} USD",
        )

    parse_results = await asyncio.gather(
        *(asyncio.create_task(process_order(fpath)) for fpath in fpaths),
    )

    move_tasks: list[asyncio.Task[tuple[Path, Path]]] = []
    to_write: list[ArticleRecord | PartialArticleRecord] = []

    for fpath, maybe_articles in parse_results:
        articles = [res for res in maybe_articles if not isinstance(res, BaseException)]
        exceptions = [res for res in maybe_articles if isinstance(res, BaseException)]

        if exceptions:
            logger.error(
                f"n={len(exceptions)} exception(s) during parsing of articles from {fpath}: {[str(exc) for exc in exceptions]}",
            )

        to_write.extend(articles)

        if not articles:
            logger.warning(f"Failed to parse any articles from '{fpath}'")
            move_to = settings.completed_none_dir
        elif any(type(art) is PartialArticleRecord for art in articles):
            move_to = settings.partial_dir
        else:
            move_to = settings.completed_dir

        move_tasks.append(
            asyncio.create_task(
                async_move_and_mkdir(fpath, move_to),
            ),
        )

    if to_write:
        csv_out_path = settings.output_csv_dir.joinpath(
            f"out-{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.csv",
        )

        await aiofiles.os.makedirs(csv_out_path.parent, exist_ok=True)
        await write_results_csv(csv_out_path, to_write)

    # done last in case other stuff fails
    if move_tasks:
        moved_input_files = await asyncio.gather(*move_tasks)

        for fpath, dst_dir in moved_input_files:
            logger.info(f"Moved input file '{fpath.name}' to '{dst_dir}'")

    return 0


if __name__ == "__main__":
    init_logger()
    exit_code = asyncio.run(cardmarket_to_csv())
    sys.exit(exit_code)
