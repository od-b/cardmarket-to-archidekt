import json
import sys
import glob
import shutil
import logging
import csv
from typing import Any, Literal
from datetime import datetime
from pathlib import Path

import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, model_validator
from loguru import logger


logger.add(sys.stderr, level=logging.DEBUG)

DATA_DIR = "./data"
INPUT_GLOB = "*.html"

EUR_TO_USD_MULTIPLIER = 1.16  # todo: find a API that provides this


COMPLETED_DIR = f"{DATA_DIR}/completed"
INPUT_DIR = f"{DATA_DIR}/input"
OUTPUT_DIR = f"{DATA_DIR}/output"

CONDITION_MAPPING = {
    "1": "M",
    "2": "NM",
    "3": "LP",
    "4": "MP",
    "5": "MP",
    "6": "HP",
    "7": "D",
}

CSV_HEADER = [
    "quantity",
    "name",
    "finish",
    "condition",
    "product_id",
    "language",
    "price",
    "scryfall_id",
]


class CsvModel(BaseModel):
    @classmethod
    def csv_header(cls):
        # not used, as it's better to control the order of fields explicitly (faster to import on archidekt with preset cols)
        schema = cls.model_json_schema(by_alias=False)
        props: dict[str, Any] | None = schema.get("properties")
        assert isinstance(props, dict)

        return [k for k in props.keys()]


class ArticleAttributes(CsvModel):
    quantity: int = Field(default=1, alias="data-amount")
    name: str = Field(alias="data-name")
    product_id: str = Field(alias="data-product-id")
    condition: str | None = Field(default=None, alias="data-condition")
    # collnumber: int | None = Field(default=None, alias="data-number")
    price: float | None = Field(default=None, alias="data-price")


class ScryfallData(CsvModel):
    scryfall_id: str = Field(alias="id")
    # set_id: str = Field(alias="set_id")
    language: str = Field(alias="lang")


class Article(ScryfallData, ArticleAttributes):
    finish: Literal["Foil"] | Literal["Normal"]

    @model_validator(mode="after")
    def map_fields(self):
        # bit janky, but whatever
        if self.condition and self.condition not in CONDITION_MAPPING.values():
            self.condition = CONDITION_MAPPING.get(self.condition, None)

        if self.price:
            self.price = round(EUR_TO_USD_MULTIPLIER * self.price, 2)

        return self

    def get_nonefields(self):
        return self.name, [k for k, v in self.__dict__.items() if v is None]

    def has_nonefield(self):
        for v in self.__dict__.values():
            if v is None:
                return True

        return False


async def fetch_scryfall_data(product_id: str):
    """
    Docs:
    https://scryfall.com/docs/api/cards/cardmarket
    """

    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://api.scryfall.com/cards/cardmarket/{product_id}"
        ) as resp:
            if not resp.ok:
                logger.error(
                    f"Failed to find product id {product_id} using scryfall API. "
                    f"Response status = {resp.status}, text = {await resp.text()}"
                )
                raise ValueError(product_id)

            content = await resp.json()
            return ScryfallData.model_validate(content)


async def parse_article(art: Any):
    # BeautifulSoup seems to lack exported types, so ignore for now
    article_attrs = ArticleAttributes.model_validate(art.attrs)  # type: ignore
    finish = (
        "Foil" if art.find("span", attrs={"title": "Foil"}) else "Normal"  # type: ignore
    )
    scryfall_data = await fetch_scryfall_data(article_attrs.product_id)

    return Article.model_validate(
        {
            **article_attrs.model_dump(by_alias=True),
            **scryfall_data.model_dump(by_alias=True),
            "finish": finish,
        }
    )


async def parse_article_soup(soup: BeautifulSoup):
    html_articles = soup.find_all("tr", attrs={"data-article-id": True})
    tasks = [asyncio.create_task(parse_article(art)) for art in html_articles]
    articles = await asyncio.gather(*tasks)

    return articles


async def process_purchase(html_path: Path):
    print(f"Processing HTML: {html_path}")

    async with aiofiles.open(html_path) as infile:
        contents = await infile.read()
        soup = BeautifulSoup(contents, "html.parser")

    articles = await parse_article_soup(soup)

    return articles


async def main():
    # this function used blocking i/o,
    # doesn't really matter unless this is exported as a module at some point

    glob_paths = glob.glob(f"{INPUT_DIR}/{INPUT_GLOB}")

    if not glob_paths:
        logger.error(f"No files found given glob = {f'{INPUT_DIR}/{INPUT_GLOB}'}")
        exit(1)

    fpaths = [Path(p) for p in glob_paths]
    completed_dir = Path(COMPLETED_DIR).resolve()
    output_dir = Path(OUTPUT_DIR).resolve()
    completed_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    outpath = output_dir.joinpath(f"out-{datetime.now()}.csv")

    results = await asyncio.gather(
        *(asyncio.create_task(process_purchase(fpath)) for fpath in fpaths)
    )
    needs_input: list[tuple[Path, Article]] = []
    n_articles_total = 0

    with open(outpath, "a+") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=CSV_HEADER)
        writer.writeheader()

        for fpath, articles in zip(fpaths, results):
            n_articles = len(articles)
            n_articles_total += n_articles

            print(f"Writing {n_articles} articles from {fpath}")

            for article in articles:
                if article.has_nonefield():
                    needs_input.append((fpath, article))

                writer.writerow(json.loads(article.model_dump_json()))

    print(f"Moving parsed files to {completed_dir}")

    for fpath in fpaths:
        shutil.move(fpath, completed_dir)

    print(f"Processed a total of {n_articles_total} articles")

    if needs_input:
        print(f"WARNING: The following {len(needs_input)} articles need manual input:")
        for fpath, article in needs_input:
            card_name, none_keys = article.get_nonefields()

            print("---")
            print(f"Card '{card_name}' (in {fpath})")
            for k in none_keys:
                print(f"> {k}")
    else:
        print("OK: No articles in need of manual input")


if __name__ == "__main__":
    asyncio.run(main())
