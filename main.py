from datetime import datetime
import json
from typing import Any, Literal
from bs4 import BeautifulSoup
import csv
import asyncio
import aiohttp
from pydantic import BaseModel, Field, model_validator
import glob
from pathlib import Path
from loguru import logger
import shutil

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

    def notify_empty_fields(self):
        has_none_field = False

        for k, v in self.__dict__.items():
            if v is None:
                has_none_field = True
                print(f"{self.name} -- field '{k}' needs manual input")

        return has_none_field


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


async def main():
    fpaths = glob.glob(f"{INPUT_DIR}/{INPUT_GLOB}")

    if not fpaths:
        logger.error(f"No files found given glob = {f'{INPUT_DIR}/{INPUT_GLOB}'}")
        exit(1)

    completed_dir = Path(COMPLETED_DIR).resolve()
    output_dir = Path(OUTPUT_DIR).resolve()
    completed_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    outpath = output_dir.joinpath(f"out-{datetime.now()}.csv")

    # should make file i/o stuff async if adding a very large amount of stuff

    for fp in fpaths:
        with open(fp) as infile:
            soup = BeautifulSoup(infile, "html.parser")

        articles = await parse_article_soup(soup)
        write_csv_header = outpath.is_file()

        with open(outpath, "a+") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=CSV_HEADER)
            if write_csv_header:
                writer.writeheader()

            for article in articles:
                article.notify_empty_fields()
                writer.writerow(json.loads(article.model_dump_json()))

        shutil.move(fp, completed_dir)


if __name__ == "__main__":
    asyncio.run(main())
