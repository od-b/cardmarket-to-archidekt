import json
import glob
import shutil
import csv

from typing import Any, Literal
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
from pydantic import AliasPath, BaseModel, Field, model_validator
from loguru import logger

DATA_DIR = "./data"
INPUT_GLOB = "*.html"

COMPLETED_DIR = f"{DATA_DIR}/completed"
INPUT_DIR = f"{DATA_DIR}/input"
OUTPUT_DIR = f"{DATA_DIR}/output"

EXCHANGE_RATE = {
    "EUR_TO_USD": 1.16,
}
""" 
Defaults to this value of fetch to ECB fails
"""

CONDITION_MAPPING = {
    "1": "M",
    "2": "NM",
    "3": "LP",
    "4": "MP",
    "5": "MP",
    "6": "HP",
    "7": "D",
}

# these could be found programmatically (as in the csv_header method), but it's easier to import into archidekt like this
# not used, as it's better to control the order of fields explicitly (faster to import on archidekt with preset cols)
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


class ECBData(BaseModel):
    observations: dict[int, list[float | None]] = Field(
        validation_alias=AliasPath("series", "0:0:0:0:0", "observations")
    )


class ECBResponse(BaseModel):
    datasets: list[ECBData] = Field(alias="dataSets")


async def update_eur_to_usd_rate():
    """
    Get EUR to USD exchange rate from the european central bank API.

    Overkill, but free and its sure to stay available.

    Docs: https://data.ecb.europa.eu/help/api/data
    """
    # set from to 3 days ago so we are sure to get a response. Selects the last one anyhow.
    yesterday = datetime.now(UTC) - timedelta(days=3)

    # .D => daily
    url = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A"
    headers = {
        "accept": "application/json",
    }
    params = {
        "format": "jsondata",
        "startPeriod": yesterday.strftime("%Y-%m-%d"),
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, params=params) as resp:
            if not resp.ok:
                logger.error(
                    f"Failed to get latest euro to USD exchange rate. "
                    f"Defaulting to 1EUR = {EXCHANGE_RATE['EUR_TO_USD']}USD"
                )
                return False

            content = await resp.json()
            # logger.debug(json.dumps(content, indent=2))
            data = ECBResponse.model_validate(content)
            # logger.debug(data)

            if not data.datasets or not data.datasets[0].observations:
                logger.error("No datasets, adjust timedelta")
                return False

            # select the last observation
            observations = data.datasets[0].observations
            latest_observation = observations[list(observations.keys())[-1]]

            if not latest_observation:
                logger.error("No observations")
                return False

            rate = latest_observation[0]

            if not isinstance(rate, float):
                logger.error(
                    f"Expected first item in latest observations {latest_observation} to be EUR rate, found {rate}"
                )
                return False

            EXCHANGE_RATE["EUR_TO_USD"] = rate

            return True


class CsvModel(BaseModel):
    @classmethod
    def csv_header(cls):
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
            self.price = round(EXCHANGE_RATE["EUR_TO_USD"] * self.price, 2)

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

    await update_eur_to_usd_rate()
    print(f"Using exchange rate: 1 EUR <=> {EXCHANGE_RATE['EUR_TO_USD']} USD")

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

    with open(outpath, "w") as outfile:
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
