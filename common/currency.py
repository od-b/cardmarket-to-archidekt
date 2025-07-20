from datetime import UTC, datetime, timedelta

import aiohttp
from loguru import logger
from pydantic import AliasPath, BaseModel, Field


class ECBData(BaseModel):
    observations: dict[int, list[float | None]] = Field(
        validation_alias=AliasPath("series", "0:0:0:0:0", "observations"),
    )


class ECBResponse(BaseModel):
    datasets: list[ECBData] = Field(alias="dataSets")


async def fetch_eur_to_usd_rate():
    """
    Get EUR to USD exchange rate from the european central bank API.

    Docs: https://data.ecb.europa.eu/help/api/data
    """
    # set from to 3 days ago so we are sure to get a response. Selects the last one anyhow.
    start_period = datetime.now(UTC) - timedelta(days=3)

    # D. => daily
    url = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A"

    headers = {
        "accept": "application/json",
    }
    params = {
        "format": "jsondata",
        "startPeriod": start_period.strftime("%Y-%m-%d"),
    }

    async with (
        aiohttp.ClientSession(headers=headers) as session,
        session.get(url, params=params) as resp,
    ):
        if not resp.ok:
            logger.error(await resp.text())
            raise ValueError(
                f"Failed to get latest euro to USD exchange rate. Response status = {resp.status}",
            )

        content = await resp.json()
        data = ECBResponse.model_validate(content)

        # logger.debug(json.dumps(content, indent=2))
        # logger.debug(data)

        if not data.datasets or not data.datasets[0].observations:
            raise ValueError("No datasets, adjust timedelta")

        observations = data.datasets[0].observations
        observation_keys = list(observations.keys())

        if not observation_keys:
            raise ValueError("No observations")

        # select the last observation
        latest_observation_key = observation_keys[-1]
        latest_observation = observations[latest_observation_key]

        # first float in the latest observation is the most recent exchange rate
        rate = latest_observation[0]

        if not isinstance(rate, float):
            raise TypeError(
                f"Expected first item in latest observations {latest_observation} to be EUR rate, found {rate}",
            )

        return rate
