## Cardmarket to Archidekt

Script to semi-automate cardmarket orders to csv, enabling easy import to e.g. archidekt.

### Features

- Process any amount of orders at once, producing a single CSV file for import
- Fetches card info from Scryfall API by the Cardmarket product ID scraped from downloaded HTML pages
- Automatically fetches current EUR to USD exchange rate from the European Central Bank (ECB) API

## Usage

**Not tested on windows. Please let me know if it works/doesnt.**

### Cardmarket to CSV

1. Install the requirements (see below)
2. Download one or more HTML-pages including orders, e.g. from any order under `https://www.cardmarket.com/en/Magic/Orders/Purchases/Sent` ('cmd/ctrl + s' on the page, select format: `Webpage, HTML Only`, and store in the `data/input/` directory with this project.)
3. Run with main.py `uv run main.py` to produce a CSV file with the articles. If manual input is needed for certain records, you will be informed.

### Importing to Archidekt

1. Produce the CSV and resolve any partial records needing manual input (delete rows or replace all placeholders with the correct values)
2. At the archidekt [import page](https://archidekt.com/collections/import), set up the following columns. (click **"Add manual column"** 8 times, then modify the last one to say "Scryfall Id")  
   <img src="./import_columns.png" width="600" alt="columns, showing [quantity, name, finish, condition, ignore, language, price, scryfall_id]"/>
3. Upload or drag the generated CSV (located at `data/records/`)

## Requirements / Installation

1. [UV](https://docs.astral.sh/uv/getting-started/installation/)

- If you have homebrew, you can install with `brew install uv`. Alternatively, run `curl -LsSf https://astral.sh/uv/install.sh | sh`.

2. Install dependencies with `uv sync`

Semi-automated tool to import Cardmarket purchases into Archidekt collection

### TODO

- Automate HTML downloads by manually resolving the auth or using something like selenium
