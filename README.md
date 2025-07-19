# cardmarket-to-archidekt

## Usage

1. Install required dependancies

- Python 3.13^
- `pip install -r requirements.txt`

2. Save HTML pages from e.g. `https://www.cardmarket.com/en/Magic/Orders/Purchases/Sent` to `./data/input`
3. Run with main.py (**from this directory**, `python main.py`)
4. At the archidekt [import page](https://archidekt.com/collections/import), click 'Add manual column' 8 times. Then, modify the fifth and last column, to "ignore" and "Scryfall ID", respectively. Set the following coloumns, as such:
   <img src="./images/import_cols.png" width="600" alt="columns, showing [quantity, name, finish, condition, ignore, language, price, scryfall_id]"/>

5. Drag the csv from `output/..`, done
