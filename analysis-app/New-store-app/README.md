# New-store-app: Promotions Analysis

Essential metrics for placing promotions. Slot-based analysis (Pre vs Post only, no YoY).

## Metrics (DoorDash)

- **Sales** – Total sales
- **Promo driven sales** – From TODC Promo + Corp Promo
- **Ads driven sales** – From TODC Ads + Corp Ads
- **Budget (Total Spend)** – Marketing spend
- **Promo ROAS** – Return on ad spend for promotions
- **Ads ROAS** – Return on ad spend for sponsored listings
- **New Customers** – From marketing promotion data
- **Orders**
- **AOV** – Average order value
- **Cost per Order**
- **CAC** – Customer acquisition cost

## Slot-based Analysis

- **DoorDash**: Uses `Timestamp local time` for slot categorization
- **UberEats**: Uses `Order Accept Time` for slot categorization
- **Slots**: Early morning, Breakfast, Lunch, Afternoon, Dinner, Late night
- **Pre vs Post only** (no Year-over-Year)

## Data Requirements

- DoorDash financial data (dd-data.csv): `Timestamp local date`, `Timestamp local time`, `Subtotal`, `Net total`
- UberEats financial data (ue-data.csv): `Order date`, `Order Accept Time`, `Sales (excl. tax)`, `Total payout`
- Marketing: `MARKETING_PROMOTION*.csv` and `MARKETING_SPONSORED_LISTING*.csv` (same column definitions as main app)

## Run

```bash
cd New-store-app
pip install -r requirements.txt
streamlit run app.py
```

**Note:** If you see `cdstreamlit` error, ensure you run `pip install -r requirements.txt` (the `-r` flag is required).
