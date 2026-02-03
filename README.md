# Forecast vs Order Comparison Tool

This is a **Streamlit app** to compare forecasted orders versus actual orders for a selected site.  
It connects to **Azure SQL databases** (integration and order management) to fetch data.

---

## **Features**

- Select a site from a dropdown to view yesterday’s data.
- Fetch **forecasted orders** for the selected site.
- Fetch **actual orders** from purchase order tables.
- Highlight differences:
  - **Green**: Forecast matches actual orders.
  - **Yellow**: Forecast and actual differ.
  - **Red**: Either forecast or actual is zero.
- Comparison table shows:
  - `ProductName` (from forecast)  
  - `NDC`  
  - `ForecastedOrderQty`  
  - `OrderedQty`  

---

## **Files in Repo**

- `forecast_vs_order_app.py` — Main Streamlit app.  
- `azure_sql.py` — Database connection helper using `pyodbc` + Azure identity.  
- `requirements.txt` — Python dependencies for the app.
