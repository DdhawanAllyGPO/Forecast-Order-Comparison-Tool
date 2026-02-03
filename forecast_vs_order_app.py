import streamlit as st
import pandas as pd
# --------------------------
# 1. Initialize DB connections
# --------------------------
from azure_sql import DatabaseConnection  # replace with actual module

# Forecast / Site info DB
integ_db = DatabaseConnection(
    server="sql-ago-aiq-prd-use.database.windows.net",
    database="sqldb-integration-management-prd",
    driver="{ODBC Driver 17 for SQL Server}",
    localRun='LOCAL'
)

# Order / Purchase DB
order_db = DatabaseConnection(
    server="sql-ago-aiq-prd-use.database.windows.net",
    database="sqldb-order-management-prd",
    driver="{ODBC Driver 17 for SQL Server}",
    localRun='LOCAL'
)

# Make the page layout wide to use more horizontal space
st.set_page_config(
    page_title="Forecast vs Orders",
    layout="wide"
)
st.title("Forecast vs Actual Orders Comparison")

# --------------------------
# 2. User selects site
# --------------------------
sites = ["", "Akron", "Beachwood", "Lorain", "Middleburg", "Westlake", "Cuyahoga"]
selected_site = st.selectbox("Select a Site", sites, index=0)  # starts at first option (empty)

# Stop execution if nothing selected
if selected_site == "":
    st.info("Please select a site to view forecast and orders.")
    st.stop()

# --------------------------
# 3. Get SiteId for the selected site from integration DB
# --------------------------
site_query = f"""
SELECT SiteCode
FROM contractDW.DimSite
WHERE PracticeCode = 293
  AND Name LIKE '%{selected_site}%'
"""
site_df = integ_db.read_sql(site_query)

if site_df.empty:
    st.error(f"No site found for {selected_site}")
    st.stop()

site_id = site_df.iloc[0]["SiteCode"]

# --------------------------
# 4. Fetch Forecast for yesterday (integration DB)
# --------------------------
forecast_query = f"""
SELECT 
    fd.ProductName,
    fd.NDC,
    fd.OrderQty,
    fd.OrderUOM,
    fd.ParMin,
    fd.ParMax,
    fd.ForecastQty,
    fd.DispensedQty,
    fd.PendingTransferQty,
    fd.PendingOrderedQty,
    fd.CurrentInventoryQty
FROM [iq].[ForecastDetails] fd
JOIN [iq].[ForecastHistory] fh
    ON fd.ForecastId = fh.Id
WHERE fh.CreatedDate >= CAST(GETDATE() - 1 AS date)
  AND fh.CreatedDate < CAST(GETDATE() AS date)
  AND fh.SiteId = {site_id}
"""
forecast_df = integ_db.read_sql(forecast_query)

# Rename for comparison
forecast_df = forecast_df.rename(columns={"OrderQty": "ForecastedOrderQty"})

st.subheader("Forecasted Orders (Yesterday)")
st.dataframe(forecast_df, width=1500)

# --------------------------
# 5. Check OrderStatus (order DB)
# --------------------------
status_query = f"""
SELECT OrderStatusId, PurchaseOrderId
FROM dbo.PurchaseOrderDetails pod
WHERE IsLatest = 1
  AND PurchaseOrderId IN (
        SELECT id
        FROM dbo.PurchaseOrders
        WHERE SiteId = {site_id}
          AND CreatedDate >= CAST(GETDATE() - 1 AS date)
          AND CreatedDate < CAST(GETDATE() AS date)
  )
"""
status_df = order_db.read_sql(status_query)

if any(status_df["OrderStatusId"].isin([1,6])):
    st.error("One or more orders for this site yesterday have invalid status (1 or 6). Cannot proceed.")
    st.stop()

# --------------------------
# 6. Fetch actual orders (order DB) with renamed column
# --------------------------
orders_query = f"""
SELECT pli.NDC, pli.DrugName, pli.Quantity
FROM dbo.PoLineItems pli
WHERE PurchaseOrderId IN (
    SELECT id
    FROM dbo.PurchaseOrders
    WHERE SiteId = {site_id}
      AND CreatedDate >= CAST(GETDATE() - 1 AS date)
      AND CreatedDate < CAST(GETDATE() AS date)
)
"""
orders_df = order_db.read_sql(orders_query)

# Rename quantity column for comparison
orders_df = orders_df.rename(columns={"Quantity": "OrderedQty"})

st.subheader("Actual Orders (Yesterday)")
st.dataframe(orders_df, width=1500)

# --------------------------
# 7. Simplified Merge for comparison
# --------------------------
comparison_df_simple = pd.merge(
    forecast_df[["ProductName", "NDC", "ForecastedOrderQty"]],
    orders_df[["NDC", "OrderedQty"]],
    on="NDC",
    how="outer"
)

# Fill missing quantities with 0
comparison_df_simple["ForecastedOrderQty"] = comparison_df_simple["ForecastedOrderQty"].fillna(0)
comparison_df_simple["OrderedQty"] = comparison_df_simple["OrderedQty"].fillna(0)

# --------------------------
# 8. Conditional highlighting
# --------------------------
def highlight_qty(row):
    if row["ForecastedOrderQty"] == 0 or row["OrderedQty"] == 0:
        return ['background-color: lightcoral']*len(row)  # Red
    elif row["ForecastedOrderQty"] == row["OrderedQty"]:
        return ['background-color: lightgreen']*len(row)  # Green
    else:
        return ['background-color: lightyellow']*len(row)  # Yellow

st.subheader("Forecast vs Actual Comparison")
st.dataframe(comparison_df_simple.style.apply(highlight_qty, axis=1), width=1500)
