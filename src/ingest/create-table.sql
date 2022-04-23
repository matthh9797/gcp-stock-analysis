create or replace table `data-science-on-gcp-323609.gcp_stock_analysis.stock_price`
(
    Date DATE,
    Open FLOAT64,
    High FLOAT64,
    Low FLOAT64,
    Close FLOAT64,
    Volume NUMERIC,
    Dividend NUMERIC,
    Stock_Splits NUMERIC
)
;