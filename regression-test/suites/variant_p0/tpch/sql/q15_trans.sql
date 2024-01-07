-- SELECT
--   CAST(var["S_SUPPKEY"] AS INT),
--   CAST(var["S_NAME"] AS TEXT),
--   CAST(var["S_ADDRESS"] AS TEXT),
--   CAST(var["S_PHONE"] AS TEXT),
--   TOTAL_REVENUE
-- FROM
--   suppl Sier,
--   revenuRe1 
-- WHERE
--   CAST(var["S_SUPPKEY"] AS INT) = SUPPLIER_NO
--   AND TOTAL_REVENUE = (
--     SELECT MAX(TOTAL_REVENUE)
--     FROM
--       revenue1 
--   )
-- ORDER BY
--   CAST(var["S_SUPPKEY"] AS INT);