SELECT
  CAST(C.var["C_CUSTKEY"] AS INT),
  CAST(C.var["C_NAME"] AS TEXT),
  SUM(CAST(L.var["L_EXTENDEDPRICE"] AS DOUBLE) * (1 - CAST(L.var["L_DISCOUNT"] AS DOUBLE))) AS REVENUE,
  CAST(C.var["C_ACCTBAL"] AS DOUBLE),
  CAST(N.var["N_NAME"] AS TEXT),
  CAST(C.var["C_ADDRESS"] AS TEXT),
  CAST(C.var["C_PHONE"] AS TEXT),
  CAST(C.var["C_COMMENT"] AS TEXT)
FROM
  customer C,
  orders O,
  lineitem L,
  nation N
WHERE
  CAST(C.var["C_CUSTKEY"] AS INT) = CAST(O.var["O_CUSTKEY"] AS INT)
  AND CAST(L.var["L_ORDERKEY"] AS INT) = CAST(O.var["O_ORDERKEY"] AS INT)
  AND CAST(O.var["O_ORDERDATE"] AS DATE) >= DATE '1993-10-01'
  AND CAST(O.var["O_ORDERDATE"] AS DATE) < DATE '1993-10-01' + INTERVAL '3' MONTH
  AND CAST(L.var["L_RETURNFLAG"] AS TEXT) = 'R'
  AND CAST(C.var["C_NATIONKEY"] AS INT) = CAST(N.var["N_NATIONKEY"] AS INT)
GROUP BY
  CAST(C.var["C_CUSTKEY"] AS INT),
  CAST(C.var["C_NAME"] AS TEXT),
  CAST(C.var["C_ACCTBAL"] AS DOUBLE),
  CAST(C.var["C_PHONE"] AS TEXT),
  CAST(N.var["N_NAME"] AS TEXT),
  CAST(C.var["C_ADDRESS"] AS TEXT),
  CAST(C.var["C_COMMENT"] AS TEXT)
ORDER BY
  REVENUE DESC
LIMIT 20