SELECT carrier, COUNT(*) AS rows, MIN(price_usd) AS min_usd, MAX(price_usd) AS max_usd
FROM rates.rate_matrix GROUP BY carrier ORDER BY carrier;

SELECT * FROM rates.rate_matrix
WHERE lane_id = 'INMAA-NLRTM' AND container_type = '40HC' ORDER BY carrier;

SELECT reject_reason, COUNT(*) FROM rates.rejects GROUP BY 1 ORDER BY 2 DESC;

SELECT * FROM rates.load_runs ORDER BY loaded_at DESC LIMIT 5;
