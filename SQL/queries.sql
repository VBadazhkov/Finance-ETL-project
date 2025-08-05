CREATE TABLE default.BTCUSDT(
	open_time DATETIME('Europe/Moscow'),
	open FLOAT,
	high FLOAT,
	low FLOAT,
	close FLOAT,
	volume FLOAT,
	turnover FLOAT,
	load_date DATETIME('Europe/Moscow')
)
ENGINE=ReplacingMergeTree(load_date)
ORDER BY open_time
;

ALTER TABLE default.BTCUSDT
DROP COLUMN turnover,
DROP COLUMN volume
;

CREATE TABLE default.ETHUSDT(
	open_time DATETIME('Europe/Moscow'),
	open FLOAT,
	high FLOAT,
	low FLOAT,
	close FLOAT,
	volume FLOAT,
	turnover FLOAT,
	load_date DATETIME('Europe/Moscow')
)
ENGINE=ReplacingMergeTree(load_date)
ORDER BY open_time
;

ALTER TABLE default.ETHUSDT
DROP COLUMN turnover,
DROP COLUMN volume