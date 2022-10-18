CREATE TABLE IF NOT EXISTS trade (
    trade_id VARCHAR(128) NOT NULL,
	version INTEGER NOT NULL,
	counterparty_id VARCHAR(128),
	book_id VARCHAR(128),
	maturity_date DATE NOT NULL,
	created_date DATE NOT NULL,
	expired BOOLEAN,
    PRIMARY KEY (trade_id, version)
);