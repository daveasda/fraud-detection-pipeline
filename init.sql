
CREATE TABLE IF NOT EXISTS raw_transactions (
    
   
    transaction_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    merchant_id VARCHAR(255),
    timestamp TIMESTAMP NOT NULL,
    card_last_four VARCHAR(4),
    country VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    merchant_id VARCHAR(255),
    timestamp TIMESTAMP NOT NULL,
    is_fraud BOOLEAN DEFAULT FALSE,
    fraud_score DECIMAL(5, 4),
    fraud_reason VARCHAR(255),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS user_profiles (
    
    user_id VARCHAR(255) PRIMARY KEY,
    avg_spend_7d DECIMAL(10, 2),
    avg_spend_30d DECIMAL(10, 2),
    std_dev_7d DECIMAL(10, 2),
    std_dev_30d DECIMAL(10, 2),
    transaction_count_7d INT,
    transaction_count_30d INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX idx_raw_transactions_user_id ON raw_transactions(user_id);

CREATE INDEX idx_raw_transactions_timestamp ON raw_transactions(timestamp);

CREATE INDEX idx_processed_transactions_is_fraud ON processed_transactions(is_fraud);

CREATE INDEX idx_processed_transactions_user_id ON processed_transactions(user_id);


GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO fraud_user;