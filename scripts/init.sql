CREATE TABLE branches (
    branch_id SERIAL PRIMARY KEY,
    branch_code VARCHAR(20) UNIQUE,
    city VARCHAR(50)
);

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    pan_number VARCHAR(15) UNIQUE,
    kyc_status VARCHAR(20)
);

CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    branch_id INT REFERENCES branches(branch_id),
    account_type VARCHAR(20),
    balance DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transactions (
    txn_id SERIAL PRIMARY KEY,
    account_id INT REFERENCES accounts(account_id),
    txn_type VARCHAR(10),
    amount DECIMAL(15,2),
    reference_number VARCHAR(50) UNIQUE,
    txn_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE branches REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

INSERT INTO branches (branch_code, city) VALUES ('MUM001', 'Mumbai'), ('DEL001', 'Delhi');
INSERT INTO customers (first_name, last_name, pan_number, kyc_status) VALUES
  ('Rahul', 'Sharma', 'ABCDE1234F', 'VERIFIED'),
  ('Priya', 'Singh', 'VWXYZ9876Q', 'PENDING');
INSERT INTO accounts (customer_id, branch_id, account_type, balance) VALUES
  (1, 1, 'SAVINGS', 50000.00),
  (2, 2, 'CURRENT', 15000.00);
INSERT INTO transactions (account_id, txn_type, amount, reference_number) VALUES
  (1, 'CR', 10000.00, 'REF10001'),
  (1, 'DR', 500.00, 'REF10002'),
  (2, 'CR', 25000.00, 'REF10003');
