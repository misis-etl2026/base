CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    name TEXT,
    country TEXT,
    signup_date DATE
);


CREATE TABLE transactions (
    transaction_id INTEGER,
    user_id INTEGER,
    amount NUMERIC,
    currency TEXT,
    event_time TIMESTAMP
);


INSERT INTO users (user_id, name, country, signup_date) VALUES
(1, 'Alice', 'DE', '2023-01-10'),
(2, 'Bob', 'NL', '2023-03-15'),
(3, 'Charlie', 'DE', '2023-05-20'),
(4, 'Diana', 'FR', '2023-07-01'),
(5, 'Eve', 'NL', '2023-09-10');

INSERT INTO users (user_id, name, country, signup_date) VALUES
(6, 'Alice', 'BR', '2025-01-10');



INSERT INTO transactions (transaction_id, user_id, amount, currency, event_time) VALUES
(1, 1, 100, 'EUR', '2024-01-01 10:15:00'),
(2, 1, 50, 'EUR', '2024-01-02 11:00:00'),
(3, 2, 200, 'EUR', '2024-01-02 12:30:00'),
(4, 3, 150, 'EUR', '2024-01-03 09:45:00'),
(5, 3, -20, 'EUR', '2024-01-04 13:20:00'), 
(6, 4, 300, 'EUR', '2024-01-05 16:10:00'),
(7, 5, 120, 'EUR', '2024-01-06 08:05:00'),
(4, 3, 150, 'EUR', '2024-01-03 09:45:00'),
(2, 1, 50, 'EUR', '2024-01-02 11:00:00'),
(8, 1, 70, 'EUR', '2024-01-07 10:00:00'),
(9, 1, 30, 'EUR', '2024-01-08 12:00:00'),
(10, 2, 90, 'EUR', '2024-01-09 14:00:00');


SELECT *
  FROM transactions;

SELECT transaction_id as tr_id, amount
  FROM transactions;


SELECT distinct transaction_id, amount
  FROM transactions;


SELECT *
  FROM transactions
 WHERE amount > 0;
 

SELECT *
  FROM transactions
 WHERE (amount > 0
        AND user_id = 1) 
       OR EVENT_TIME <= date'2024-01-05';


SELECT *
  FROM transactions
 ORDER BY event_time DESC;


SELECT *
  FROM transactions
 ORDER BY 2;


SELECT user_id, SUM(amount) as sum_amount, avg(amount) as avg_amount
  FROM transactions
 where amount >= 0
 GROUP BY user_id
 having SUM(amount) > 200;



SELECT t.transaction_id,
       u.name,
       u.country,
       t.amount
  FROM transactions t
       JOIN users u
       ON t.user_id = u.user_id;


select t.transaction_id,
       u.name,
       u.country,
       t.amount
  from transactions t, users u
 where t.user_id = u.user_id;



SELECT *
  FROM users u
       left JOIN transactions t
       ON t.user_id = u.user_id
 where t.user_id is null;


SELECT user_id, 
       amount,
       event_time,
       avg(amount) over (partition by user_id) AS total_amount, 
       (amount - lag(amount) over (partition by user_id order by event_time asc)) / amount * 100 AS percent_diff_amount
  FROM transactions;



WITH clean_data AS (
    SELECT *
      FROM transactions
     WHERE amount > 0
)
SELECT user_id, 
       SUM(amount)
  FROM clean_data
 GROUP BY user_id;


SELECT user_id, 
       SUM(amount)
  FROM (
  		SELECT *
          FROM transactions
         WHERE amount > 0
       )
 GROUP BY user_id;



WITH clean_transactions AS (
    SELECT *
      FROM transactions
     WHERE amount > 0
),
joined_data AS (
    SELECT t.*, 
           u.country
      FROM clean_transactions t
           JOIN users u
           ON t.user_id = u.user_id
)
SELECT country, 
       SUM(amount)
  FROM joined_data
 GROUP BY country
 limit 2;



CREATE INDEX idx_user_id
    ON transactions(user_id);

CREATE INDEX idx_tr_user_id
    ON transactions(user_id, transaction_id);


drop table medium_transactions;

select * 
  from transactions
 where user_id = 1;


CREATE INDEX idx_user_id_medium
    ON medium_transactions(user_id);

drop table medium_transactions_part; 

CREATE TABLE medium_transactions_part (
    transaction_id INTEGER,
    user_id INTEGER,
    amount NUMERIC,
    currency TEXT,
    event_time TIMESTAMP
) PARTITION BY HASH (user_id);


CREATE TABLE medium_transactions_part_0 PARTITION OF medium_transactions_part
    FOR VALUES WITH (MODULUS 4, REMAINDER 0);

CREATE TABLE medium_transactions_part_1 PARTITION OF medium_transactions_part
    FOR VALUES WITH (MODULUS 4, REMAINDER 1);

CREATE TABLE medium_transactions_part_2 PARTITION OF medium_transactions_part
    FOR VALUES WITH (MODULUS 4, REMAINDER 2);

CREATE TABLE medium_transactions_part_3 PARTITION OF medium_transactions_part
    FOR VALUES WITH (MODULUS 4, REMAINDER 3);


select count(1) from medium_transactions_part_0;

select count(1) from medium_transactions_part_1;

select count(1) from medium_transactions_part_2;

select count(1) from medium_transactions_part_3;

