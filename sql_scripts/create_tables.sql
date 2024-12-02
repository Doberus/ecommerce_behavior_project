CREATE TABLE user_behavior (
    event_time TIMESTAMP,
    event_type VARCHAR(50),
    product_id INT,
    category_id INT,
    category_code VARCHAR(255),
    brand VARCHAR(100),
    price NUMERIC,
    user_id INT,
    user_session VARCHAR(100)
);
