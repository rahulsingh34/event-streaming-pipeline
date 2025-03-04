CREATE TABLE listening_activity (
    user_id INT,
    activity TEXT,
    event_timestamp INT,
    ip_address TEXT,
    PRIMARY KEY (user_id, activity, event_timestamp, ip_address)
);
