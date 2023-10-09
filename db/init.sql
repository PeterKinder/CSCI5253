CREATE TABLE outcome_fact (
    id INT PRIMARY KEY,
    animal_id INT,
    outcome_type_id INT,
    outcome_date_id INT,
    animal_natural_key VARCHAR
);

CREATE TABLE animal_dim (
    animal_id INT PRIMARY KEY,
    animal_natural_key VARCHAR,
    animal_name VARCHAR,
    animal_type VARCHAR,
    animal_breed VARCHAR,
    animal_color VARCHAR,
    animal_sex VARCHAR,
    animal_dob DATE
);

CREATE TABLE outcome_type_dim (
    outcome_type_id INT PRIMARY KEY,
    outcome_type VARCHAR,
    outcome_type_subtype VARCHAR,
    outcome_type_neutered VARCHAR
);

CREATE TABLE outcome_date_dim (
    outcome_date_id INT PRIMARY KEY,
    outcome_date_year INT,
    outcome_date_month INT,
    outcome_date_day INT,
    outcome_date_hour INT,
    outcome_date_minute INT
);