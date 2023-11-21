CREATE TABLE IF NOT EXISTS outcome_animal_dim (
    animal_id VARCHAR PRIMARY KEY,
    animal_natural_key VARCHAR,
    animal_name VARCHAR,
    animal_type VARCHAR,
    animal_breed VARCHAR,
    animal_color VARCHAR,
    animal_sex VARCHAR,
    animal_dob DATE
);

CREATE TABLE IF NOT EXISTS outcome_type_dim (
    outcome_type_id VARCHAR PRIMARY KEY,
    outcome_type VARCHAR,
    outcome_type_subtype VARCHAR,
    outcome_type_neutered VARCHAR
);

CREATE TABLE IF NOT EXISTS outcome_date_dim (
    outcome_date_id VARCHAR PRIMARY KEY,
    outcome_date TIMESTAMP,
    outcome_date_year INT,
    outcome_date_month INT,
    outcome_date_day INT
);

CREATE TABLE IF NOT EXISTS outcome_fact_table (
    id SERIAL PRIMARY KEY,
    animal_natural_key VARCHAR,
    animal_id VARCHAR,
    outcome_type_id VARCHAR,
    outcome_date_id VARCHAR,
    FOREIGN KEY (animal_id) REFERENCES outcome_animal_dim(animal_id),
    FOREIGN KEY (outcome_type_id) REFERENCES outcome_type_dim(outcome_type_id),
    FOREIGN KEY (outcome_date_id) REFERENCES outcome_date_dim(outcome_date_id)
);