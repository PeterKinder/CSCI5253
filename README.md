In this lab, you will expand on you work from Lab 01.

Assignment

    * Use dimensional modeling to model the data in data warehouse in a PostgreSQL database
    * Provide an Entity-Relationship diagram for how this data would have been represented if it used Third Normal Form. You do NOT need to implement 3NF -- just provide a diagram image.
    * Modify the ETL pipeline from Lab 01 to load the data into the PostgreSQL database which will serve as a data warehouse
    * Use Docker Compose to containerize the ETL pipeline and the database
    * Once the data is in the warehouse, use SQL to run analysis on it (see Criteria below for exact queries)

Data

You can use the Austin Shelter Outcomes data.Links to an external site. Here's a link for a smaller dataset for testing: https://shelterdata.s3.amazonaws.com/shelter1000.csvLinks to an external site.

You can use a different dataset if you prefer, but please contact Alex ahead of submission to clarify Acceptance Criteria.

Acceptance criteria

The following are acceptance criteria. You can use any method to accomplish them, as long as the functionality and deliverables match what is described:

Data modeling:

    * Data in the Postgres database serving as a data warehouse should modeled according to Kimball's dimensional modeling
        * there should be a fact table
        * there should be at least 3 dimension tables

    Notes: 

    If using the Austin Shelter Outcomes dataset:

        * pay attention to the "sex" column: it combines inherent property of an animal property of an outcome. You might want to separate it into two columns!
        * Not all columns from the original dataset need to be present in the target warehouse . For example, do we need an "age" column and a date of birth column? We might, but we also might not! It's up to you to make these decisions.
    * In addition to the implementation of the Kimball's model, provide an entity-relationship diagram (ERD) of how this data would be represented using the Third Normal Form. You do not need to implement it in SQL, just provide a diagram.

Containerization:

    - There is a docker compose file that contains two services, one containerizing the database and another containerizing the pipeline
the database service has the following features:
       - it uses a docker volume to create persistent storage
       - it uses an initialization script to create table(s) that will store the data. The tables should be created according to the dimensional data model.
       - it maps the 5432 port inside a container to a port on the host machine (can be 5432 or different)
       - it uses either environment variables or environment file to pass the information needed to start the container (postgres user, postgres password, and postgres database
the pipeline service has the following features:
        - it builds from the image defined by a (possibly modified) Dockerfile from Lab 01
        - it is executed after ("depends on") the database service
        - it uses an environment variable or environment file to store the database connection string
Pipeline functionality
The modified ETL pipeline should do the following:

everything described in Lab 01, but  instead of saving the results into a csv file, it connects to the database (specified in docker-compose.yml -- see above) and loads the data there
the data transformation step, in particular, should make the data consistent with the table schema in the target warehouse.
Note: the transformation step might also do other things like handling missing values, cleaning data (e.g. removing * from names) but those are not a hard requirement of this assignment

SQL analysis
A single .sql file that contains the queries answering the following questions:

How many animals of each type have outcomes?
I.e. how many cats, dogs, birds etc. Note that this question is asking about number of animals, not number of outcomes, so animals with multiple outcomes should be counted only once.
How many animals are there with more than 1 outcome?
What are the top 5 months for outcomes? 
Calendar months in general, not months of a particular year. This means answer will be like April, October, etc rather than April 2013, October 2018, 
A "Kitten" is a "Cat" who is less than 1 year old. A "Senior cat" is a "Cat" who is over 10 years old. An "Adult" is a cat who is between 1 and 10 years old.
What is the percentage of kittens, adults, and seniors, whose outcome is "Adopted"?
Conversely, among all the cats who were "Adopted", what is the percentage of kittens, adults, and seniors?
For each date, what is the cumulative total of outcomes up to and including this date?
Note: this type of question is usually used to create dashboard for progression of quarterly metrics. In SQL, this is usually accomplished using something called Window Functions. You'll need to research and learn this on your own!

Note that in class we've seen how to connect an editor like DBeaver to a dockerized database. You could use this to write and test your queries!

Submission
Check the following into a Github repo:

docker-compose.yml
If you are using env file instead of environment variables, then include a template of the .env file
Dockerfile needed for the ETL pipeline
ETL pipeline .py file(s)
.sql file containing the queries answering the questions above
An image containing the 3NF ERD
Open a PR and submit the link.