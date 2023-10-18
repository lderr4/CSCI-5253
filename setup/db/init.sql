DROP TABLE IF EXISTS animal_dim;
DROP TABLE IF EXISTS outcome_dim;
DROP TABLE IF EXISTS time_dim;
DROP TABLE IF EXISTS outcomes_fact;


-- Animal dimension table
CREATE TABLE animal_dim (
    Animal_ID VARCHAR PRIMARY KEY,
    Name VARCHAR,
    Date_of_Birth DATE,
    Animal_Type VARCHAR,
    Breed VARCHAR,
    Color VARCHAR,
    Sex VARCHAR
    
);


-- Outcome dimension table
CREATE TABLE outcome_dim (
    Outcome_ID SERIAL PRIMARY KEY,
    Outcome_Type VARCHAR,
    Outcome_Subtype VARCHAR,
    Age_upon_Outcome FLOAT
);

-- Time dimension table
CREATE TABLE time_dim (
    Time_ID SERIAL PRIMARY KEY,
    DateTime TIMESTAMP,
    Month VARCHAR,
    Year VARCHAR
);

-- Fact Table
CREATE TABLE outcomes_fact (
    Outcomes_Fact_ID SERIAL PRIMARY KEY,
    Animal_ID_FK VARCHAR,
    Outcome_ID_FK SERIAL,
    Time_ID_FK SERIAL,

    FOREIGN KEY (Animal_ID_FK) REFERENCES animal_dim(Animal_ID),
    FOREIGN KEY (Outcome_ID_FK) REFERENCES outcome_dim(Outcome_ID),
    FOREIGN KEY (Time_ID_FK) REFERENCES time_dim(Time_ID)
);

CREATE TABLE TEST(
    test1 VARCHAR
)




