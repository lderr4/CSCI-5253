-- Question 1
SELECT animal_type, COUNT(*) AS count
FROM animal_dim ad 
GROUP BY animal_type 
ORDER BY count DESC;

-- Question 2
SELECT COUNT(*) AS count
FROM (
    SELECT animal_id_fk, COUNT(animal_id_fk) AS occurrence_count
    FROM outcomes_fact of2
    GROUP BY animal_id_fk
    HAVING COUNT(animal_id_fk) > 1
) AS subquery;

-- Question 3
SELECT month, COUNT(*) AS count
FROM time_dim td
GROUP BY month 
ORDER BY count desc limit 5;

-- Question 4.1
SELECT COUNT(*)
FROM outcomes_fact of2 
JOIN animal_dim ad ON of2.animal_id_fk = ad.animal_id
JOIN outcome_dim od ON of2.outcome_id_fk = od.outcome_id
WHERE od.outcome_type = 'Adoption' and ad.animal_type = 'Cat';

-- Question 4.2
SELECT
    SUM(CASE
        WHEN ad.animal_type = 'Cat' AND od.age_upon_outcome < 1 and od.outcome_type = 'Adoption' THEN 1
        ELSE 0
    END) AS Kittens,
    SUM(CASE
        WHEN ad.animal_type = 'Cat' AND od.age_upon_outcome >= 1 AND od.age_upon_outcome <= 10 and od.outcome_type = 'Adoption' THEN 1
        ELSE 0
    END) AS Adults,
    SUM(CASE
        WHEN ad.animal_type = 'Cat' AND od.age_upon_outcome > 10 and od.outcome_type = 'Adoption' THEN 1
        ELSE 0
    END) AS Seniors
FROM outcomes_fact of2 
JOIN animal_dim ad ON of2.animal_id_fk = ad.animal_id
JOIN outcome_dim od ON of2.outcome_id_fk = od.outcome_id;


-- Question 5
SELECT
    time_id,
    datetime,
    month,
    year,
    SUM(outcome_count) OVER (ORDER BY datetime) AS cumulative_outcomes
FROM (
    SELECT
        time_id,
        datetime,
        month,
        year,
        COUNT(*) AS outcome_count
    FROM time_dim td
    GROUP BY time_id, datetime, month, year
) subquery
ORDER BY datetime;








