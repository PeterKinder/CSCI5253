-- Question 1

select distinct count(outcome_fact.animal_natural_key), outcome_animal_dim.animal_type
from outcome_fact
inner join outcome_animal_dim
on outcome_fact.animal_id = outcome_animal_dim.animal_id
group by outcome_animal_dim.animal_type;

-- Question 2

select count(animal_natural_key) 
from (
	select animal_natural_key
	from outcome_fact
	group by animal_natural_key
	having count(animal_natural_key) > 1
);

-- Question 3

select outcome_date_month, count(outcome_date_month)
from outcome_date_dim
right join outcome_fact
on outcome_date_dim.outcome_date_id = outcome_fact.outcome_date_id
group by outcome_date_month
order by count(outcome_date_month) desc
limit 5;

-- Question 4

with cat_adoption as (
select outcome_type, animal_type, animal_dob, outcome_date 
from outcome_type_dim
join outcome_fact
on outcome_type_dim.outcome_type_id = outcome_fact.outcome_type_id 
join outcome_animal_dim
on outcome_fact.animal_id = outcome_animal_dim.animal_id
join outcome_date_dim
on outcome_date_dim.outcome_date_id = outcome_fact.outcome_date_id
where outcome_type = 'Adoption'
and animal_type = 'Cat')

(select 'Kitten' as cat_type, count(animal_dob)
from cat_adoption 
where date_part('days', outcome_date - animal_dob::timestamp) < 365)
union all 
(select 'Senior Cat' as cat_type, count(animal_dob)
from cat_adoption 
where date_part('days', outcome_date - animal_dob::timestamp) > 365 * 10)
union all
(select 'Adult' as cat_type, count(animal_dob)
from cat_adoption 
where date_part('days', outcome_date - animal_dob::timestamp) <= 365 * 10
and date_part('days', outcome_date - animal_dob::timestamp) >= 365);

-- Question 5

select outcome_date::date, sum(count(id)) over (order by outcome_date::date)
from outcome_date_dim
join outcome_fact
on outcome_fact.outcome_date_id = outcome_date_dim.outcome_date_id
group by outcome_date::date
order by outcome_date::date desc;