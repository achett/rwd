drop table if exists sandbox_truven.ac_pregnancy_codes;
create table sandbox_truven.ac_pregnancy_codes as

select distinct code as code, case when code is not NULL then 'pregnancy' else NULL end as criteria
from sandbox_truven.ac_icd10_descriptions
where code between 'O09%' and 'O48%'
or code between '640%' and '649%'
or code ilike 'Z34%'
or code ilike 'V23%'
or code in (
'V241',
'Z331',
'Z333',
'V220',
'V221',
'V222'
)



