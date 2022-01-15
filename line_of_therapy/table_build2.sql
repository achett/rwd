create table sandbox_truven.ac_Pca_compliance as

select * from truven_lookups.redbook 
where upper(prodnme) ilike '%XTANDI%' or
upper(prodnme) ilike '%ZYTIGA%' or
upper(prodnme) ilike '%ERLEADA%' or
upper(prodnme) ilike '%YONSA%' or
upper(prodnme) ilike '%ABIRATERONE ACETATE%' or 
upper(gennme) ilike '%XTANDI%' or
upper(gennme) ilike '%ZYTIGA%' or
upper(gennme) ilike '%ERLEADA%' or
upper(gennme) ilike '%YONSA%' or
upper(gennme) ilike '%ABIRATERONE ACETATE%';

create table sandbox_truven.ac_Pca_compliance_claims_names as

select t1.*, t2.gennme, t2.prodnme from truven_commercial_claims_union.outpatient_pharmaceutical_claims t1
left join sandbox_truven.ac_Pca_compliance t2 on t1.ndcnum = t2.ndcnum
where t1.ndcnum in (select ndcnum from sandbox_truven.ac_Pca_compliance);

drop table if exists sandbox_truven.ac_Pca_compliance_claims_names2; 
create table sandbox_truven.ac_Pca_compliance_claims_names2 as

select enrolid as id, cast(to_date(svcdate, 'MM/DD/YYYY') as timestamp) as date, prodnme as code, daysupp as days_of_supply from sandbox_truven.ac_Pca_compliance_claims_names;