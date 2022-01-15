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

create table sandbox_truven.ac_Pca_compliance_claims as

select * from truven_commercial_claims_union.outpatient_pharmaceutical_claims
where ndcnum in (select ndcnum from sandbox_truven.ac_Pca_compliance);

drop table if exists sandbox_truven.ac_Pca_compliance_claims2; 
create table sandbox_truven.ac_Pca_compliance_claims2 as

select enrolid as id, cast(to_date(svcdate, 'MM/DD/YYYY') as timestamp) as date, ndcnum as code, daysupp as days_of_supply from sandbox_truven.ac_Pca_compliance_claims;