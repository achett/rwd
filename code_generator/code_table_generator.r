code_table_generator <- function(code_type, range_start, range_end, listed_values, table_name, criteria, category)

{
    if(tolower(code_type)=='icd') 
    {
        query_base <- paste0("
        --create table sandbox_grwi.", table_name, " stored as parquet as
        select distinct dx as code, case when dx is not NULL then '", criteria,"' else NULL end as criteria,
        case when dx is not NULL then '", category,"' else NULL end as category  
        from 
        (
        select distinct dx1 as dx from truven_commercial_claims_union.outpatient_services where ", code_list, 
        " union select distinct dx2 as dx from truven_commercial_claims_union.outpatient_services where ", code_list,
        " union select distinct dx3 as dx from truven_commercial_claims_union.outpatient_services where ", code_list,
        " union select distinct dx4 as dx from truven_commercial_claims_union.outpatient_services where ", code_list,
        " union select distinct dx1 as dx from truven_commercial_claims_union.inpatient_services where ", code_list, 
        " union select distinct dx2 as dx from truven_commercial_claims_union.inpatient_services where ", code_list,
        " union select distinct dx3 as dx from truven_commercial_claims_union.inpatient_services where ", code_list,
        " union select distinct dx4 as dx from truven_commercial_claims_union.inpatient_services where ", code_list,
        ") t1"
        )

    }

}

code_table_generator(code_type='icd', range_start=NULL, range_end=NULL, listed_values=NULL, table_name=NULL, criteria=, category)


create table sandbox_grwi.ac_206_e5_codes stored as parquet as

select distinct dx as code, case when dx is not NULL then 'e5' else NULL end as criteria,
case when dx is not NULL then 'Vesico-ureteral reflux' else NULL end as category  
from 
(
select distinct dx1 as dx from truven_commercial_claims_union.outpatient_services
where dx1 ilike 'n1372%'
or dx1 ilike 'n1373%'
or lower(dx1) in ('n1370', 'n1371') 

union

select distinct dx2 as dx from truven_commercial_claims_union.outpatient_services
where dx2 ilike 'n1372%'
or dx2 ilike 'n1373%'
or lower(dx2) in ('n1370', 'n1371') 

union

select distinct dx3 as dx from truven_commercial_claims_union.outpatient_services
where dx3 ilike 'n1372%'
or dx3 ilike 'n1373%'
or lower(dx3) in ('n1370', 'n1371') 

union 

select distinct dx4 as dx from truven_commercial_claims_union.outpatient_services
where dx4 ilike 'n1372%'
or dx4 ilike 'n1373%'
or lower(dx4) in ('n1370', 'n1371') 

union

select distinct dx1 as dx from truven_commercial_claims_union.inpatient_services
where dx1 ilike 'n1372%'
or dx1 ilike 'n1373%'
or lower(dx1) in ('n1370', 'n1371') 

union

select distinct dx2 as dx from truven_commercial_claims_union.inpatient_services
where dx2 ilike 'n1372%'
or dx2 ilike 'n1373%'
or lower(dx2) in ('n1370', 'n1371') 

union

select distinct dx3 as dx from truven_commercial_claims_union.inpatient_services
where dx3 ilike 'n1372%'
or dx3 ilike 'n1373%'
or lower(dx3) in ('n1370', 'n1371') 

union 

select distinct dx4 as dx from truven_commercial_claims_union.inpatient_services
where dx4 ilike 'n1372%'
or dx4 ilike 'n1373%'
or lower(dx4) in ('n1370', 'n1371') 
) t1