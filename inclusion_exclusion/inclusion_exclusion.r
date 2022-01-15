inclusion_exclusion <- function(data_source, schema, schema2, criteria_type, code_type, code_table, primary, code_criteria, 
patient_id_index_table, trial_start, criteria_ref_date, period_type_start, period_length_start, period_type_end, period_length_end, output_type, output_tb_nm)

#inclusion_exclusion(data_source='truven', schema='commercial', schema2=NULL, criteria_type='normal', code_type='icd', code_table='sandbox_truven.ac_gc_codes', code_criteria='primary_alt', patient_id_index_table='ac_gc_cohort4',
#trial_start='2017-03-01', criteria_ref_date="trial start", period_type_start="months", period_length_start=-12, period_type_end="months", period_length_end=0, output_type='df', output_tb_nm='sandbox_truven.xx')



{

if(tolower(data_source)=='truven') {
    if(tolower(schema)=='commercial') {
        schema = 'truven_commercial_claims_union'
    }
    else if (tolower(schema)=='medicare') { 
        schema = 'truven_medicare_claims_union'
    }

    else if (tolower(schema)=='medicaid') { 
        schema = 'truven_medicaid_claims_union'

    }

    if(tolower(criteria_type)=='normal') {
        criteria_type = 'then 1 else 0'
    }

    else if (tolower(criteria_type)=='reverse') { 
        criteria_type = 'then 0 else 1'
    }

    if(criteria_ref_date=='index date') {
        if(period_type_start=='months') {
            if(period_length_start == 'ever') {
                criteria_ref_date_start = paste0("")
                period_length_start = paste0("")
            }
            else { 
                criteria_ref_date_start = paste0("and cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) >= add_months(cast(substring(t2.index_date,7,4)||'-'||substring(t2.index_date,1,2)||'-'||substring(t2.index_date,4,2) as timestamp), ", period_length_start, ") ")
            }
            
        }
        else if(period_type_start=='days') {
            if(period_length_start == 'ever') {
                criteria_ref_date_start = paste0("")
                period_length_start = paste0("")
            }
            else {
            criteria_ref_date_start = paste0("and cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) >= adddate(cast(substring(t2.index_date,7,4)||'-'||substring(t2.index_date,1,2)||'-'||substring(t2.index_date,4,2) as timestamp), ", period_length_start, ") ")
            }
        }

        if(period_type_end=='months') {
            if(period_length_end == 'ever') {
                criteria_ref_date_end = paste0("")
                period_length_end = paste0("")
            }
            else {
                criteria_ref_date_end = paste0("and cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) <= add_months(cast(substring(t2.index_date,7,4)||'-'||substring(t2.index_date,1,2)||'-'||substring(t2.index_date,4,2) as timestamp), ", period_length_end, ") ")
            }
            
        }
        else if(period_type_end=='days') {
               if(period_length_end == 'ever') {
                criteria_ref_date_end = paste0("")
                period_length_end = paste0("")
            }
            else {
                criteria_ref_date_end = paste0("and cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) <= adddate(cast(substring(t2.index_date,7,4)||'-'||substring(t2.index_date,1,2)||'-'||substring(t2.index_date,4,2) as timestamp), ", period_length_end, ") ")
            }
        }
    }

    else if (criteria_ref_date=='trial start') {
        trial_start = as.character(as.Date(trial_start))
        if(period_type_start=='months') {
            if(period_length_start == 'ever') {
                criteria_ref_date_start = paste0("")
                period_length_start = paste0("")
            }
            else {
                criteria_ref_date_start = paste0("and cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) >= add_months(CAST('", trial_start, "' AS timestamp), ", period_length_start, ") ")
            }
            
        }
        else if(period_type_start=='days') {
            if(period_length_start == 'ever') {
                criteria_ref_date_start = paste0("")
                period_length_start = paste0("")
            }
            else {
            criteria_ref_date_start = paste0("and cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) >= adddate(CAST('", trial_start, "' AS timestamp), ", period_length_start, ") ")
            }
        }

        if(period_type_end=='months') {
            if(period_length_end == 'ever') {
                criteria_ref_date_end = paste0("")
                period_length_end = paste0("")
            }
            else {
                criteria_ref_date_end = paste0("and cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) <= add_months(CAST('", trial_start, "' AS timestamp), ", period_length_end, ") ")
            }
            
        }
        else if(period_type_end=='days') {
               if(period_length_end == 'ever') {
                criteria_ref_date_end = paste0("")
                period_length_end = paste0("")
            }
            else {
                criteria_ref_date_end = paste0("and cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) <= adddate(CAST('", trial_start, "' AS timestamp), ", period_length_end, ") ")
            }
        }
    }

    if(primary==TRUE) { 
        cohort_base=paste0("where")
    }
    else {
        cohort_base = paste0("where t1.enrolid in (select id from ", patient_id_index_table, " ) and ")
    }

    if(patient_id_index_table=='primary') {
          cohort_base_merge =  paste0(" ")
          index_date_string=paste0(" ")
        lead_string = paste0("select distinct enrolid, index_date, case when enrolid is not null ", criteria_type, " end as criteria from 
            (select enrolid, min(svcdate) as index_date from ( ")
        end_string = paste0(" group by enrolid) t4 ")
    }
    else {
        cohort_base_merge =  paste0("left outer join ", patient_id_index_table, " t2 on t1.enrolid = t2.id ")
        index_date_string=paste0(", cast(substring(t2.index_date,7,4)||'-'||substring(t2.index_date,1,2)||'-'||substring(t2.index_date,4,2) as timestamp) AS index_date")
        lead_string = paste0("select distinct enrolid, case when enrolid is not null ", criteria_type, " end as criteria from ( ")
        end_string = paste0(" ")
    }

    if(output_type=='count')
        {
         lead_string = paste0('select count ( distinct enrolid) from ( ' ,lead_string)   
         end_string = paste0(end_string, ' )')
        }
    if(output_type=='redshift table')
        {
         lead_string = paste0('drop table if exists ', output_tb_nm, '; create table ', output_tb_nm, ' as ' , lead_string)   
        }

    if(tolower(code_type)=='icd') {
        code_type = 'code'
        code_type1 = 'dx1'
        code_type2 = 'dx2'
        code_type3 = 'dx3'
        code_type4 = 'dx4'

         query <- paste0(lead_string, "
            select t1.enrolid,
            cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS svcdate
             ", index_date_string, " 
            from ", schema, ".outpatient_services t1
             ", cohort_base_merge, "  
             ", cohort_base, "
             (lower(t1.", code_type1, ") in (select lower(", code_type, ") from ", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end, 
            " ", criteria_ref_date_start, 
            " union all
            select t1.enrolid,
            cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS svcdate
             ", index_date_string, " 
            from ", schema, ".outpatient_services t1
             ", cohort_base_merge, "  
             ", cohort_base, "
             (lower(t1.", code_type2, ") in (select lower(", code_type, ") from ", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end, 
            " ", criteria_ref_date_start,  
            " union all
            select t1.enrolid,
            cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS svcdate
             ", index_date_string, " 
            from ", schema, ".outpatient_services t1
             ", cohort_base_merge, "  
             ", cohort_base, "
             (lower(t1.", code_type3, ") in (select lower(", code_type, ") from ", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end,  
            " ", criteria_ref_date_start,  
            " union all
            select t1.enrolid,
            cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS svcdate
             ", index_date_string, " 
            from ", schema, ".outpatient_services t1
             ", cohort_base_merge, "  
             ", cohort_base, "
             (lower(t1.", code_type4, ") in (select lower(", code_type, ") from ", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end,  
            " ", criteria_ref_date_start, 
            " union all

            select t1.enrolid,
            cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS svcdate
             ", index_date_string, " 
            from ", schema, ".inpatient_services t1
             ", cohort_base_merge, "  
             ", cohort_base, "
             (lower(t1.", code_type1, ") in (select lower(", code_type, ") from ", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end,  
            " ", criteria_ref_date_start, 
            " union all
            select t1.enrolid,
            cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS svcdate
             ", index_date_string, " 
            from ", schema, ".inpatient_services t1
             ", cohort_base_merge, "  
             ", cohort_base, "
             (lower(t1.", code_type2, ") in (select lower(", code_type, ") from ", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end,  
            " ", criteria_ref_date_start, 
            " union all
            select t1.enrolid,
            cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS svcdate
             ", index_date_string, " 
            from ", schema, ".inpatient_services t1
             ", cohort_base_merge, "  
             ", cohort_base, "
             (lower(t1.", code_type3, ") in (select lower(", code_type, ") from ", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end, 
            " ", criteria_ref_date_start, 
            " union all
            select t1.enrolid,
            cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS svcdate
             ", index_date_string, " 
            from ", schema, ".inpatient_services t1
             ", cohort_base_merge, "  
             ", cohort_base, "
             (lower(t1.", code_type4, ") in (select lower(", code_type, ") from ", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end, 
            " ", criteria_ref_date_start,   " ) t3", end_string)
    }

    else if (tolower(code_type)=='cpt') { 
        code_type = 'proc1'

        query <- paste0(lead_string, "
 select t1.enrolid,
 cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS svcdate
  ", index_date_string, " 
 from ", schema, ".outpatient_services t1
  ", cohort_base_merge, "  
 ", cohort_base, "
 (lower(t1.", code_type, ") in (select lower(code) from ", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end,  
" ", criteria_ref_date_start, 
" union all
select t1.enrolid,
cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS svcdate
 ", index_date_string, " 
from ", schema, ".inpatient_services t1
 ", cohort_base_merge, "  
 ", cohort_base, "
 (lower(t1.", code_type, ") in (select lower(code) from ", code_table, ")) ", criteria_ref_date_end,  
" ", criteria_ref_date_start,  " ) t3", end_string)
    }

    else if (tolower(code_type)=='ndc') { 
        code_type = 'ndcnum'

        query <- paste0(lead_string, 
        "select t1.enrolid,
cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS svcdate
 ", index_date_string, " 
from ", schema, ".outpatient_pharmaceutical_claims t1
 ", cohort_base_merge, "  
 ", cohort_base, "
 (lower(t1.", code_type, ") in (select lower(code) from ", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end,  
" ", criteria_ref_date_start, " ) t3", end_string)
    }

           
            }
  

    else {query <- paste0("No data source selected")}

    return(query)

}

#inclusion_exclusion("truven", "commercial", "exclusion", "icd", "ac_gc_e1", "ac_gc_step1_index", '2017-03-01', "trial start", "months", -12, "months", -6)






