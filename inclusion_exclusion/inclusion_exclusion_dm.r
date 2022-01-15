inclusion_exclusion_dm <- function(data_source, schema, claims_table, criteria_type, code_type, code_table, code_criteria, 
patient_id_index_table, trial_start, criteria_ref_date, period_type_start, period_length_start, period_type_end, period_length_end, 
index_date, claim_counts, claim_range_start, claim_range_end)

{

if(tolower(data_source)=='sandbox_claims') {

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
                criteria_ref_date_start = paste0("and CAST(date AS timestamp) >= add_months(CAST(index_date AS timestamp), ", period_length_start, ") ")
            }
            
        }
        else if(period_type_start=='days') {
            if(period_length_start == 'ever') {
                criteria_ref_date_start = paste0("")
                period_length_start = paste0("")
            }
            else {
            criteria_ref_date_start = paste0("and CAST(date AS timestamp) >= adddate(CAST(index_date AS timestamp), ", period_length_start, ") ")
            }
        }

        if(period_type_end=='months') {
            if(period_length_end == 'ever') {
                criteria_ref_date_end = paste0("")
                period_length_end = paste0("")
            }
            else {
                criteria_ref_date_end = paste0("and CAST(date AS timestamp) <= add_months(CAST(index_date AS timestamp), ", period_length_end, ") ")
            }
            
        }
        else if(period_type_end=='days') {
               if(period_length_end == 'ever') {
                criteria_ref_date_end = paste0("")
                period_length_end = paste0("")
            }
            else {
                criteria_ref_date_end = paste0("and CAST(date AS timestamp) <= adddate(CAST(index_date AS timestamp), ", period_length_end, ") ")
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
                criteria_ref_date_start = paste0("and CAST(date AS timestamp) >= add_months(CAST('", trial_start, "' AS timestamp), ", period_length_start, ") ")
            }
            
        }
        else if(period_type_start=='days') {
            if(period_length_start == 'ever') {
                criteria_ref_date_start = paste0("")
                period_length_start = paste0("")
            }
            else {
            criteria_ref_date_start = paste0("and CAST(date AS timestamp) >= adddate(CAST('", trial_start, "' AS timestamp), ", period_length_start, ") ")
            }
        }

        if(period_type_end=='months') {
            if(period_length_end == 'ever') {
                criteria_ref_date_end = paste0("")
                period_length_end = paste0("")
            }
            else {
                criteria_ref_date_end = paste0("and CAST(date AS timestamp) <= add_months(CAST('", trial_start, "' AS timestamp), ", period_length_end, ") ")
            }
            
        }
        else if(period_type_end=='days') {
               if(period_length_end == 'ever') {
                criteria_ref_date_end = paste0("")
                period_length_end = paste0("")
            }
            else {
                criteria_ref_date_end = paste0("and CAST(date AS timestamp) <= adddate(CAST('", trial_start, "' AS timestamp), ", period_length_end, ") ")
            }
        }
    }

    if(index_date == TRUE) {
                index_date_string = paste0("index_date,")
            }
    else {
                index_date_string = paste0(" ")
            }
    if(is.na(claim_range_end)) {
                claim_range_end = paste0(" ")
            }
    else {
                claim_range_end = paste0("and days_between_claims <=", claim_range_end)
            }

    if(is.na(claim_range_start)) {
                claim_range_start = paste0(" ")
            }
    else {
                claim_range_start = paste0("where days_between_claims <=", claim_range_start)
            }

    if(tolower(code_type)=='icd') {
        code_type1 = 'code'

         query <- paste0("select distinct id,", index_date_string, " case when id is not null ", criteria_type, " end as criteria
            from (
            select *, count(*) over(partition by t2.id) AS claim_counts from 
            (select t1.id,
            CAST(date AS timestamp) AS date,
            min(CAST(date AS timestamp)) over(partition by t1.id) AS index_date,
            lead(date, 1) over (partition by t1.id order by date) as next_date,
            datediff(day, date, next_date) as days_between_claims   
            from ", schema, ".", claims_table, " t1

            left outer join ", schema, ".", patient_id_index_table, " t2 on t1.id = t2.id 
            where t1.id in (select id from ", schema, ".", patient_id_index_table, ")
            and (lower(t1.", code_type1, ") in (select lower(", code_type1, ") from ", schema, ".", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end, 
            " ", criteria_ref_date_start, ") t2 ", claim_range_start, 
            claim_range_end, 
            ") t3 
            where claim_counts>=", claim_counts)
    }

    else if (tolower(code_type)=='icd10_gm4') { 
            code_type1 = 'icd10_4_code'
            code_type2 = 'code'

                 query <- paste0("select distinct patient_no,", index_date_string, " case when patient_no is not null ", criteria_type, " end as criteria
            from  (
            select *, count(*) over(partition by t2.patient_no) AS claim_counts from 
            (select t1.patient_no,
            CAST(date AS timestamp) AS date,
            min(CAST(date AS timestamp)) over(partition by t1.patient_no) AS index_date,
            lead(date, 1) over (partition by t1.patient_no order by date) as next_date,
            datediff(day, date, next_date) as days_between_claims   
            from sandbox_grwi.", claims_table, " t1

            left outer join sandbox_grwi.", patient_id_index_table, " t2 on t1.patient_no = t2.patient_no 
            where t1.patient_no in (select patient_no from sandbox_grwi.", patient_id_index_table, ")
            and (lower(t1.", code_type1, ") in (select lower(", code_type2, ") from sandbox_grwi.", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end, 
            " ", criteria_ref_date_start, ") t2 ", claim_range_start, 
            claim_range_end, 
            ") t3 
            where claim_counts>=", claim_counts)
           
            }

        else if (tolower(code_type)=='icd10_gm3') { 
            code_type1 = 'icd10_3_code'
            code_type2 = 'code'

                 query <- paste0("select distinct patient_no,", index_date_string, " case when patient_no is not null ", criteria_type, " end as criteria
            from  (
            select *, count(*) over(partition by t2.patient_no) AS claim_counts from 
            (select t1.patient_no,
            CAST(date AS timestamp) AS date,
            min(CAST(date AS timestamp)) over(partition by t1.patient_no) AS index_date,
            lead(date, 1) over (partition by t1.patient_no order by date) as next_date,
            datediff(day, date, next_date) as days_between_claims   
            from sandbox_grwi.", claims_table, " t1

            left outer join sandbox_grwi.", patient_id_index_table, " t2 on t1.patient_no = t2.patient_no 
            where t1.patient_no in (select patient_no from sandbox_grwi.", patient_id_index_table, ")
            and (lower(t1.", code_type1, ") in (select lower(", code_type2, ") from sandbox_grwi.", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end, 
            " ", criteria_ref_date_start, ") t2 ", claim_range_start, 
            claim_range_end, 
            ") t3 
            where claim_counts>=", claim_counts)
           
            }

        else if (tolower(code_type)=='icd10_gm2') { 
            code_type1 = 'icd10_2_code'
            code_type2 = 'code'

                 query <- paste0("select distinct patient_no,", index_date_string, " case when patient_no is not null ", criteria_type, " end as criteria
            from  (
            select *, count(*) over(partition by t2.patient_no) AS claim_counts from 
            (select t1.patient_no,
            CAST(date AS timestamp) AS date,
            min(CAST(date AS timestamp)) over(partition by t1.patient_no) AS index_date,
            lead(date, 1) over (partition by t1.patient_no order by date) as next_date,
            datediff(day, date, next_date) as days_between_claims   
            from sandbox_grwi.", claims_table, " t1

            left outer join sandbox_grwi.", patient_id_index_table, " t2 on t1.patient_no = t2.patient_no 
            where t1.patient_no in (select patient_no from sandbox_grwi.", patient_id_index_table, ")
            and (lower(t1.", code_type1, ") in (select lower(", code_type2, ") from sandbox_grwi.", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end, 
            " ", criteria_ref_date_start, ") t2 ", claim_range_start, 
            claim_range_end, 
            ") t3 
            where claim_counts>=", claim_counts)
           
            }

        else if (tolower(code_type)=='icd10_gm1') { 
            code_type1 = 'icd10_1_code'
            code_type2 = 'code'

                 query <- paste0("select distinct patient_no,", index_date_string, " case when patient_no is not null ", criteria_type, " end as criteria
            from (
            select *, count(*) over(partition by t2.patient_no) AS claim_counts from 
            (select t1.patient_no,
            CAST(date AS timestamp) AS date,
            min(CAST(date AS timestamp)) over(partition by t1.patient_no) AS index_date,
            lead(date, 1) over (partition by t1.patient_no order by date) as next_date,
            datediff(day, date, next_date) as days_between_claims   
            from sandbox_grwi.", claims_table, " t1

            left outer join sandbox_grwi.", patient_id_index_table, " t2 on t1.patient_no = t2.patient_no 
            where t1.patient_no in (select patient_no from sandbox_grwi.", patient_id_index_table, ")
            and (lower(t1.", code_type1, ") in (select lower(", code_type2, ") from sandbox_grwi.", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end, 
            " ", criteria_ref_date_start, ") t2 ", claim_range_start, 
            claim_range_end, 
            ") t3 
            where claim_counts>=", claim_counts)
           
            }

    else if (tolower(code_type)=='cpt') { 
        code_type1 = 'code'

        query <- paste0("select distinct id,", index_date_string, " case when id is not null ", criteria_type, " end as criteria
            from  (
            select *, count(*) over(partition by t2.id) AS claim_counts from 
            (select t1.id,
            CAST(date AS timestamp) AS date,
            min(CAST(date AS timestamp)) over(partition by t1.id) AS index_date,
            lead(date, 1) over (partition by t1.id order by date) as next_date,
            datediff(day, date, next_date) as days_between_claims   
            from ", schema, ".", claims_table, " t1

            left outer join ", schema, ".", patient_id_index_table, " t2 on t1.id = t2.id 
            where t1.id in (select id from ", schema, ".", patient_id_index_table, ")
            and (lower(t1.", code_type1, ") in (select lower(", code_type1, ") from ", schema, ".", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end, 
            " ", criteria_ref_date_start, ") t2 ", claim_range_start, 
            claim_range_end, 
            ") t3 
            where claim_counts>=", claim_counts)
    }

    else if (tolower(code_type)=='ndc') { 
        code_type1 = 'code'

        query <- paste0("select distinct id,", index_date_string, " case when id is not null ", criteria_type, " end as criteria
            from  (
            select *, count(*) over(partition by t2.id) AS claim_counts from 
            (select t1.id,
            CAST(date AS timestamp) AS date,
            min(CAST(date AS timestamp)) over(partition by t1.id) AS index_date,
            lead(date, 1) over (partition by t1.id order by date) as next_date,
            datediff(day, date, next_date) as days_between_claims   
            from ", schema, ".", claims_table, " t1

            left outer join ", schema, ".", patient_id_index_table, " t2 on t1.id = t2.id 
            where t1.id in (select id from ", schema, ".", patient_id_index_table, ")
            and (lower(t1.", code_type1, ") in (select lower(", code_type1, ") from ", schema, ".", code_table, " where criteria='", code_criteria,"')) ", criteria_ref_date_end, 
            " ", criteria_ref_date_start, ") t2 ", claim_range_start, 
            claim_range_end, 
            ") t3 
            where claim_counts>=", claim_counts)
    }
  

    else {query <- paste0("No data source selected")}

    return(query)

}

}