clean_period <- function(schema, patient_id_table, clean_period_start, clean_period_end, code_table, code_type, code_criteria)

{
        clean_period_start = as.character(as.Date(clean_period_start))
        clean_period_end = as.character(as.Date(clean_period_end))

        criteria_ref_date_start = paste0("and CAST(date AS timestamp) >= CAST('", clean_period_start, "' AS timestamp)")
        criteria_ref_date_end = paste0("and CAST(date AS timestamp) >= CAST('", clean_period_end, "' AS timestamp)")

    query <- paste0("select enrolid as id,
            cast(substring(t1.svcdate,7,4)||'-'||substring(t1.svcdate,1,2)||'-'||substring(t1.svcdate,4,2) as timestamp) AS date,
            from truven_commercial_claims_union.outpatient_sevices
            where id in (select id from ", schema, ".", patient_id_index_table, ")
            and (lower(code) in (select lower(code) from ", schema, ".", code_table, " where criteria='", code_criteria,"'))", 
            " ", criteria_ref_date_start,
            " ", criteria_ref_date_end) 

}
