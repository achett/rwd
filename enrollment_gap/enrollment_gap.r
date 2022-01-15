enrollment_gap <- function(current_cohort, start_date, end_date) {

        query <- paste0("
            select enrolid, case when datediff(day, dtend, '", end_date, "')+gap_sum=0 then 1 else 0 end as cont_enrol, 
            datediff(day, dtend, '", end_date, "'), gap_sum, dtstart, dtend    
            from (
            select enrolid, min(dtstart) as dtstart, max(dtend) as dtend, sum(gap) as gap_sum
            from
            (select enrolid, dtstart, dtend, datediff(day, dtstart, dtend)-cast(memdays as int)+1 as gap 
            from (
            select enrolid,
            cast(substring(dtstart,7,4)||'-'||substring(dtstart,1,2)||'-'||substring(dtstart,4,2) as timestamp) as dtstart, 
            cast(substring(dtend,7,4)||'-'||substring(dtend,1,2)||'-'||substring(dtend,4,2) as timestamp) as dtend, memdays     
            from truven_commercial_claims_union.detail_enrollment
            where enrolid in (select id from ", current_cohort, ")
            and cast(substring(dtstart,7,4)||'-'||substring(dtstart,1,2)||'-'||substring(dtstart,4,2) as timestamp) >='", start_date, 
            "' and cast(substring(dtend,7,4)||'-'||substring(dtend,1,2)||'-'||substring(dtend,4,2) as timestamp) <='", end_date, "
            ') t1
            union
            select enrolid, dtstart, dtend, datediff(day, dtstart, dtend)-cast(memdays as int)+1 as gap 
            from (
            select enrolid,
            cast(substring(dtstart,7,4)||'-'||substring(dtstart,1,2)||'-'||substring(dtstart,4,2) as timestamp) as dtstart, 
            cast(substring(dtend,7,4)||'-'||substring(dtend,1,2)||'-'||substring(dtend,4,2) as timestamp) as dtend, memdays     
            from truven_medicare_claims_union.detail_enrollment
            where enrolid in (select id from ", current_cohort, ")
            and cast(substring(dtstart,7,4)||'-'||substring(dtstart,1,2)||'-'||substring(dtstart,4,2) as timestamp) >='", start_date, 
            "' and cast(substring(dtend,7,4)||'-'||substring(dtend,1,2)||'-'||substring(dtend,4,2) as timestamp) <='", end_date, "
            ') t2
            ) t3 group by enrolid) t4")


    return(query)
}

#enrollment_gap("Commercial", "outpatient_services", "ac_chemo_anemia_breast_pts", 2016, 2017, "ac_enrollment_table3")
enrollment_gap(current_cohort='sandbox_truven.ac_gc_cohort4', start_date='2016-03-01', end_date='2018-09-01')


