regimens_5 <- function(step4_table_name, step5_table_name) {

    library(data.table)
    library(RODBC)
    library(dplyr)
    library(DT)
    library(DBI)
    library(redshiftTools)
     

        query <- paste0("select t3.*, (floor(t3.days_into_treatment/t3.cycle_length)+1) as cycle
            from 
            (select t2.id, t2.treatment_start, t2.date, cast(t2.days_into_treatment as int) as days_into_treatment,  
            cast(t2.cycle_length_final as int) as cycle_length,  
            t2.reg1 as reg, t2.reg1_start as reg_start, t2.reg1_end as reg_end, t2.proc_rank, 
            t2.next_administration, t2.max_procs, t2.daily_procs, 
            t2.code, t2.drug_cocktail
            from ( select t1.*, 
            case when max_interval in ('21','22','23', '20', '19') then '21' else '28' end as cycle_length_final  
            from ", step4_table_name, " t1) t2) t3")

    regimen <- sqlQuery(rs_conn, query)
    df <- data.frame(regimen)
    sqlQuery(rs_conn, query=paste0('drop table if exists ', step5_table_name))
    x=strsplit(step5_table_name, ".", fixed = TRUE)
    ld_tbl(df, x[[1]][1], x[[1]][2])


    return(df)
}
