regimens_4 <- function(step3_table_name, step3a_table_name, step4_table_name) {
  
    library(data.table)
    library(RODBC)
    library(dplyr)
    library(DT)
    library(DBI)
    library(redshiftTools)

        query <- paste0("select t3.*, max(t3.cycle_length) over (partition by t3.id, t3.reg1) as max_interval
            from
            (
            select t1.*, t2.drug_cocktail, datediff('day', t1.date, t1.next_administration) as cycle_length 
            from ", step3_table_name, " t1
            left outer join (
            select * from ", step3a_table_name,") t2 on t1.id = t2.id and t1.days_into_treatment = t2.days_into_treatment and t1.proc_rank = t2.proc_rank) t3 
            order by id, treatment_start, date")

    regimen <- sqlQuery(rs_conn, query)
    df <- data.frame(regimen)
    sqlQuery(rs_conn, query=paste0('drop table if exists ', step4_table_name))
    x=strsplit(step4_table_name, ".", fixed = TRUE)
    ld_tbl(df, x[[1]][1], x[[1]][2])


    return(df)
}