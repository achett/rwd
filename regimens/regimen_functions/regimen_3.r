regimens_3 <- function(step2_table_name, step3_table_name) {

    library(data.table)
    library(RODBC)
    library(dplyr)
    library(DT)
    library(DBI)
    library(redshiftTools)
    

        query <- paste0("select id, treatment_start, date, datediff('day', treatment_start, date) as days_into_treatment, reg1,
            min(reg1_start_options) over (partition by id, reg1) as reg1_start,
            max(reg1_end_options) over (partition by id, reg1) as reg1_end,
            dense_rank() over(partition by id, code order by date) as proc_rank,
            lead(date, 1) over (partition by id, code order by date) as next_administration,
            max_procs, daily_procs, code from(   
            select id, treatment_start, date, reg1,
            case when reg1 = '1' and daily_procs = max_procs then date else NULL end as reg1_start_options,
            case when reg1 = '1' then date else NULL end as reg1_end_options,
            max_procs, daily_procs, code from( select * from ", step2_table_name, 
            " ) t4
            order by id, treatment_start, date, reg1) t5
            order by id, treatment_start, date, reg1")

    regimen <- sqlQuery(rs_conn, query)
    df <- data.frame(regimen)
    sqlQuery(rs_conn, query=paste0('drop table if exists ', step3_table_name))
    x=strsplit(step3_table_name, ".", fixed = TRUE)
    ld_tbl(df, x[[1]][1], x[[1]][2])


    return(df)
}