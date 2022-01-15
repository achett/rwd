regimens_2 <- function(claims_table_name, step2_table_name) {

    library(data.table)
    library(RODBC)
    library(dplyr)
    library(DT)
    library(DBI)
    library(redshiftTools)
    
            rs_conn<-odbcDriverConnect(RedshiftConnect()) 
        con <- dbConnect(odbc::odbc(), .connection_string = RedshiftConnect())

        query <- paste0("select id, treatment_start, date, reg1, daysdiff, 
            max(daily_procs) over (partition by id, reg1) as max_procs, daily_procs, code from(
            select id, treatment_start, date, datediff('day', treatment_start, date) as daysdiff, 
            case when datediff('day', treatment_start, date) <= 28 then 1 else 0 end as reg1,
            daily_procs, code from ", claims_table_name,  " t2
            order by id, treatment_start, date) t3
            order by id, treatment_start, date, reg1")

    regimen_2 <- sqlQuery(rs_conn, query)
    df <- data.frame(regimen_2)
    sqlQuery(rs_conn, query=paste0('drop table if exists ', step2_table_name))
    x=strsplit(step2_table_name, ".", fixed = TRUE)
    ld_tbl(df, x[[1]][1], x[[1]][2])


    return(df)
}