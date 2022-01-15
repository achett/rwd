regimens_build <- function(code_table, criteria, claims_table) {

    library(data.table)
    library(RODBC)
    library(dplyr)
    library(DT)
    library(DBI)
    library(redshiftTools)
    
        rs_conn<-odbcDriverConnect(RedshiftConnect()) 
      
        query <- paste0("select id, min(date) over (partition by id) as treatment_start, date, 
            count(id) over (partition by date, id) as daily_procs, code from
            (
            select id, date, code from ", claims_table,
            " where code in (select code from ", code_table, " where criteria='", criteria, "' ) 
            order by id, date) t1 
            order by id, treatment_start, date ")

    regimen <- sqlQuery(rs_conn, query)
    df <- data.frame(regimen)
    return(df)
}

