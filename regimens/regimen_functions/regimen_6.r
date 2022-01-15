regimens_6 <- function(step5_table_name, step6_table_name) {

    library(data.table)
    library(RODBC)
    library(dplyr)
    library(DT)
    library(DBI)
    library(redshiftTools)
    

        query <- paste0("select t5.id, t5.regimen, 
      cast((floor(t5.days_into_regimen/t5.cycle_length)+1) as int) as cycle1, 
      rank() over(partition by id, regimen order by date) as cycle2,
      t5.drug_cocktail, t5.code, t5.cycle_length, 
      datediff('day', treatment_start, date) as days_into_treatment,
      t5.days_into_regimen, t5.date, t5.regimen_start, t5.regimen_end, t5.treatment_start  
            from
            (select t4.id, treatment_start, date, days_into_treatment, 
            dense_rank() over(partition by id order by regimen_start) as regimen, 
            drug_cocktail, regimen_start, regimen_end, cast(t4.cycle_length as int) as cycle_length, 
            datediff('day', regimen_start, date) as days_into_regimen, code from
            (select *, min(date) over (partition by id, drug_cocktail) as regimen_start,
            max(date) over (partition by id, drug_cocktail) as regimen_end
            from
            (select * from ", step5_table_name,   ") t3
            order by id, treatment_start, date) t4) t5")


    regimen <- sqlQuery(rs_conn, query)
    df <- data.frame(regimen)
    sqlQuery(rs_conn, query=paste0('drop table if exists ', step6_table_name))
    x=strsplit(step6_table_name, ".", fixed = TRUE)
    ld_tbl(df, x[[1]][1], x[[1]][2])


    return(df)
}
