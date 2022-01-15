regimens_3a <- function(step3_table_name, step3a_table_name) {
 
    library(data.table)
    library(RODBC)
    library(dplyr)
    library(DT)
    library(DBI)
    library(redshiftTools)
    


        query <- paste0("select * from ", step3_table_name, 
            " order by id, treatment_start, date, code")

            regimen_step3a <- sqlQuery(rs_conn, query)
            df <- data.frame(regimen_step3a)

            df <- df %>% group_by(id, days_into_treatment, proc_rank) %>% summarise(drug_cocktail = toString(sort(unique(code))))
            df$drug_cocktail = gsub(",", " |", df$drug_cocktail)


    sqlQuery(rs_conn, query=paste0('drop table if exists ', step3a_table_name))
    x=strsplit(step3a_table_name, ".", fixed = TRUE)
    ld_tbl(df, x[[1]][1], x[[1]][2])
    return(df)
}


