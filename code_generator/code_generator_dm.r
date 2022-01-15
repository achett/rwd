code_generator_dm <- function(conn_string, code_table, code_list, code_criteria_col, schema, claims_table)
{
    i=1
    for (i in 1:length(code_list))
    {
            criteria=code_table[[code_criteria_col]][[i]]
            x=code_list[i]
            n=1
            last_char=substr(x, nchar(x)-n+1, nchar(x))
            print(last_char)
        if (last_char=='%')
        {
            query=paste0("select code from  ", schema, ".", claims_table,
                " where (code ilike '", x,"')")

                print(query)

                codes=sqlQuery(conn_string, query)
                print(NROW(codes))
                codes <- data.frame(codes[!duplicated(codes$code),])
                colnames(codes)=c("code")
                codes$code=as.character(codes$code)
                if (i==1)
                {
                    if (NROW(codes)>0)
                    {
                    codes_final=codes
                    codes_final$criteria=criteria
                    }
                }
                else
                {
                    if (NROW(codes)>0)
                    {
                    codes$criteria=criteria 
                    codes_final=rbind(codes_final, codes)
                    }
                }
        }
    else
    {
        x=data.frame(x)
        colnames(x)=c("code")
                if (i==1)
                {
                    codes_final=x
                    codes_final$criteria=criteria
                }   
                else
                {
                    x$criteria=criteria 
                    codes_final=rbind(codes_final, x)
                }
    }
    }
    return(codes_final)
}


#q=code_generator_dm(rs_conn, codes[which(codes$criteria=='Anxiety'),], codes[which(codes$criteria=='Anxiety'),]$code, 'criteria', schema="sandbox_truven", claims_table='ac_meno_burden_claims_full')
