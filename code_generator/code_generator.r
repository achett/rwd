code_generator <- function(conn_string, code_list, criteria_id)
{
    i=1
    for (i in 1:length(code_list))
    {
            x=code_list[i]
            n=1
            last_char=substr(x, nchar(x)-n+1, nchar(x))
        if (last_char=='%')
        {
            query=paste0("select dx1 as code from truven_commercial_claims_union.outpatient_services 
                where (dx1 ilike '", x,"') union 
                select dx2 as code from truven_commercial_claims_union.outpatient_services 
                where (dx2 ilike '", x,"') union 
                select dx3 as code from truven_commercial_claims_union.outpatient_services 
                where (dx3 ilike '", x,"') union 
                select dx4 as code from truven_commercial_claims_union.outpatient_services 
                where (dx4 ilike '", x,"') union 
                select dx1 as code from truven_commercial_claims_union.inpatient_services 
                where (dx1 ilike '", x,"') union 
                select dx2 as code from truven_commercial_claims_union.inpatient_services 
                where (dx2 ilike '", x,"') union 
                select dx3 as code from truven_commercial_claims_union.inpatient_services 
                where (dx3 ilike '", x,"') union 
                select dx4 as code from truven_commercial_claims_union.inpatient_services 
                where (dx4 ilike '", x,"') union 
                
                select dx1 as code from truven_medicare_claims_union.outpatient_services 
                where (dx1 ilike '", x,"') union 
                select dx2 as code from truven_medicare_claims_union.outpatient_services 
                where (dx2 ilike '", x,"') union 
                select dx3 as code from truven_medicare_claims_union.outpatient_services 
                where (dx3 ilike '", x,"') union 
                select dx4 as code from truven_medicare_claims_union.outpatient_services 
                where (dx4 ilike '", x,"') union 
                select dx1 as code from truven_medicare_claims_union.inpatient_services 
                where (dx1 ilike '", x,"') union 
                select dx2 as code from truven_medicare_claims_union.inpatient_services 
                where (dx2 ilike '", x,"') union 
                select dx3 as code from truven_medicare_claims_union.inpatient_services 
                where (dx3 ilike '", x,"') union 
                select dx4 as code from truven_medicare_claims_union.inpatient_services 
                where (dx4 ilike '", x,"')")

                codes=sqlQuery(conn_string, query)
                codes <- data.frame(codes[!duplicated(codes$code),])
                colnames(codes)=c("code")
                codes$code=as.character(codes$code)
                if (i==1)
                {
                    codes_final=codes
                }
                else
                {
                    codes_final=rbind(codes_final, codes)
                }
        }
    else
    {
        x=data.frame(x)
        colnames(x)=c("code")
                if (i==1)
                {
                    codes_final=x
                }   
                else
                {
                    codes_final=rbind(codes_final, x)
                }
    } 
    }
    codes_final$criteria=criteria_id
    return(codes_final)
}


q=code_generator(rs_conn, c('f40%'), 'anxiety')