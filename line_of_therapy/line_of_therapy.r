# Input: table with four columns: c("id", "date", "code", "days_of_supply")
# Output: table with nine columns: 'id', 'line_combo', 'date', 'code', 'days_of_supply', 'end_date', 'first_date', 'last_date', 'line', 'next_line_start_date', 'prev_line_end_date', 'combo', 'drug_cocktail')
# Specifications: Identify patients with specific groupings of medications of treatments. The first treatment on day 1 will be line 1, then
# after that treatment is line 2. If a line of therapy overlaps with another for >=15 days then that shit's a combo baby!

# Arguments:
# df described above
# time_param1 = overlap days to be considered a combo

# Include Later
# If a regimen has a gap >= 180 days then restart cycle as type of patient = R (returning)
# If a regimen has a gap >= 365 days then restart cycle as type of patient  = N (New)

#Examples: 
#Table: 'sandbox_truven.ac_Pca_compliance_claims2 ' for example below was built in was built in /rwi/users/achettiath/macros/line_of_therapy/table_build1.SQL
#Table: 'sandbox_truven.ac_Pca_compliance_claims2_names ' for example below was built in was built in /rwi/users/achettiath/macros/line_of_therapy/table_build2.SQL
#Interesting patients are id=='266662301' and id=='32767505' 

#Example 1: Drug codes are used 
#library(DBI)
#library(redshiftTools)
#library(RODBC)

#rs_conn<-odbcDriverConnect(RedshiftConnect())
#df=sqlQuery(rs_conn,query=paste0('select * from sandbox_truven.ac_Pca_compliance_claims2 limit 10000'))

#x=line_of_therapy(df, 15)
#x=x[id=='266662301']

#Example 2: Drug names are used 
# library(DBI)
# library(redshiftTools)
# library(RODBC)

# rs_conn<-odbcDriverConnect(RedshiftConnect())
# df=sqlQuery(rs_conn,query=paste0('select * from sandbox_truven.ac_Pca_compliance_claims_names2'))

# x=line_of_therapy(df, 15)
# x=data.table(x)
# x=x[id=='266662301']


line_of_therapy <- function(df, time_param1)
{

library(data.table)
library(dplyr)

#format date and order by id and date
df$date=as.Date(df$date, '%Y-%m-%d')
df <- df[order(df$id, as.Date(df$date,'%Y-%m-%d')),]

#Add days of supply to date to get end date
df$end_date=df$date+as.numeric(df$days_of_supply)

# identify line by change in code by each id
df=data.table(df)
df=df[, line:= rleid(code), by = .(id)]

#find potential line start and end dates
df=df %>% group_by(id, line) %>% mutate(first_date = min(date))
df=df %>% group_by(id, line) %>% mutate(last_date = max(end_date))

#find time overlap of regimens - next
df=df %>% group_by(id) %>% mutate(next_line_start_date = lead(first_date))
df=df %>% group_by(id, code, line) %>% mutate(next_line_start_date = max(next_line_start_date))
df$overlap_days=df$end_date-df$next_line_start_date

#find time overlap of regimens - previous
df=df %>% group_by(id) %>% mutate(prev_line_end_date = lag(last_date))
df=df %>% group_by(id, code, line) %>% mutate(prev_line_end_date = min(prev_line_end_date))
df$overlap_days2=df$prev_line_end_date-df$date

#if overlap days are grater than time_param1, then it is eligible for identification as a combo, 
#so update overlap_days to 0 so you mark claim as rabble-rouser
#non-elgible claims are marked as original line
df$overlap_days=ifelse(df$overlap_days>=time_param1, 0, df$line)
df$overlap_days2=ifelse(df$overlap_days2>=time_param1, 0, df$line)

#update overlap days of first and last lines to the line if it is NA
df$overlap_days=ifelse(is.na(df$next_line_start_date), df$line, df$overlap_days)
df$overlap_days2=ifelse(is.na(df$prev_line_end_date), df$line, df$overlap_days2)

df$line2=ifelse(df$overlap_days==df$overlap_days2, df$line, 0)

# Update line start and end dates
df=df %>% group_by(id, line2) %>% mutate(first_date = min(date))
df=df %>% group_by(id, line2) %>% mutate(last_date = max(end_date))

# Update lines
df=data.table(df)
df=df[, line_combo:= rleid(line2), by = .(id)]

# Update previous and next line start dates
df=df %>% group_by(id) %>% mutate(next_line_start_date = lead(first_date))
df=df %>% group_by(id, line2) %>% mutate(next_line_start_date = max(next_line_start_date))
df=df %>% group_by(id) %>% mutate(prev_line_end_date = lag(last_date))
df=df %>% group_by(id, line2) %>% mutate(prev_line_end_date = min(prev_line_end_date))

# Flag combos
df$line2=ifelse(df$line2==0, 1, 0)

# Summarize drug cocktails
df2 <- df %>% group_by(id, line_combo) %>% summarise(drug_cocktail = toString(sort(unique(code))))
df2$drug_cocktail = gsub(",", " |", df2$drug_cocktail)

# Merge drug cocktails back into original dataframe
df3=merge(df, df2, all=TRUE, by=c('id', 'line_combo'))

# Make final dataframe
final=df3[, c('id', 'line_combo', 'date', 'code', 'days_of_supply', 'end_date', 'first_date', 'last_date', 'line', 'next_line_start_date', 'prev_line_end_date', 'line2', 'drug_cocktail')]
colnames(final)=c('id', 'line', 'date', 'code', 'days_of_supply', 'end_date', 'first_date', 'last_date', 'line_initial', 'next_line_start_date', 'prev_line_end_date', 'combo', 'drug_cocktail')
return(final)

}




