regimens <- function (regimen_build_table_name)  {

        library(R.utils)
        sd <- sourceDirectory("/rwi/users/achettiath/macros/regimens/regimen_functions")
        sapply(sd, source)

        rs_conn<-odbcDriverConnect(RedshiftConnect()) 
        con <- dbConnect(odbc::odbc(), .connection_string = RedshiftConnect())

        step2_table_name = paste0(regimen_build_table_name, "_step2")
        step3_table_name = paste0(regimen_build_table_name, "_step3")
        step3a_table_name = paste0(regimen_build_table_name, "_step3a")
        step4_table_name = paste0(regimen_build_table_name, "_step4")
        step5_table_name = paste0(regimen_build_table_name, "_step5")
        step6_table_name = paste0(regimen_build_table_name, "_step6")

        regimens_2(regimen_build_table_name, step2_table_name) 
        print(paste0("Step 2 done! Check out results in: ", step2_table_name))
        regimens_3(step2_table_name, step3_table_name)
        print(paste0("Step 3 done! Check out results in: ", step3_table_name))
        regimens_3a(step3_table_name, step3a_table_name)
        print(paste0("Step 3a done! Check out results in: ", step3a_table_name))
        regimens_4(step3_table_name, step3a_table_name, step4_table_name) 
        print(paste0("Step 4 done! Check out results in: ", step4_table_name))
        regimens_5(step4_table_name, step5_table_name) 
        print(paste0("Step 5 done! Check out results in: ", step5_table_name))
        regimens_6(step5_table_name, step6_table_name) 
        print(paste0("All Steps done! Check out results in: ", step6_table_name))
}
