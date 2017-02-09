library(RMySQL)

#########
Nb_week = 2
Nb_winner = 8
Nb_waitingList = 8

#########

option_codes = seq(17+5*(Nb_week-1),21+5*(Nb_week-1))

host_systexdb = 'Systexdb.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com'


conn <- dbConnect(MySQL(), user = 'debut',dbname = 'pbplus_tw', password = 'tp6m4xup6',
                  host = host_systexdb) 

data = dbGetQuery(conn, "SELECT * FROM cpbl_pollcount")

winners = sample(unique(data[data$event=='niceplay' & data$v1 %in% option_codes,"phone_number"]),Nb_winner + Nb_waitingList,replace = F)

Inlist <- paste0("\'",paste0(paste0('886',substr(winners,2,10)),collapse = "\','"),"\'")

sql <- paste0('SELECT p_name,p_emailaddress,p_mobilenumber FROM users WHERE p_mobilenumber in', ' (',Inlist,")")

conn1 <- dbConnect(MySQL(), user = 'pbplus',dbname = 'pbplus', password = 'pbplus',
                  host = host_systexdb) 

data1 = dbGetQuery(conn1, sql)

data1$award = c(rep('正取',Nb_winner),rep('備取',Nb_waitingList))
write.csv(data1,past0("~/Desktop/pb+/niceplay/Winners_week",Nb_week,".csv"),fileEncoding = 'big5')

