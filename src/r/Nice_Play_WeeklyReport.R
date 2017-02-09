library(RMySQL)
#########
Nb_week = 2

#########

host_systexdb = 'Systexdb.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com'


conn <- dbConnect(MySQL(), user = 'debut',dbname = 'pbplus_tw', password = 'tp6m4xup6',
                  host = host_systexdb) 

sum_table <- dbGetQuery(conn, "SELECT * FROM cpbl_pollsummary") 
sum_table <- sum_table[sum_table$event=='niceplay',c("id","count")]
data = dbGetQuery(conn, "SELECT * FROM cpbl_pollcount") 


data.df <- data.frame(table(data[data$event=='niceplay',"v1"]))

n_voters <- length(unique(data[data$event=='niceplay' & (data$v1 >= 17+5*(Nb_week-1)) &
                                    (data$v1 <= 21+5*(Nb_week-1)),"phone_number"]))


## numbers of registor to vote nice play

conn1 <- dbConnect(MySQL(), user = 'pbplus',dbname = 'pbplus', password = 'pbplus',
host = host_systexdb)

sql <- paste0('SELECT p_mobilenumber,createdTS FROM users WHERE p_mobilenumber in', ' (',Inlist,")")

data1 = dbGetQuery(conn1, sql)

new_voters_all <- merge(new_voters,data1,by.x = "phone_number_international",by.y = "p_mobilenumber",all.x = T)

new_voters_all$created_date_new <- substr(new_voters_all$created_date,1,19)
new_voters_all$createdTS_new <- substr(new_voters_all$createdTS,1,19)

new_voters_all$created_date_new <- as.POSIXct(new_voters_all$created_date_new, format="%Y-%m-%d %H:%M:%S")
new_voters_all$createdTS_new <- as.POSIXct(new_voters_all$createdTS_new, format="%Y-%m-%d %H:%M:%S")


voters_within1 <- new_voters_all[new_voters_all$created_date_new - new_voters_all$createdTS_new < 3600,]


voters_within1 <- voters_within1[voters_within1$v1 >= 17+5*(Nb_week-1) &voters_within1$v1 <= 21+5*(Nb_week-1),]

print('Actual Number of votes:')
print(pastdata.df)
print('Non-duplicated voters number:')
print(n_voters)
print(paste0('Numbers of registor to vote nice play in week',Nb_week))
print(voters_within1)