require(RMySQL)
options(scipen=999)


host="dev-rds.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com"
database="bi"
user="root"
password="54883155"

conn = dbConnect(MySQL(), user = user,dbname = database,
                  password = password,
                  host = host)



host="media-pbplus-me.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com"
database="event"
user="media_pbplus_me"
password="pcgbros_54883155"


conn1 = dbConnect(MySQL(), user = user,dbname = database,
                 password = password,
                 host = host)

sql = "select rrp.email, rrp.phone_number from registration_register as rr
    inner join registration_registeritem as rri on rr.id = rri.register_id
inner join registration_registerpbuser as rrp on rr.id = rrp.register_id
where rr.event_id =59
and rr.status = 'paymentCleared'"

user_register = dbGetQuery(conn1, sql)  


sql = "select PK,p_mobilenumber,p_emailaddress from profile_user "


profile = dbGetQuery(conn, sql)  


all = merge(user_register,profile,by.x = "email","p_emailaddress",all.x = T)

all_import = all[,c('PK',"email")]
all_import$response = ''
all_import$program_id=2
all_import$batch_id=1
colnames(all_import)[1] = 'hash'

if(sum(is.na(all_import$hash)!=0)){
  all_import = all_import[!is.na(all_import$hash),]
  
}

host="media-pbplus-me.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com"
database="api"
user="media_pbplus_me"
password="pcgbros_54883155"


conn2 = dbConnect(MySQL(), user = user,dbname = database,
                  password = password,
                  host = host)

dbWriteTable(conn2, value = all_import, name = "log_edm_response", append = TRUE,row.names=FALSE) 

