require(RMySQL)
require(yaml)

args_date_str = commandArgs(trailingOnly=TRUE)
date_args = as.Date(args_date_str,"%Y_%m_%d")

config = yaml.load_file("config.yml")
conn <- dbConnect(MySQL(), user = config$btv_registers$user,dbname = config$btv_registers$name,
    password = config$btv_registers$pass,host = config$btv_registers$host)

dbGetQuery(conn,'SET NAMES utf8')
sql = paste0("SELECT * FROM api_app_log WHERE create_date >= \'",date_args-1,"\' AND create_date < \'",date_args+1,"\'")
api_log = dbGetQuery(conn, sql)

api_log$create_date = strptime(as.character(api_log$create_date),"%Y-%m-%d %H:%M:%S",tz="UTC")
api_log$create_date = api_log$create_date + 28800
api_log_registered = api_log[api_log$action == 'Register',]
api_log_registered = api_log[api_log$comment == 'code:s - message:註冊成功，請用您的手機號碼和設定的密碼登入',]
api_log_registered$create_date = as.Date(api_log_registered$create_date, '%Y-%m-%d')
api_log_registered = api_log_registered[api_log_registered$create_date==date_args,]

## TODO change output format modification needed
sink(paste0("data/registers/daily_btv_",args_date_str,".txt"), append=TRUE, split=TRUE)
print(nrow(api_log_registered))
