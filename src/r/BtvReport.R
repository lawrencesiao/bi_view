host_media = 'media-pbplus-me.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com'

conn2 <- dbConnect(MySQL(), user = 'api',dbname = 'api', password = 'tp6m4xup6',
                   host = host_media) 
dbGetQuery(conn2,'SET NAMES utf8')

api_log = dbGetQuery(conn2, "SELECT * FROM api_app_log") # 使用 SQL query 讀取資料。



api_log$create_date <- strptime(as.character(api_log$create_date),"%Y-%m-%d %H:%M:%S",tz="UTC")

api_log$create_date <- api_log$create_date + 28800
api_log.registered = api_log[api_log$action == 'Register',]

api_log.registered = api_log[api_log$comment == 'code:s - message:註冊成功，請用您的手機號碼和設定的密碼登入',]



gameData = c('2016/03/28','2016/04/01','2016/04/02','2016/04/03','2016/04/06'
             ,'2016/04/07','2016/04/08','2016/04/09','2016/04/19','2016/04/21'
             ,'2016/04/22','2016/04/23','2016/04/24','2016/04/26')
api_log.registered$create_date = as.Date(api_log.registered$create_date, '%Y-%m-%d')

i = 0
api_log.registered[,'gameDate'] = Sys.Date()
while (TRUE) {
  i= i+1
  idx = (api_log.registered[,'create_date'] > gameData[i] & api_log.registered[,'create_date'] <= gameData[i+1])
  api_log.registered[idx,'gameDate'] =  gameData[i+1]
  if(i == (length(gameData)-1)){
    break
  }
}

api_log.registered_counts = aggregate(api_log.registered$id, by=list(Category=api_log.registered$gameDate), FUN=length)



