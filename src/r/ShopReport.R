### woo comerce report download needed

shop = read.csv('好物加/woo-export.csv.csv',header = T)

weekNb = seq(as.Date('03/14/16', "%m/%d/%y"), as.Date('04/25/16', "%m/%d/%y"), "week")
shop$Order.Date = as.character.factor(shop$Order.Date)
shop$Order.Date = as.Date(shop$Order.Date,"%Y/%m/%d")

item = read.csv('好物加/item.csv',header=T,stringsAsFactors = F)
item = item[item$Product.Type %in% c('simple','variable'),]
item = item[-1,]
item$Date.Published[1] = "2016-03-29 09:51:40"

sellSum = aggregate(shop$Item.price.EXCL..tax, by=list(Category=shop$weekStart), FUN=sum)
sellCounts = aggregate(shop$Quantity.of.items.purchased, by=list(Category=shop$weekStart), FUN=sum)
item = allocateWeek(item,'03/14/16','04/25/16','Date.Published','%Y-%m-%d')

itemRelease = aggregate(item$Product.Name, by=list(Category=item$weekStart), FUN=length)


allocateWeek = function(df1,str,end,DateCol,dateSchema){
  weekNb = seq(as.Date(str, "%m/%d/%y"), as.Date(end, "%m/%d/%y"), "week")
  df1[,DateCol] = as.Date(df1[,DateCol],dateSchema)
  
  i = 0
  df1[,'weekStart'] = Sys.Date()
  while (TRUE) {
    i= i+1
    idx = (df1[,DateCol] >= weekNb[i] & df1[,DateCol] < weekNb[i+1])
    df1[idx,'weekStart'] =  weekNb[i]
    if(i == (length(weekNb)-1)){
      break
    }
  }
  return(df1)
}


####
require(RMySQL)
host_systexdb = 'shop-pbplus-me.cj68c06i5nax.ap-northeast-1.rds.amazonaws.com'


conn <- dbConnect(MySQL(), user = 'wordpress-user',dbname = 'wordpress-db', password = 'jabawork',
                  host = host_systexdb) 

wp_users = dbGetQuery(conn, "SELECT * FROM wp_users")
wp_users = wp_users[-c(1:11),]
wp_users = allocateWeek(wp_users,'03/14/16','04/25/16','user_registered','%Y-%m-%d')
RegistorCounts = aggregate(wp_users$ID, by=list(Category=wp_users$weekStart), FUN=length)

print(RegistorCounts)
print(sellSum)
print(sellCounts)
print(itemRelease)