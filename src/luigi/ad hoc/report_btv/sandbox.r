
require(RGoogleAnalytics)
ga.counts <- as.numeric(gsub(",","",as.character.factor(ga[seq(8,54,by = 2),])))

id = '247756390135-2l4u5ls8kru5cmptplobqve4vlb92475.apps.googleusercontent.com'
st = 'U3v7j2t3IdBOA9fpnn4nPG09'

tokens <- Auth(id,st)
ValidateToken(tokens)



query.list <- Init(start.date = "2016-03-20",
                   end.date = "2016-07-26",
                   dimensions = c("ga:city"),
                   metrics = "ga:users",
                   max.results = 10000,
                   table.id = "ga:118928229")


ga.query <- QueryBuilder(query.list)
ga.data_browse <- GetReportData(ga.query,tokens)


query.list <- Init(start.date = "2016-03-20",
                   end.date = "2016-07-26",
                   dimensions = c("ga:city"),
                   metrics = "ga:users",
                   max.results = 10000,
                   table.id = "ga:124837146")
ga.query <- QueryBuilder(query.list)
ga.data_ios <- GetReportData(ga.query,tokens)

query.list <- Init(start.date = "2016-03-20",
                   end.date = "2016-07-26",
                   dimensions = c("ga:city"),
                   metrics = "ga:users",
                   max.results = 10000,
                   table.id = "ga:121547195")
ga.query <- QueryBuilder(query.list)
ga.data_android <- GetReportData(ga.query,tokens)


ga_all = merge(ga.data_browse,ga.data_android,by = 'city')
ga_all = merge(ga_all,ga.data_ios, by='city')

ga_all$sum = ga_all$users.x+ga_all$users.y+ga_all$users

users_by_city_ordered = ga_all[order(ga_all$sum,decreasing = T),]

users_by_city_ordered$lng = 0 
users_by_city_ordered$lat = 0

for(i in 2:nrow(users_by_city_ordered)){
  geo_reply = geocode(users_by_city_ordered$city[i], output='all', messaging=TRUE, override_limit=TRUE)
  users_by_city_ordered$lat[i] = geo_reply$results[[1]]$geometry$location$lat
  users_by_city_ordered$lng[i] = geo_reply$results[[1]]$geometry$location$lng
}

for(i in 2:nrow(session_by_city_ordered)){
  geo_reply = geocode(session_by_city_ordered$city[i], output='all', messaging=TRUE, override_limit=TRUE)
  session_by_city_ordered$lng1[i] = geo_reply$results[[1]]$geometry$location$lat
  session_by_city_ordered$lat1[i] = geo_reply$results[[1]]$geometry$location$lng
}


write.csv(users_by_city_ordered[,c('city','sum','lng','lat')],"~/pbplus/report_btv/user_by_city.csv",row.names = F)


session_by_city_ordered$lng = geo_reply$results[[1]]$geometry$bounds$northeast$lng

library(ggmap)


library(ggmap)
library(mapproj)
map <- get_map(location = 'Taiwan', zoom = 8)
ggmap(map)

test = read.csv('~/pbplus/report_btv/sesiion_by_city.csv',header = T)

session_by_city_ordered$exp.sum = exp(session_by_city_ordered$sum)

ggmap(map,darken = c(0.5, "white")) +
  geom_point(aes(x = logi, y = lati, size = sum), data = test)



####

ggmap(map,darken = c(0.5, "white")) +
  geom_point(aes(x = logi, y = lati,color=sum,size=sum), data = test)+
  scale_colour_gradient(low='white',high='red',trans = "log") + scale_size(trans='log')

