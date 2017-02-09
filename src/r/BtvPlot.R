############
GuestTeam = '義大'
Date = '0423'
strTime <- '2016-04-23 17:00:00'

############

files <- list.files(paste0("Livestream/2016",Date,"/"))

totalViews <- read.csv(paste0("Livestream/2016",Date,"/",files[1]),header=T)
totalViews <- totalViews[-61,]
for(f in files[-1]){
  tmp <- read.csv(paste0("Livestream/2016",Date,"/",f),header=T)[-61,]
  totalViews <- rbind(totalViews,tmp)
}
totalViews$date.time <- as.POSIXct(totalViews$date.time, format="%Y-%m-%d %H:%M")


pttData <- read.csv(paste0("~/posts_",Date,".csv"),header = F)

for(i in 1:nrow(pttData)){
  pttData$inner[i] <- substr(strsplit(as.character.factor(pttData$V3[i]),"轉播] ")[[1]][2],1,2)
}

pttData <- pttData[!is.na(pttData$inner),]
startIdx <- which(pttData$inner=='賽前')
endIdx <- which(pttData$inner=='賽後')

if(length(startIdx)==0){
  pttData = pttData[(1):(endIdx),]
}else{
  pttData = pttData[(startIdx+1):(endIdx),]
}
  
middleInd <- which(pttData$inner=='中場')
pttData <- pttData[-(middleInd),]

pttData$change = 0
for(i in 1:(nrow(pttData)-1)){
  if(pttData$inner[i] != pttData$inner[i+1]){
    pttData$change[i] = 1
  }
}
pttData$V5[pttData$change==1]

if(length(startIdx)==0){
  gameStart = as.POSIXct(as.character(strTime), format="%Y-%m-%d %H:%M:%S")
}else{
  gameStart = as.POSIXct(as.character.factor(pttData$V5[startIdx]), format="%Y-%m-%d %H:%M:%S")
  
}


commercialAdStart = as.POSIXct(as.character.factor(pttData$V5[pttData$change==1]), format="%Y-%m-%d %H:%M:%S")

commercialAdEnd = commercialAdStart
commercialAdEnd[1:9] = commercialAdStart[1:9] + 90
commercialAdEnd[10] = commercialAdStart[10] + 270
commercialAdEnd[11:length(commercialAdStart)] = commercialAdStart[11:length(commercialAdStart)] + 90

guestStart = c(gameStart,commercialAdEnd[c(seq(2,length(commercialAdStart)-1,2))])
guestEnd = commercialAdStart[seq(1,length(commercialAdStart),2)]

homeStart = commercialAdEnd[seq(1,length(commercialAdStart)-1,2)]
homeEnd = commercialAdStart[seq(2,length(commercialAdStart),2)]

a <- ggplot(totalViews, aes(date.time, average.concurrent.viewers)) + geom_line()


rect <- data.frame(xmin=commercialAdStart, xmax=commercialAdEnd, ymin=-Inf, ymax=Inf)
rectGuest <- data.frame(xmin=guestStart, xmax=guestEnd, ymin=-Inf, ymax=Inf)
rectHome <- data.frame(xmin=homeStart, xmax=homeEnd, ymin=-Inf, ymax=Inf)
a = a + geom_rect(data=rect, aes(xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax),
                  color="grey20",
                  alpha=0.5,
                  inherit.aes = FALSE) 
b = a  + geom_rect(data=rectGuest, aes(xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax),
                   fill="blue",
                   alpha=0.5,
                   inherit.aes = FALSE) 
b = b  + geom_rect(data=rectHome, aes(xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax),
                   fill="yellow",
                   alpha=0.5,
                   inherit.aes = FALSE) 


jpeg(paste0('Livestream/Monitoring',Date,".jpg"), width = 7, height = 4, units = 'in', res = 300)
b + ggtitle(paste0(Date,"v.s ",GuestTeam)) +theme(text = element_text(family = 'SimSun'))
dev.off()
