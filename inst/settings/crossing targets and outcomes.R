target=read.csv("./inst/settings/targets.csv")  
outcome=read.csv("./inst/settings/outcomes.csv")
crossed <- merge(target, outcome, by = NULL)

names(crossed)=c("target_id","target_name","outcome_id","outcome_name","episodic","chronic")
crossed <- crossed[order(crossed$target_id), ]
write.csv(x=crossed,file = "inst/settings/IRsettings.csv")