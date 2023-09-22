# Copyright 2023 PIONEER
#
# This file is part of PioneerMetastaticAE
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Execute the cohort diagnostics
#'
#' @details
#' This function extracts the data needed to calculate incidence rates of outcomes across targets.
#'
#' @param connectionDetails                   An object of type \code{connectionDetails} as created
#'                                            using the
#'                                            \code{\link[DatabaseConnector]{createConnectionDetails}}
#'                                            function in the DatabaseConnector package.
#' @param cdmDatabaseSchema                   Schema name where your patient-level data in OMOP CDM
#'                                            format resides. Note that for SQL Server, this should
#'                                            include both the database and schema name, for example
#'                                            'cdm_data.dbo'.
#' @param cohortDatabaseSchema                Schema name where intermediate data can be stored. You
#'                                            will need to have write privileges in this schema. Note
#'                                            that for SQL Server, this should include both the
#'                                            database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable                         The name of the table that will be created in the work
#'                                            database schema. This table will hold the exposure and
#'                                            outcome cohorts used in this study.
#' @param tempEmulationSchema                 Some database platforms like Oracle and Impala do not
#'                                            truly support temp tables. To emulate temp tables,
#'                                            provide a schema with write privileges where temp tables
#'                                            can be created.
#' @param verifyDependencies                  Check whether correct package versions are installed?
#' @param outputFolderIR          	      Name of local folder to place results; make sure to use
#'                                            forward slashes (/). Do not use a folder on a network
#'                                            drive since this greatly impacts performance.
#' @param databaseId                          A short string for identifying the database (e.g.
#'                                            'Synpuf').
#' @param databaseName                        The full name of the database (e.g. 'Medicare Claims
#'                                            Synthetic Public Use Files (SynPUFs)').
execute_IR <- function(connectionDetails,
                       cdmDatabaseSchema,
                       cohortDatabaseSchema = cdmDatabaseSchema,
                       cohortTable = "cohort",
                       tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                       databaseId = databaseId,
					   outputFolderIR=outputFolderIR) 
{
# Let's check if output folder is present and if not create it 
if (!file.exists(outputFolderIR)) {
  dir.create(outputFolderIR, recursive = TRUE)
}
# the csv where the analysis settings are
IRsettings = read.csv("./inst/settings/IRsettings.csv")
##########################################################################################
##########################################################################################
##########################################################################################
# first get all the sql
#all outcomes
sql_all_outcomes='SELECT T.COHORT_DEFINITION_ID as target_id, O.COHORT_DEFINITION_ID as outcome_id, T.SUBJECT_ID ,T.COHORT_START_DATE,O.COHORT_START_DATE as OUTCOME_START_DATE,T.COHORT_END_DATE, p.YEAR_OF_BIRTH
FROM @cohortDatabaseSchema.@cohortTable T
JOIN @cohortDatabaseSchema.@cohortTable O ON T.subject_id = O.subject_id
JOIN @cdmDatabaseSchema.person p ON T.subject_id = p.person_id
WHERE T.cohort_definition_id = @target_id
  AND O.cohort_definition_id = @outcome_id
  AND O.cohort_start_date >= T.cohort_start_date
  AND O.cohort_end_date <= T.cohort_end_date'
##########################################################################################
##########################################################################################
##########################################################################################
#first outcome during after target
sql_first_outcome='SELECT T.COHORT_DEFINITION_ID as target_id, O.COHORT_DEFINITION_ID as outcome_id, T.SUBJECT_ID ,T.COHORT_START_DATE,O.COHORT_START_DATE as OUTCOME_START_DATE,T.COHORT_END_DATE, p.YEAR_OF_BIRTH
FROM @cohortDatabaseSchema.@cohortTable T
JOIN @cohortDatabaseSchema.@cohortTable O ON T.subject_id = O.subject_id
JOIN @cdmDatabaseSchema.person p ON T.subject_id = p.person_id
WHERE T.cohort_definition_id = @target_id
  AND O.cohort_definition_id = @outcome_id
  AND O.cohort_start_date >= T.cohort_start_date
  AND O.cohort_start_date = (
    SELECT MIN(cohort_start_date)
    FROM @cohortDatabaseSchema.@cohortTable
    WHERE subject_id = O.subject_id
      AND cohort_definition_id = @outcome_id
      AND cohort_start_date >= T.cohort_start_date
      AND cohort_end_date <= T.cohort_end_date
  )'
##########################################################################################
##########################################################################################
##########################################################################################
# All the target population 
sql_target ='SELECT T.COHORT_DEFINITION_ID as target_id, T.SUBJECT_ID,T.COHORT_START_DATE,T.COHORT_END_DATE,p.YEAR_OF_BIRTH
FROM @cohortDatabaseSchema.@cohortTable T 
JOIN @cdmDatabaseSchema.person p ON T.subject_id = p.person_id 
WHERE T.COHORT_DEFINITION_ID=@target_id'

##########################################################################################
##########################################################################################
##########################################################################################
### 
#takes in the sql, target id and outcome id as input
sql_translate_ae=function(sql,target_id,outcome_id)
{
sql_rendered= SqlRender::render(sql,
				  cdmDatabaseSchema=cdmDatabaseSchema,
				  cohortDatabaseSchema=cohortDatabaseSchema,
				  cohortTable=cohortTable,
				  target_id=target_id,
				  outcome_id=outcome_id)
return(SqlRender::translate(sql_rendered,targetDialect = DBMS))
}
##########################################################################################
##########################################################################################
##########################################################################################
#takes in Sql and target id as input 
sql_translate_target=function(sql,target_id)
{
sql_rendered= SqlRender::render(sql,
				  cdmDatabaseSchema=cdmDatabaseSchema,
				  cohortDatabaseSchema=cohortDatabaseSchema,
				  cohortTable=cohortTable,
				  target_id=target_id)
return(SqlRender::translate(sql_rendered,targetDialect = DBMS))
}
#Connection details
conn=DatabaseConnector::connect(connectionDetails)
##########################################################################################
##########################################################################################
##########################################################################################
#takes in translated and rendered SQL as input and gives the data from a connection
get_data=function(sql)
{
suppressMessages(as.data.frame(DatabaseConnector::querySql(conn,sql)))
}
##########################################################################################
##########################################################################################
##########################################################################################
episodic_prep=function(target,outcome)
{
  suppressMessages(
 df_target <-data.frame(  (data.frame(target_id       = target$TARGET_ID) %>%
                  mutate(subject_id        = target$SUBJECT_ID,
                         age_at_index_T    = lubridate::year(target$COHORT_START_DATE) - target$YEAR_OF_BIRTH,
                         follow_up_years   = as.numeric(difftime( target$COHORT_END_DATE, target$COHORT_START_DATE, units = "days"))/365.25,
                         index_year_target = lubridate::year(target$COHORT_START_DATE)) %>% 
                  group_by(target_id, subject_id, age_at_index_T, index_year_target) %>%
                  summarise(follow_up_time = sum(follow_up_years)))))

  
 df_ae <- (mutate(outcome,
                   outcome_id             = outcome$OUTCOME_ID,
                   subject_id             = outcome$SUBJECT_ID,
                   age_at_index_T         = lubridate::year(outcome$COHORT_START_DATE) - outcome$YEAR_OF_BIRTH,
                   time_to_outcome_years  = as.numeric(difftime(outcome$OUTCOME_START_DATE, outcome$COHORT_START_DATE, units = "days"))/365.25,
                   index_year_target      = lubridate::year(outcome$COHORT_START_DATE)))

df_ae_1=data.frame(cbind(	df_ae$TARGET_ID,
							df_ae$OUTCOME_ID,
							df_ae$SUBJECT_ID,
							df_ae$age_at_index_T,
							df_ae$index_year_target,
							df_ae$time_to_outcome_years))		
names(df_ae_1)=c("target_id","outcome_id","subject_id","Age_Group","Year","time")
event=1
df_ae_1=cbind(df_ae_1,event)
OUTCOME_ID=unique(df_ae_1$outcome_id)
df_target=cbind(df_target,OUTCOME_ID)
df_o_1=data.frame(cbind(df_target$target_id ,
						df_target$OUTCOME_ID,
						df_target$subject_id ,
						df_target$age_at_index_T,
						df_target$index_year_target,
						df_target$follow_up_time ))
names(df_o_1)=c("target_id","outcome_id","subject_id","Age_Group","Year","time")
event=0
df_o_1=cbind(df_o_1,event)
df=data.frame(rbind(df_o_1, df_ae_1))
df$Age_Group= cut(df$Age_Group, breaks = c(18,55,70,80,120))
return(df)
}

##########################################################################################
##########################################################################################
##########################################################################################

episodic_events=function(df)
{
df=df[df$event==1,]
df2 <- 
 df %>%
  arrange(subject_id, time) %>%
  group_by(subject_id, target_id, outcome_id, Age_Group) %>%
  summarise(
    count_of_events = n(),
    mean_time_between_events = ifelse(count_of_events > 1, mean(diff(time)), NA),
    sd_time_between_events = ifelse(count_of_events > 1, sd(diff(time)), NA)
  ) %>%
  group_by(target_id, outcome_id, Age_Group) %>%
  summarise(
    total_num_events = sum(count_of_events),
    avg_number_of_events = mean(count_of_events),
    conditional_mean_time_between_events = mean(mean_time_between_events, na.rm = TRUE),
    conditional_sd_time_between_events = sqrt(sum((count_of_events - 1) * sd_time_between_events^2) / (total_num_events - length(unique(subject_id))))
  )
 return(as.data.frame(df2))
}
##########################################################################################
##########################################################################################
##########################################################################################
#data preparation for non episodic events
data_prep <- function(ae, target){
  # Create a table for the first adverse event
  df_ae <- data.frame(target_id = ae$TARGET_ID)
  
  # Create the dataframe with adverse events with relevant variables
  suppressMessages(
  df_ae <- (mutate(df_ae,
                   outcome_id             = ae$OUTCOME_ID,
                   subject_id             = ae$SUBJECT_ID,
                   age_at_index_T         = lubridate::year(ae$COHORT_START_DATE) - ae$YEAR_OF_BIRTH,
                   time_to_outcome_years  = as.numeric(difftime(ae$OUTCOME_START_DATE, ae$COHORT_START_DATE, units = "days"))/365.25,
                   index_year_target      = lubridate::year(ae$COHORT_START_DATE)) %>%
              group_by(target_id, outcome_id, subject_id, age_at_index_T, time_to_outcome_years, index_year_target) %>%
              summarise(count = n())))
  
  # Create the dataframe containing information on all patients
  suppressMessages(
  df_target <- (data.frame(target_id       = target$TARGET_ID) %>%
                  mutate(subject_id        = target$SUBJECT_ID,
                         age_at_index_T    = lubridate::year(target$COHORT_START_DATE) - target$YEAR_OF_BIRTH,
                         follow_up_years   = as.numeric(difftime( target$COHORT_END_DATE, target$COHORT_START_DATE, units = "days"))/365.25,
                         index_year_target = lubridate::year(target$COHORT_START_DATE)) %>% 
                  group_by(target_id, subject_id, age_at_index_T, index_year_target) %>%
                  summarise(follow_up_time = sum(follow_up_years))))
  
  # Combine the dataframes with information on adverse events and all patients
  out_id  <- unique(df_ae$outcome_id)
  suppressMessages(
  df_comb <- (left_join(df_target, df_ae) %>% 
                mutate(studytime = if_else(!is.na(time_to_outcome_years), time_to_outcome_years, follow_up_time),
		                   outcome_id = out_id) %>%
                mutate(count = if_else(is.na(count), 0, count))))
  
  return(as.data.frame(df_comb))
}

#data preparation for episodic events
data_prep_episodic <- function(ae, target){
  # Create a table for the first adverse event
  df_ae <- data.frame(target_id = ae$TARGET_ID)
  
  # Create the dataframe with adverse events with relevant variables
  suppressMessages(
  df_ae <- (mutate(df_ae,
                   outcome_id             = ae$OUTCOME_ID,
                   subject_id             = ae$SUBJECT_ID,
                   age_at_index_T         = lubridate::year(ae$COHORT_START_DATE) - ae$YEAR_OF_BIRTH,
                   time_to_outcome_years  = as.numeric(difftime(ae$OUTCOME_START_DATE, ae$COHORT_START_DATE, units = "days"))/365.25,
                   index_year_target      = lubridate::year(ae$COHORT_START_DATE)) %>%
              group_by(target_id, outcome_id, subject_id, age_at_index_T, time_to_outcome_years, index_year_target) %>%
              summarise(count = n())))
  
  # Create the dataframe containing information on all patients
  suppressMessages(
  df_target <- (data.frame(target_id       = target$TARGET_ID) %>%
                  mutate(subject_id        = target$SUBJECT_ID,
                         age_at_index_T    = lubridate::year(target$COHORT_START_DATE) - target$YEAR_OF_BIRTH,
                         follow_up_years   = as.numeric(difftime( target$COHORT_END_DATE, target$COHORT_START_DATE, units = "days"))/365.25,
                         index_year_target = lubridate::year(target$COHORT_START_DATE)) %>% 
                  group_by(target_id, subject_id, age_at_index_T, index_year_target) %>%
                  summarise(follow_up_time = sum(follow_up_years))))
  
  # Combine the dataframes with information on adverse events and all patients
  out_id  <- unique(df_ae$outcome_id)
  suppressMessages(
  df_comb <- (left_join(df_target, df_ae) %>% 
                mutate(studytime = follow_up_time,
		                   outcome_id = out_id) %>%
                mutate(count = if_else(is.na(count), 0, count))))
  
  return(as.data.frame(df_comb))
}
##########################################################################################
##########################################################################################
##########################################################################################

# cut and aggregate data
cut_and_aggregate = function(df, breaks_age = c(55, 70, 80)){	
  break_points_age  <- c(18, breaks_age, Inf) 
  df                <- mutate(df, age_cat = cut(age_at_index_T, breaks = break_points_age), include.lowest = TRUE)
  out_id            <- unique(df$outcome_id)
  suppressMessages(
  df <- (group_by(df, target_id, age_cat, index_year_target) %>%
           summarise(nr_events = sum(count, na.rm = TRUE), time_at_risk = sum(studytime), nr_patients = n())))
  
  # Include possibly missing groups
  res        <- expand.grid(unique(df$target_id), 
                            unique(df$age_cat), 
                            unique(df$index_year_target))
  names(res) <- c("target_id", "age_cat", "index_year_target")  
    suppressMessages(
  res        <- (left_join(res, df) %>% 
                   mutate(nr_events    = coalesce(nr_events, rep(0, nrow(res))), 
                          time_at_risk = coalesce(time_at_risk, rep(0, nrow(res))),
                          nr_patients  = coalesce(nr_patients, rep(0, nrow(res))))))
  res$outcome_id <- out_id
  return(as.data.frame(res))
}

##########################################################################################
##########################################################################################
##########################################################################################
write_table=function(object,csv_file)
{
if (file.exists(csv_file)) {
		  write.table(object, csv_file, sep = ",", col.names = FALSE, row.names = FALSE, append = TRUE)
		} else 	{
		  write.table(object, csv_file, sep = ",", col.names = TRUE, row.names = FALSE)
				}     
}
##########################################################################################
##########################################################################################
##########################################################################################
  cohort_id_target	=numeric(0)
  cohort_id_outcome	=numeric(0)
  target_count		=numeric(0)
  outcome_count		=numeric(0)
  chronic			=numeric(0)
  episodic			=numeric(0)
  target_id  <- unique(IRsettings$target_id)
  outcome_id <- unique(IRsettings$outcome_id)
  total_iterations <- nrow(IRsettings)
  counter1=0   #counter for chronic  events
  counter2=0   #counter for episodic events
  s1=NULL
  s1_age=NULL
  s2=NULL
  s2_age=NULL
  mcf1=NULL
  mcf1_age=NULL
  mcf_episodic=NULL
  mcf_episodic_by_age=NULL
  df_counts=  cbind(IRsettings,target_count=0,outcome_count=0)
for(i in 1:length(target_id)){
    target <- sql_translate_target(sql = sql_target, target_id = target_id[i]) %>% get_data
	if(nrow(target)==0)
	{
	print(paste("No data for cohort:", target_id[i]))	 
	next
	}
	indicator=which(target_id [i]==	df_counts	$target_id)
	df_counts$target_count[indicator]=nrow(target)
	OutcomeList=IRsettings[IRsettings$target_id==target_id[i],]
	#if target is not empty, start looping over the outcomes
	for(j in 1:nrow(OutcomeList))
	{
		if(OutcomeList$chronic[j]==1)
		{
           outcome <- sql_translate_ae(sql = sql_first_outcome,target_id=target_id[i], outcome_id = OutcomeList$outcome_id[j]) %>% get_data
		   if(nrow(outcome)==0)
			{
				print(paste("No outcomes for target cohort:", target_id[i],"and outcome:",OutcomeList$outcome_id[j]))
 				next
			}
		indicator=which(	OutcomeList	$target_name	[j]	==	df_counts	$target_name	&
							OutcomeList	$target_id		[j]	==	df_counts	$target_id		&
							OutcomeList	$outcome_name	[j]	==	df_counts	$outcome_name	&
							OutcomeList	$outcome_id		[j]	==	df_counts	$outcome_id		&
							OutcomeList	$episodic		[j]	==	df_counts	$episodic		&
							OutcomeList	$chronic		[j]	==	df_counts	$chronic	)
		df_counts$outcome_count[indicator]=nrow(outcome)
		counter1=counter1+1
		print(paste( " Calculating incidence for target:", target_id[i],"and outcome:", OutcomeList$outcome_id[j]))
		# Get the prepped data set
		ds <- data_prep(ae = outcome, target = target)
        Age_Group= cut(ds$age_at_index_T, breaks = c(18,55,70,80,120))
	 
	 
		# fit Keplan-Meier Curves (overall (s1) and by age s1_age) - chronic events
		 
		s1[[counter1]] <- survfit(Surv(ds$studytime, ds$count) ~  1,   data = ds)
		s1[[counter1]]$target		=target_id[i]
		s1[[counter1]]$outcome		=OutcomeList$outcome_id[j]	
		s1[[counter1]]$databaseId	=databaseId

		s1_age[[counter1]] <- survfit(Surv(ds$studytime, ds$count) ~  Age_Group,   data = ds)
		s1_age[[counter1]]$target		=target_id[i]
		s1_age[[counter1]]$outcome		=OutcomeList$outcome_id[j]	
		s1_age[[counter1]]$databaseId	=databaseId
		# fit survival curve (daan)
		ds_split <- survival::survSplit(Surv(ds$studytime, ds$count) ~ 1, data = ds, cut = c(0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.5, 3),zero=-0.1)
        ds_split <- (group_by(ds_split, tstart) %>%
                         summarise(event        = sum(event), 
                                   time_at_risk = sum(tstop - tstart),
                                   n_at_risk    = n(),
								   target_id=target_id[i],
								   outcome_id=OutcomeList$outcome_id[j],
								   databaseId=databaseId))
								   
        csv_file <- paste0( outputFolderIR, "/survsplit.csv")
		write_table(ds_split,csv_file)
     	   
		ds_ir     <- cbind(cut_and_aggregate(ds),databaseId)
		csv_file <- paste0(outputFolderIR,"/chronic.csv")
		write_table(ds_ir,csv_file)
		}
		##############################################################################
		##############################################################################
		##############################################################################
		##############################################################################
		############################################################################## 
        # episodic events
		 if(OutcomeList$episodic[j]==1)
		{
           outcome <- sql_translate_ae(sql = sql_all_outcomes,target_id=target_id[i], outcome_id = OutcomeList$outcome_id[j]) %>% get_data
		     if(nrow(outcome)==0)
			   {
			   print(paste("No outcomes for cohort:", target_id[i],"and outcome:",OutcomeList$outcome_id[j]))
			   next
			   }
	
		indicator=which(	OutcomeList	$target_name	[j]	==	df_counts	$target_name	&
							OutcomeList	$target_id		[j]	==	df_counts	$target_id		&
							OutcomeList	$outcome_name	[j]	==	df_counts	$outcome_name	&
							OutcomeList	$outcome_id		[j]	==	df_counts	$outcome_id		&
							OutcomeList	$episodic		[j]	==	df_counts	$episodic		&
							OutcomeList	$chronic		[j]	==	df_counts	$chronic	)
		df_counts$outcome_count[indicator]=nrow(outcome)
 		counter2=counter2+1
	    print(paste( " Calculating incidence for target:", target_id[i],"and outcome:", OutcomeList$outcome_id[j]))
		ds <- data_prep_episodic(ae = outcome, target = target)
		Age_Group= cut(ds$age_at_index_T, breaks = c(18,55,70,80,120))
		modified_count <- ifelse(ds$count> 0, 1, 0)
	 
		s2[[counter2]] <- survfit(Surv(ds$studytime, modified_count) ~ 1,data = ds)
		s2[[counter2]]$target		=target_id[i]
		s2[[counter2]]$outcome		=OutcomeList$outcome_id[j]	
		s2[[counter2]]$databaseId	=databaseId
		
		s2_age[[counter2]] <- survfit(Surv(ds$studytime, modified_count) ~ Age_Group,data = ds)
		s2_age[[counter2]]$target		=target_id[i]
		s2_age[[counter2]]$outcome		=OutcomeList$outcome_id[j]	
		s2_age[[counter2]]$databaseId	=databaseId

		# add the mcf here
		# write this to a csv table
		# this does not take outcome (takes an object of type 
		#save the episodic event table
		ds_ep=episodic_prep(target,outcome)
		df_events=episodic_events(ds_ep)
		df_events=cbind(df_events,databaseId)
        csv_file <- paste0(outputFolderIR,"/episodic_events.csv")
		write_table(df_events,csv_file)
		
 		mcf1=reda::mcf(Recur(id=subject_id,time=time,event=event)~1,data=ds_ep)
		mcf1_age=reda::mcf(Recur(id=subject_id,time=time,event=event)~Age_Group,data=ds_ep)
		mcf_episodic[[counter2]]= mcf1@MCF
		mcf_episodic[[counter2]]$target=target_id[i]
		mcf_episodic[[counter2]]$outcome=OutcomeList$outcome_id[j]
		mcf_episodic[[counter2]]$databaseId=databaseId
		
		mcf_episodic_by_age[[counter2]]=mcf1_age@MCF
 		mcf_episodic_by_age[[counter2]]$target=target_id[i]
		mcf_episodic_by_age[[counter2]]$outcome=OutcomeList$outcome_id[j]
		mcf_episodic_by_age[[counter2]]$databaseId=databaseId
		ds_ir     <- cbind(cut_and_aggregate(ds),databaseId)
		csv_file <- paste0(outputFolderIR,"/episodic.csv")
		write_table(ds_ir,csv_file)
		}
	}
   }
   df_counts=cbind(df_counts,databaseId)
   
   saveRDS(s1,					paste0(outputFolderIR,		 	"/chronic_tte.rds"))
   saveRDS(s1_age,				paste0(outputFolderIR, 			"/chronic_tte_by_age.rds"))
   saveRDS(s2,					paste0(outputFolderIR,	 		"/episodic_tte.rds"))
   saveRDS(s2_age,				paste0(outputFolderIR, 			"/episodic_tte_by_age.rds"))
   saveRDS(mcf_episodic,		paste0(outputFolderIR,	 		"/mcf_episodic.rds"))
   saveRDS(mcf_episodic_by_age,	paste0(outputFolderIR, 			"/mcf_episodic_by_age.rds"))
   write.csv(df_counts,			paste0(outputFolderIR, 			"/counts.csv"))
	}
