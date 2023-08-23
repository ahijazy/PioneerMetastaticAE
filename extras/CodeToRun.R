library(PioneerMetastaticAE)

# Maximum number of cores to be used:
maxCores <- parallel::detectCores()

# The folder where the study intermediate and result files will be written:
outputFolder <- "C:/studyResults/PioneerMetastaticAE"

# Specify another folder where the Incidence rate results will be written:
outputFolderIR<-  "C:/studyResults/PioneerMetastaticAE_IR"
# Optional: specify where the temporary files (used by the Andromeda package) will be created:
options(andromedaTempFolder = file.path(outputFolder, "andromedaTemp"))

# Details for connecting to the server:
# Database management system name; has to be one of the following:
#'sql server','oracle','postgresql','pdw','impala','netezza','bigquery','spark','sqlite','redshift','hive','sqlite extended','duckdb','snowflake','synapse'
 DBMS = "postgresql" 
#user name
 USER = "johnsmith"
#password
 PASSWORD= "john1234"
#server
 SERVER = "localhost/postgres"
#port
 DB_PORT = 5432
# path to local JDBC driver
 pathToDriver='C:/JDBC' 

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = DBMS,
                                                                user = USER,
                                                                password = PASSWORD,
                                                                server = SERVER,
                                                                port = DB_PORT,
                                                                pathToDriver = pathToDriver)
# The name of the database schema where the CDM data can be found:
cdmDatabaseSchema <- "CDM_IBM_MDCD_V1153.dbo"

# The name of the database schema and table where the study-specific cohorts will be instantiated:
cohortDatabaseSchema <- "scratch.dbo"
cohortTable <- "PioneerMetastaticAE"

# Some meta-information that will be used by the export function:
databaseId <- "Synpuf"
databaseName <- "Medicare Claims Synthetic Public Use Files (SynPUFs)"
databaseDescription <-
  "Medicare Claims Synthetic Public Use Files (SynPUFs) were created to allow interested parties to gain familiarity using Medicare claims data while protecting beneficiary privacy. These files are intended to promote development of software and applications that utilize files in this format, train researchers on the use and complexities of Centers for Medicare and Medicaid Services (CMS) claims, and support safe data mining innovations. The SynPUFs were created by combining randomized information from multiple unique beneficiaries and changing variable values. This randomization and combining of beneficiary information ensures privacy of health information."

# For some database platforms (e.g. Oracle): define a schema that can be used to emulate temp tables:
options(sqlRenderTempEmulationSchema = NULL)

PioneerMetastaticAE::execute(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  verifyDependencies = TRUE,
  outputFolder = outputFolder,
  databaseId = databaseId,
  databaseName = databaseName,
  databaseDescription = databaseDescription
)


# Execute the incidence rate analysis
execute_IR  (connectionDetails=connectionDetails,
                       cdmDatabaseSchema=cdmDatabaseSchema,
                       cohortDatabaseSchema = cohortDatabaseSchema,
                       cohortTable = cohortTable,
                       tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                       databaseId = databaseId,
		       outputFolderIR=outputFolderIR
                      ) 

# if you want to view the shiny app locally, uncomment the following section
# CohortDiagnostics::createMergedResultsFile(
#  dataFolder = outputFolder,
#  sqliteDbPath = file.path(outputFolder,
#                           "MergedCohortDiagnosticsData.sqlite")
#)
#CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)

