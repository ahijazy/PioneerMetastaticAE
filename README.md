PioneerMetastaticAE
==============================
<img src="https://img.shields.io/badge/Study%20Status-Results%20Available-yellow.svg" alt="Study Status: Results Available">

- Analytics use case(s): **Characterization**
- Study type: **Clinical Application**
- Tags: **cancer**
- Study lead: **Pawel Rajwa**
- Study lead forums tag: **[bdemeulder](https://forums.ohdsi.org/u/bdemeulder)**
- Study start date: **October 31, 2022**
- Study end date: **-**
- Protocol: **[Published research protocol](https://doi.org/10.1016/j.euros.2024.02.019)**
- Publications: **-**
- Results explorer: **[Shiny App: Characterization study](https://pioneer-shiny.hzdr.de/app/PioneerMetastaticAE)**
- Results explorer: **[Shiny App: Incidence rates analysis](https://pioneer-shiny.hzdr.de/app/PioneerIncidenceRates)**

The aim of this study is to characterize the metastatic Hormone-Sensitive Prostate Cancer (mHSPC) patients and the rates adverse events of systemic treatments they encounter.

This study is undertaken by the 2023 prostate cancer studyathon of the [IMI PIONEER](https://prostate-pioneer.eu) project.

Requirements
============

- A database in [Common Data Model version 5](https://github.com/OHDSI/CommonDataModel) in one of these platforms: SQL Server, Oracle, PostgreSQL, IBM Netezza, Apache Impala, Amazon RedShift, Google BigQuery, or Microsoft APS.
- R version 4.0.0 or newer
- On Windows: [RTools](http://cran.r-project.org/bin/windows/Rtools/)
- [Java](http://java.com)
- 25 GB of free disk space

How to run
==========
1. Follow [these instructions](https://ohdsi.github.io/Hades/rSetup.html) for setting up your R environment, including RTools and Java. 

2. Open your study package in RStudio. Use the following code to install all the dependencies:

	```r
	#For windows users, you need to first execute this first command:
	Sys.setenv(RENV_DOWNLOAD_FILE_METHOD = "libcurl")
	
	
	renv::restore()
	```

3. In RStudio, select 'Build' then 'Install and Restart' to build the package.

3. Once installed, you can execute the study by modifying and using the code below. For your convenience, this code is also provided under `extras/CodeToRun.R`:

	```r
	library(PioneerMetastaticAE)
	
	# Optional: specify where the temporary files (used by the Andromeda package) will be created:
	options(andromedaTempFolder = "s:/andromedaTemp")
	
	# Maximum number of cores to be used:
	maxCores <- parallel::detectCores()
	
	# The folder where the study intermediate and result files will be written:
	outputFolder <- "c:/PioneerMetastaticAE"
	
	# Details for connecting to the server:
	# See ?DatabaseConnector::createConnectionDetails for help
	connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
									server = "some.server.com/ohdsi",
									user = "joe",
									password = "secret",
									pathToDriver =" path to local JDBC driver")
	
	# The name of the database schema where the CDM data can be found:
	cdmDatabaseSchema <- "cdm_synpuf"
	
	# The name of the database schema and table where the study-specific cohorts will be instantiated:
	cohortDatabaseSchema <- "scratch.dbo"
	cohortTable <- "my_study_cohorts"
	
	# Some meta-information that will be used by the export function:
	databaseId <- "Synpuf"
	databaseName <- "Medicare Claims Synthetic Public Use Files (SynPUFs)"
	databaseDescription <- "Medicare Claims Synthetic Public Use Files (SynPUFs) were created to allow interested parties to gain familiarity using Medicare claims data while protecting beneficiary privacy. These files are intended to promote development of software and applications that utilize files in this format, train researchers on the use and complexities of Centers for Medicare and Medicaid Services (CMS) claims, and support safe data mining innovations. The SynPUFs were created by combining randomized information from multiple unique beneficiaries and changing variable values. This randomization and combining of beneficiary information ensures privacy of health information."
	
	# For some database platforms (e.g. Oracle): define a schema that can be used to emulate temp tables:
	options(sqlRenderTempEmulationSchema = NULL)
	
	runCohortDiagnostics(connectionDetails = connectionDetails,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortDatabaseSchema = cohortDatabaseSchema,
            cohortTable = cohortTable,
            outputFolder = outputFolder,
            databaseId = databaseId,
            databaseName = databaseName,
            databaseDescription = databaseDescription,
            verifyDependencies = TRUE,
            createCohorts = TRUE,
            synthesizePositiveControls = TRUE,
            runAnalyses = TRUE,
            packageResults = TRUE,
            maxCores = maxCores)
	```
 
		
4. To view the results, use the Shiny app:

	```r
	CohortDiagnostics::launchDiagnosticsExplorer()
	```
  
  Note that you can save plots from within the Shiny app. 

License
=======
The PioneerMetastaticAE package is licensed under Apache License 2.0

Development
===========
PioneerMetastaticAE was developed in ATLAS and R Studio.

### Development status

Under development

### Citation
Please do not forget to cite cohort diagnostics developers when using the results of the package:

Gilbert J, Rao G, Schuemie M, Ryan P, Weaver J (2023). CohortDiagnostics: Diagnostics for OHDSI Cohorts. https://ohdsi.github.io/CohortDiagnostics, https://github.com/OHDSI/CohortDiagnostics.


