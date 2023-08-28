# Load required libraries
library(dplyr)
library(purrr)

results_folder='C:/studyathon 3/results/IR'

merge_csv_files=function(results_folder)
{
# Set your results folder path

# Step 1: List zip files
zip_files <- list.files(path = results_folder, pattern = "*.zip", full.names = TRUE)

# Initialize a list to store merged data frames
merged_data_list <- list()

# Step 2: Process each zip file
for (zip_file in zip_files) {
  # Create a temporary folder to extract files
  temp_extracted_folder <- tempfile()
  dir.create(temp_extracted_folder)
  
  # Step 2a: Extract zip files
  unzip(zip_file, exdir = temp_extracted_folder)
  
  # Step 2b: List CSV files within the extracted folder
  csv_files <- list.files(temp_extracted_folder, pattern = "*.csv", full.names = TRUE)
  
  # Step 2c: Read CSV files and merge them by filename
  for (csv_file in csv_files) {
    data <- read.csv(csv_file, header = TRUE)  # Adjust options as needed
    filename <- basename(csv_file)
    
    if (is.null(merged_data_list[[filename]])) {
      merged_data_list[[filename]] <- data
    } else {
      merged_data_list[[filename]] <- bind_rows(merged_data_list[[filename]], data)
    }
  }
  
  # Clean up extracted folder
  unlink(temp_extracted_folder, recursive = TRUE)
}
return(merged_data_list)
}

# Example: Access merged data for a specific filename (CSV)
specific_filename <- "example.csv" 
merged_data <- merged_data_list[[specific_filename]]




