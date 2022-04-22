####################################################################
########################### script setup ########################### 
####################################################################

### specify working directory
wd <- '~/mke-property-sales'

setwd(wd)

### install packages (load)
if (!require("pacman")) install.packages("pacman")
pacman::p_load(dplyr,
               stringr,
               rjson,
               lubridate,
               tidyr,
               RMySQL)

### get user name (these need to be stored in .bash_profile and/or .Renviron file)
mysql_user_name <- Sys.getenv("mysql_user_name")

### get password (these need to be stored in .bash_profile and/or .Renviron file)
mysql_password <- Sys.getenv("mysql_password")

### get host (these need to be stored in .bash_profile and/or .Renviron file)
mysql_host <- Sys.getenv("mysql_host")

### connect to MySQL database
database_connection <- RMySQL::dbConnect(
  RMySQL::MySQL(),
  dbname = 'barutt_prod',
  username = mysql_user_name,
  password = mysql_password,
  host = mysql_host,
  port = 3306,
)

### remove objects
rm(mysql_user_name)
rm(mysql_password)
rm(mysql_host)

####################################################################
############################ data import ########################### 
####################################################################

urls <- c(
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/b96b9f87-f4b2-4578-aee1-3fbe547b123b/download/2009-property-sales-data.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/9047305c-fb7d-43b4-8f5b-9475a5a74ac5/download/2010-property-sales-data.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/7af4ee9d-d0c0-4afa-9018-134451ada91c/download/2011-property-sales-data.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/4d84e2a5-a07d-4197-93b1-bb1a2b822839/download/2012-property-sales-data.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/207703e7-c925-4b2f-990d-82fa967cea77/download/2013-property-sales-data.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/d0407d22-0dcc-48f6-a573-267788326b1d/download/2014-property-sales-data.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/65b39e27-7286-47ae-b01b-4a963732b63e/download/2015-property-sales-data.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/88b6442e-5563-4d5d-ba8f-59bb3042673b/download/2016-property-sales-data.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/0a12bdda-f080-4e98-99ea-52ef13466579/download/2017-property-sales-data.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/1e7c4ef4-d12f-4d82-9029-f5622151b51b/download/2018-property-sales-data.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/7c2f3357-8380-4cd7-9b50-67b0e554ff7d/download/2019-property-sales-data.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/5ad3b44d-ba65-47eb-bd08-3f6cd07bf597/download/property-sales-data-2020.csv',
  'https://data.milwaukee.gov/dataset/7a8b81f6-d750-4f62-aee8-30ffce1c64ce/resource/31f81cfe-b34f-496b-9361-2025514920cb/download/armslengthsales_2021_valid.csv'
)

annual_sales_data <- list()

for (i in 1:length(urls)){
  
  tmp_data <- read.csv(file = urls[i], header = TRUE)
  names(tmp_data) <- gsub(pattern = '_', replacement = '', x = tolower(names(tmp_data)))
  try(tmp_data <- tmp_data %>% select(-propertyid))
  
  colnames(tmp_data)[1] <- 'property_type'
  colnames(tmp_data)[2] <- 'tax_key'
  colnames(tmp_data)[3] <- 'address'
  colnames(tmp_data)[4] <- 'condo_project'
  colnames(tmp_data)[5] <- 'distict'
  colnames(tmp_data)[6] <- 'neighborhood'
  colnames(tmp_data)[7] <- 'style'
  colnames(tmp_data)[8] <- 'exterior_wall_type'
  colnames(tmp_data)[9] <- 'stories'
  colnames(tmp_data)[10] <- 'year_built'
  colnames(tmp_data)[11] <- 'rooms'
  colnames(tmp_data)[12] <- 'finished_square_feet'
  colnames(tmp_data)[13] <- 'units'
  colnames(tmp_data)[14] <- 'bedrooms'
  colnames(tmp_data)[15] <- 'full_baths'
  colnames(tmp_data)[16] <- 'half_baths'
  colnames(tmp_data)[17] <- 'lot_size'
  colnames(tmp_data)[18] <- 'sale_date'
  colnames(tmp_data)[19] <- 'sale_price'
  
  annual_sales_data[[length(annual_sales_data) + 1]] <- tmp_data
  
  print(i)
  
}

annual_sales_data <- do.call(rbind, annual_sales_data)

annual_sales_data <- annual_sales_data %>%
                     mutate(char_num = nchar(as.character(sale_date)),
                            slash_ind = grepl(pattern = '/', x = sale_date),
                            sale_date = ifelse(char_num == 7,as.character(paste(sale_date,'-01',sep='')),as.character(as.Date(sale_date,format = "%m/%d/%Y"))),
                            sale_date = as.Date(sale_date),
                            sale_price = gsub(pattern = '\\$', replacement = '', x = as.character(sale_price)),
                            sale_price = gsub(pattern = ',', replacement = '', x = as.character(sale_price)),
                            finished_square_feet = gsub(pattern = ',', replacement = '', x = as.character(finished_square_feet)),
                            lot_size = gsub(pattern = ',', replacement = '', x = as.character(lot_size))) %>%
                     select(-c(char_num,slash_ind))

### write data do MySQL database
dbWriteTable(conn = database_connection,
             name = 'mke_property_sales',
             value = annual_sales_data,
             append = FALSE,
             overwrite = TRUE,
             row.names = FALSE)
  