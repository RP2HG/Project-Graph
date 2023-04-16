library(yaml)
library(log4r)
library(tidyverse)
library(DBI)

#  'Bootstrap' data loader into the database - this loads a simple nodes & edges set of data from
# CSV files into the database - loadas a small but know good data set into the database


# Set the logger's file output, current level is get all, and then send a startup message
# To-Do - come back here and build a better file name & date stamp
# To-Do - Pull the log file level out of YAML File

log_filename <- 'Logs/TLDG2a.log'
log_loglevel <- 'DEBUG'

if (file.exists(log_filename)){
  file.rename(log_filename, paste(log_filename, ".old", sep=""))
}


logger <- create.logger()
logfile(logger) <- log_filename
level(logger) <- log_loglevel

log_msg <- paste("Starting Temp Loader with Logging set to ", log_loglevel)
info(logger, log_msg)

# Set up our file names, table names etc

config_file <- "Test-Yaml.yml"
graph_data_store <- "Project-Graph-DB.db"

# table names for the graph master data - these are the datasets we feed into igraph. tidygrap, visNetwork etc
master_nodes <- "HR-nodes"
master_edges <- "HR-edges"
graph_nodes <- "G-nodes"
graph_edges <- "G-edges"

# Time to start work, load up the configuration file
# The config file points to the files containing our Programme components
# To-Do - create a error handler so that YAML config errors get captured and reported in the log file

log_msg <- paste("Loading Config File: ", config_file)
debug(logger, log_msg)
yaml_data <- read_yaml('Test-Yaml.yml')
# print(yaml_data)

# Now open the database so
#    1 - Test it exists
#    2 - test it has a "nodes" and 'edges' table

log_msg <- paste("Opening Database: ", graph_data_store)
debug(logger, log_msg)

if (file.exists(graph_data_store)){
  gds <- dbConnect(RSQLite::SQLite(), graph_data_store)
  log_msg <- paste('Connected to databse in ', graph_data_store)
  info(logger, log_msg)
} else {
  log_msg <- paste("Databse ", graph_data_store, " doesnot exist")
  info(logger, log_msg)
  stop('No database to load graph from')
}

# 'gt' means 'graph table' - using this to distinguish the actual graph data from the human readable data
# As this is a temporary file loader we'll just go straight ahead and write the tables, but we'll check for 
# logging purposes

check_gt <- function(gt_name) {
  # Create a master table that tells us which tables in the database are valid
  if(!dbExistsTable(gds,gt_name)){
    log_msg <- paste("Table: ", gt_name, " doesnot exist")
    info(logger, log_msg)
  } else {
    log_msg <- paste("Table: ", gt_name , "is available, creating table")
    info(logger, log_msg)
  }
}

check_gt(graph_nodes)
check_gt(graph_edges)


# now go and grab data we know is working, and then write it to the relevant tables using dpylr functions
# Note that here we are in tidyverse, we are creating tibbles, not data.frames

gpg_nodes <- read_csv('gnodes.csv', col_names=TRUE)
gpg_edges <- read_csv('gedges.csv', col_names=TRUE)
print(gpg_nodes)
print(gpg_edges)

dbWriteTable(gds, graph_nodes, as.data.frame(gpg_nodes), overwrite = TRUE)
dbWriteTable(gds, graph_edges, as.data.frame(gpg_edges), overwrite = TRUE)

# Closing down so tidy everything up
log_msg <- paste("Closing Database ")
info(logger, log_msg)
dbDisconnect(gds)

log_msg <- paste("Load Completed")
info(logger, log_msg)


