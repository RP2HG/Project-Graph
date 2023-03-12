library('yaml')
library('log4r')
library('tidyverse')
library('DBI')

#  Start of day stuff goes here


# Set the logger's file outpu, current lkevel is get all, and then send a startup message
# To-Do - come back here and build a better filename & datestamp
# To-Do - Pull the logfile level out of YAML File

log_filename <- 'PGLoad.log'
log_loglevel <- 'DEBUG'

if (file.exists(log_filename)){
  file.rename(log_filename, "oldlogfile")
}
logger <- create.logger()
logfile(logger) <- log_filename
level(logger) <- log_loglevel

log_msg <- paste("Starting Load with Logging set to ", log_loglevel)
info(logger, log_msg)

# info(logger, 'An Info Message')
# warn(logger, 'A Warning Message')
# error(logger, 'An Error Message')
# fatal(logger, 'A Fatal Error Message')
# End of logfile start up

# Set up our filenames

config_file <- "Test-Yaml.yml"
graph_data_store <- "Project-Graph-DB.sql"

# Time to start work, load up the configuration file
# The config file points to the files containing our Programme components
# To-Do - create a error handler so that YAML config errors get captured and reported in the log file

log_msg <- paste("Loading Config File: ", config_file)
debug(logger, log_msg)
yaml_data <- read_yaml('Test-Yaml.yml')

# Now create an empty SQLite database file where we will store the various 'artefacts'
# Tables are NODE-{type} where type is from the file being uploaded, or alternatively EDGE-{type}
# The reason I use a SQLite database is (a) to hold the graph data in a persistent single store, and (b)
# so I can add the node & edge numbers when I create the graph

log_msg <- paste("Creating Databse: ", graph_data_store)
debug(logger, log_msg)

gds <- dbConnect(RSQLite::SQLite(), graph_data_store)

# test here - load a CSV file and post into the database
j <- read_csv(
  yaml_data$nodes[[1]]$name
)

print(j)

dbWriteTable(gds, "mtcars", j)
dbListTables(gds)



fatal(logger, "Err Nerr")

print(yaml_data)
askYesNo("Finish")

# Closing down so tidy everything up
log_msg <- paste("Closing Database ")
info(logger, log_msg)
dbDisconnect(gds)

log_msg <- paste("Load Completed")
info(logger, log_msg)
