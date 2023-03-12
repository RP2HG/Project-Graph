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
graph_data_store <- "Project-Graph-DB.db"

# Time to start work, load up the configuration file
# The config file points to the files containing our Programme components
# To-Do - create a error handler so that YAML config errors get captured and reported in the log file

log_msg <- paste("Loading Config File: ", config_file)
debug(logger, log_msg)
yaml_data <- read_yaml('Test-Yaml.yml')
# print(yaml_data)

# Now create an empty SQLite database file where we will store the various 'artefacts'
# Tables are NODE-{type} where type is from the file being uploaded, or alternatively EDGE-{type}
# The reason I use a SQLite database is (a) to hold the graph data in a persistent single store, and (b)
# so I can add the node & edge numbers when I create the graph

log_msg <- paste("Creating Databse: ", graph_data_store)
debug(logger, log_msg)

gds <- dbConnect(RSQLite::SQLite(), graph_data_store)

log_msg <- paste("No of node files to process", length(yaml_data$nodes))
info(logger, log_msg)
log_msg <- paste("No of edge files to process", length(yaml_data$edges))
info(logger, log_msg)

# walk through loading the node files into DB tables
for (i in 1:length(yaml_data$nodes)){
  # grab the appropriate bits of the YAML entry
  nodes_file <- yaml_data$nodes[[i]]$name
  nodes_type <- yaml_data$nodes[[i]]$nodetype
  tbl_name <- paste("NODE-", nodes_type, sep='' )
  log_msg <- paste("Processing file ", nodes_file, " to DB table ", tbl_name )
  info(logger, log_msg)

  # now do it
  nodes_data <- read_csv(
    nodes_file
    )
  
  if(dbExistsTable(gds,tbl_name)){
    log_msg <- paste("Table: ", tbl_name , "already exists, replacing data")
    info(logger, log_msg)
    # To-Do - put in the logic to merge new with old data - dbAppendTable(gds, tbl_name, nodes_data)
    } else {
      log_msg <- paste("Table: ", tbl_name , "is new, creating table")
      info(logger, log_msg)
    }
  dbWriteTable(gds, tbl_name, nodes_data, overwrite = TRUE)
  
  log_msg <- paste( rows_written, "Rows created in Table: ", tbl_name)
  info(logger, log_msg)
  }

# walk through loading the edge files into DB tables
for (i in 1:length(yaml_data$edges)){
  # grab the appropriate bits of the YAML entry
  edges_file <- yaml_data$edges[[i]]$name
  # To-Do - Cahnge the YAML to say "edgetype"
  edges_type <- yaml_data$edges[[i]]$edgetype
  tbl_name <- paste("EDGE-", edges_type, sep='' )
  log_msg <- paste("Processing file ", edges_file, " to DB table ", tbl_name )
  info(logger, log_msg)
  
  # now do it
  edges_data <- read_csv(
    edges_file
  )
  
  if(dbExistsTable(gds,tbl_name)){
    log_msg <- paste("Table: ", tbl_name , "already exists, replacing data")
    info(logger, log_msg)
    # To-Do - put in the logic to merge new with old data - dbAppendTable(gds, tbl_name, nodes_data)
  } else {
    log_msg <- paste("Table: ", tbl_name , "is new, creating table")
    info(logger, log_msg)
  }
  dbWriteTable(gds, tbl_name, edges_data, overwrite = TRUE)
  
  log_msg <- paste( rows_written, "Rows created in Table: ", tbl_name)
  info(logger, log_msg)
}


dbListTables(gds)

askYesNo("Finish")

# Closing down so tidy everything up
log_msg <- paste("Closing Database ")
info(logger, log_msg)
dbDisconnect(gds)

log_msg <- paste("Load Completed")
info(logger, log_msg)
