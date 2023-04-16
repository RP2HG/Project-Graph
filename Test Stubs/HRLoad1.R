library('yaml')
library('log4r')
library('tidyverse')
library('DBI')

#  Start of day stuff goes here


# Set the logger's file output, current level is get all, and then send a startup message
# To-Do - come back here and build a better file name & date stamp
# To-Do - Pull the logfile level out of YAML File

log_filename <- 'Logs/HRLoad1.log'
log_loglevel <- 'DEBUG'

if (file.exists(log_filename)){
  file.rename(log_filename, paste(log_filename, ".old", sep=""))
}
logger <- create.logger()
logfile(logger) <- log_filename
level(logger) <- log_loglevel

log_msg <- paste("Starting HRLoad1 with Logging set to ", log_loglevel)
info(logger, log_msg)

# Set up our file names, table names etc
config_file <- "Test-Yaml.yml"
graph_data_store <- "Project-Graph-DB.db"

# table names for the master data
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

# Now create an empty SQLite database file where we will store the various 'artefacts'
# Tables are NODE-{type} where type is from the file being uploaded, or alternatively EDGE-{type}
# The reason I use a SQLite database is (a) to hold the graph data in a persistent single store, and (b)
# so I can add the node & edge numbers when I create the graph

log_msg <- paste("Creating Database: ", graph_data_store)
debug(logger, log_msg)
gds <- dbConnect(RSQLite::SQLite(), graph_data_store)

create_mt <- function(mt_name) {
  # Create a master table that tells us which tables in the database are valid
  if(dbExistsTable(gds,mt_name)){
  log_msg <- paste("Table: ", mt_name, " already exists, replacing data")
  info(logger, log_msg)
  dbRemoveTable(gds, mt_name)
  # To-Do - put in the logic to merge new with old data - dbAppendTable(gds, tbl_name, nodes_data)
} else {
  log_msg <- paste("Table: ", mt_name , "is new, creating table")
  info(logger, log_msg)
}

# This is where I create a mastare table so create columns for 
#   1 - Table name 
#   2 - Filename I loaded it form - we'll need this when we write it out again
#   3 - Type of data in that file eg Plan of Record etc
dbCreateTable(gds, mt_name, data.frame(tnames='text',tfiles='text', ttypes = 'text'))

log_msg <- paste("Created Master Table", mt_name)
info(logger, log_msg)
} 

create_mt(master_nodes)
create_mt(master_edges)


log_msg <- paste("No of node files to process", length(yaml_data$nodes))
info(logger, log_msg)
log_msg <- paste("No of edge files to process", length(yaml_data$edges))
info(logger, log_msg)

# generic add - mt_type will be either master_nodes or master_edges ie the master table where the node tables are 
# of the master table where the edge tables are
# Similarly table_name is the actual table that has just been created so outcomes, of plan of record etc

add_to_master_list <- function(mt_type, table_name, table_file, table_type){
  dbAppendTable(gds, mt_type, data.frame(tnames = table_name, tfiles = table_file, ttypes = table_type), row.names = NULL)
  log_msg <- paste("Added ", table_name, " to Master Nodes Table: ", mt_type)
  info(logger, log_msg)
  
}


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
  log_msg <- paste("Rows created in Table: ", tbl_name)
  info(logger, log_msg)
  add_to_master_list(master_nodes, tbl_name, nodes_file, nodes_type)
  }

# walk through loading the edge files into DB tables
for (i in 1:length(yaml_data$edges)){
  # grab the appropriate bits of the YAML entry
  edges_file <- yaml_data$edges[[i]]$name
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
  log_msg <- paste("Rows created in Table: ", tbl_name)
  info(logger, log_msg)
  add_to_master_list(master_edges, tbl_name, edges_file, edges_type)
}


dbListTables(gds)

# Closing down so tidy everything up
log_msg <- paste("Closing Database ")
info(logger, log_msg)
dbDisconnect(gds)

log_msg <- paste("Load Completed")
info(logger, log_msg)
