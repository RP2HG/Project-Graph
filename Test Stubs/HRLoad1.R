library('yaml')
library('log4r')
library('tidyverse')
library('DBI')
library(fs)

#  File   HRLoad1.R      - Human Readable Load    Stage 1
#         =========        ===================    =======
#
#  Overview -  Take our Systme of System input files in CSV format, and load them into a SQL database


# Set up the logger with a filename based on the source file name. Sert default level,
# then send an initail start up message. For safety, if a log file exists from a previous run
# then move the file to xxx.old so we have comparative results if we need them

log_filename <- 'Logs/HRLoad1.log'
log_loglevel <- 'DEBUG'

if (file.exists(log_filename)){
  file.rename(log_filename, path_ext_set(path_ext_remove(log_filename), "old"))
}
logger <- create.logger()
logfile(logger) <- log_filename
level(logger) <- log_loglevel

levellog(logger, 'INFO', paste("Starting HRLoad1 with Logging set to ", log_loglevel))
cat("Starting Human Readable Files Load - Logfile is ", log_filename, "at level set to:", log_loglevel, fill = TRUE)

# Set up our default file names, table names etc
config_file <- "PGConf.yml"
graph_data_store <- "Project-Graph-DB.db"

cat("Taking configuration from:", config_file, fill = TRUE)


# Set up default table names for the master data
master_nodes <- "HR-nodes"
master_edges <- "HR-edges"
graph_nodes <- "G-nodes"
graph_edges <- "G-edges"


# Time to start work, load up the configuration file
# The config file points to the files containing our Programme components
# To-Do - create a error handler so that YAML config errors get captured and reported in the log file

levellog(logger, 'INFO', paste("Loading Config File: ", config_file))
yaml_data <- read_yaml(config_file)

# Let's see if we have defined a database name in the YAML configuration file. If not
# then default

if (has_name(yaml_data, 'database')) {
  graph_data_store <- yaml_data$database
  levellog(logger, 'INFO', paste("Database Configuration set to ", graph_data_store))
} else {
  levellog(logger, 'INFO', paste("Database Configuration not set. Using default", graph_data_store))
}


# Let's see if we have defined a database name in the YAML configuration file. If not
# then default

if (has_name(yaml_data, 'database')) {
  graph_data_store <- yaml_data$database
  levellog(logger, 'INFO', paste("Database Configuration set to ", graph_data_store))
} else {
  levellog(logger, 'INFO', paste("Database Configuration not set. Using default", graph_data_store))
}


# Now create an empty SQLite database file where we will store the various 'artefacts'
# Tables are NODE-{type} where type is from the file being uploaded, or alternatively EDGE-{type}
# The reason I use a SQLite database is (a) to hold the graph data in a persistent single store, and (b)
# so I can add the node & edge numbers when I create the graph

levellog(logger, 'INFO', paste("Creating Database: ", graph_data_store))
gds <- dbConnect(RSQLite::SQLite(), graph_data_store)

create_mt <- function(mt_name) {
  # Create a master table that tells us which tables in the database are valid
  if(dbExistsTable(gds,mt_name)){
    levellog(logger, 'INFO', paste("Table: ", mt_name, " already exists, replacing data"))
    dbRemoveTable(gds, mt_name)
    # To-Do - put in the logic to merge new with old data - dbAppendTable(gds, tbl_name, nodes_data)
  } else {
    levellog(logger, 'INFO', paste("Table: ", mt_name , "is new, creating table"))
  }

  # This is where I create a master table so create columns for 
  #   1 - Table name 
  #   2 - Filename I loaded it form - we'll need this when we write it out again
  #   3 - Type of data in that file eg Plan of Record etc
  dbCreateTable(gds, mt_name, data.frame(tnames='text',tfiles='text', ttypes = 'text'))
  levellog(logger, 'INFO', paste("Created Master Table", mt_name))
}

create_mt(master_nodes)
create_mt(master_edges)

levellog(logger, 'INFO', paste("No of node files to process", length(yaml_data$nodes)))
levellog(logger, 'INFO', paste("No of edge files to process", length(yaml_data$edges)))

# generic add - mt_type will be either master_nodes or master_edges ie the master table where the node tables are 
# of the master table where the edge tables are
# Similarly table_name is the actual table that has just been created so outcomes, of plan of record etc

add_to_master_list <- function(mt_type, table_name, table_file, table_type){
  dbAppendTable(gds, mt_type, data.frame(tnames = table_name, tfiles = table_file, ttypes = table_type), row.names = NULL)
  levellog(logger, 'INFO', paste("Added ", table_name, " to Master Nodes Table: ", mt_type))
}

#
# walk through the list of node files in the YAML configuration file, loading each file into a 
# database table called "NODE-xxxxxxx" where xxxxx is the type of the data defined in the YAML
#
# Assumption here is that CSV file has "Name" | "Type" as first two columns, we can check that later
# 
# for now we assume that each file to be loaded is unique to the type of data being loaded
# 
for (i in 1:length(yaml_data$nodes)){
  # grab the appropriate bits of the YAML entry
  nodes_file <- yaml_data$nodes[[i]]$name
  cat("Processing Node File: ", nodes_file, fill = TRUE)
  nodes_type <- yaml_data$nodes[[i]]$nodetype
  tbl_name <- paste("NODE-", nodes_type, sep='' )
  levellog(logger, 'INFO', paste("Processing file:", nodes_file, " into database table:", tbl_name ))

  # now do it by reading the relevant file
  nodes_data <- read.csv(nodes_file)

  if(dbExistsTable(gds,tbl_name)){
    levellog(logger, 'INFO', paste("Table: ", tbl_name , "already exists, replacing data"))
    # To-Do - put in the logic to merge new with old data - dbAppendTable(gds, tbl_name, nodes_data)
    } else {
      levellog(logger, 'INFO', paste("Table: ", tbl_name , "is new, creating table"))
    }
  dbWriteTable(gds, tbl_name, nodes_data, overwrite = TRUE)
  levellog(logger, 'INFO', paste( nrow(nodes_data), " rows created in Table:", tbl_name))
  add_to_master_list(master_nodes, tbl_name, nodes_file, nodes_type)
}

#
# Now we load the edges, for now we assume that the edges file has four columns for now
#       From    |    From-Type     A    To    |    To-Type
#
# walk through loading the edge files into DB tables
#
for (i in 1:length(yaml_data$edges)){
  # grab the appropriate bits of the YAML entry
  edges_file <- yaml_data$edges[[i]]$name
  cat("Processing Edge File: ", edges_file, fill = TRUE)
  edges_type <- yaml_data$edges[[i]]$edgetype
  tbl_name <- paste("EDGE-", edges_type, sep='' )
  levellog(logger, 'INFO', paste("Processing file ", edges_file, " to DB table ", tbl_name ))

  # now do it
  edges_data <- read.csv(
    edges_file
  )
  
  if(dbExistsTable(gds,tbl_name)){
    levellog(logger, 'INFO', paste("Table: ", tbl_name , "already exists, replacing data"))
    # To-Do - put in the logic to merge new with old data - dbAppendTable(gds, tbl_name, nodes_data)
  } else {
    levellog(logger, 'INFO', paste("Table: ", tbl_name , "is new, creating table"))
  }
  dbWriteTable(gds, tbl_name, edges_data, overwrite = TRUE)
  levellog(logger, 'INFO', paste("Rows created in Table: ", tbl_name))
  add_to_master_list(master_edges, tbl_name, edges_file, edges_type)
}



# Closing down so tidy everything up
cat("Files loaded - saving to database: ", graph_data_store, fill = TRUE)
levellog(logger, 'INFO', paste("Closing Database "))
dbDisconnect(gds)

levellog(logger, 'INFO', paste("Load Completed"))
cat("Processing complete ", fill = TRUE)

