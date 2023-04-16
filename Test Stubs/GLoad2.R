library('yaml')
library('log4r')
library('tidyverse')
library('DBI')
library(fs)
library(plyr)

#  Start of day stuff goes here


# Set the logger's file output, current level is get all, and then send a startup message
# To-Do - come back here and build a better file name & date stamp
# To-Do - Pull the logfile level out of YAML File

log_filename <- 'Logs/GLoad2.log'
log_loglevel <- 'DEBUG'

if (file.exists(log_filename)){
  file.rename(log_filename, path_ext_set(path_ext_remove(log_filename), "old"))
}
logger <- create.logger()
logfile(logger) <- log_filename
level(logger) <- log_loglevel

levellog(logger, 'INFO', paste("Starting GLoad2 with Logging set to ", log_loglevel))

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

levellog(logger, 'DEBUG', paste("Loading Config File: ", config_file))

yaml_data <- read_yaml('Test-Yaml.yml')
# print(yaml_data)

# The first thing is to check that the database actually exists. Without nothing can happen, so a
# simple bit of logic
levellog(logger, 'INFO', paste("Connecting to Database: ", graph_data_store))
if (file.exists(graph_data_store)){
  gds <- dbConnect(RSQLite::SQLite(), graph_data_store)
  levellog(logger, 'INFO', paste('Connected to databse in ', graph_data_store))
} else {
  levellog(logger, 'FATAL', paste("Databse ", graph_data_store, " doesnot exist"))
  stop('No database to load graph from')
}

# GLoad2 creates a new set of Graph Nodes & Edges, so here we delete any existing tables
#
check_gt <- function(gt_name) {
  if(dbExistsTable(gds,gt_name)){
    levellog(logger, 
             'INFO', 
             paste("Table: ", gt_name, " already exists, replacing data by deleting table"))
    dbRemoveTable(gds, gt_name)
  # To-Do - put in the logic to merge new with old data - dbAppendTable(gds, tbl_name, nodes_data)
  }
}

# GLoad2 loads from the Human readable tables create by HRLoad1. We need to check that
# HRLoad1 has run, so we look for the two HR master tables (Nodes & Edges)
# If either doesn't exist then we have to stop 
check_hrmt <- function(mt_name) {
  # Create a master table that tells us which tables in the database are valid
  if(!dbExistsTable(gds,mt_name)){
    levellog(logger, 'FATAL', paste("Table: ", mt_name, " doesnot exist"))
    stop(log_msg)
  }
}

check_hrmt(master_nodes)
check_hrmt(master_edges)

check_gt(graph_nodes)
dbCreateTable(gds, 
              graph_nodes,
              data.frame(id ='numeric',
                         label ='text', 
                         title = 'text',
                         shape ='text', 
                         group = 'text',
                         ttypes ='text'
                         )
              )
levellog(logger, 'INFO', paste("Table: ", graph_nodes , "has been created"))


check_gt(graph_edges)
dbCreateTable(gds, 
              graph_edges, 
              data.frame(id='numeric',
                         from ='numeric', 
                         to = 'numeric',
                         label ='text'              )
)
levellog(logger, 'INFO', paste("Table: ", graph_edges , "has been created"))

each(dbListTables, print)


# Closing down so tidy everything up

levellog(logger, 'INFO', paste("Closing database", graph_data_store))
dbDisconnect(gds)

log_msg <- paste("Load Completed")
info(logger, log_msg)
