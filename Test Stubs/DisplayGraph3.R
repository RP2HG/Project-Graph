require(shiny)
library('yaml')
library('log4r')
library('tidyverse')
library('DBI')
library(visNetwork)
library(fs)

# Core display function here - goes to the database, extracts the node & edge data
# from tables G-nodes & G-edges, then pushes it into visNetwork


#  Start of day stuff goes here


# Set the logger's file output, current level is get all, and then send a startup message
# To-Do - come back here and build a better file name & date stamp
# To-Do - Pull the log file level out of YAML File

log_filename <- 'Logs/DisplayGraph3.log'
log_loglevel <- 'DEBUG'

if (file.exists(log_filename)){
  file.rename(log_filename, path_ext_set(path_ext_remove(log_filename), "old"))
}
logger <- create.logger()
logfile(logger) <- log_filename
level(logger) <- log_loglevel

log_msg <- paste("Starting Display with Logging set to ", log_loglevel)
info(logger, log_msg)


# Set up our file names, table names etc

config_file <- "Test-Yaml.yml"
graph_data_store <- "Project-Graph-DB.db"

# table names for the master data. HR = Human readable, G = Graph
# master_nodes: tnames, tfiles, ttypes (needs to be 'group')
# master_edges: tnames, tfiles, ttypes (needs to be 'group')
# graph_nodes: id, label, title, shape, group
#   node table = 'NODE-'{group}
# graph_edges: id, from {node id from grpah nodes}, to
#   edge table = 'EDGE-'{group}
# cross_group_edges: from {node_lable}, from_node_group, to {node_lable}, to_node_group

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

check_gt <- function(gt_name) {
  # Create a master table that tells us which tables in the database are valid
  if(!dbExistsTable(gds,gt_name)){
    log_msg <- paste("Table: ", gt_name, " doesnot exist")
    info(logger, log_msg)
    stop(log_msg)
  } else {
  log_msg <- paste("Table: ", gt_name , "is available, loading data from table")
  info(logger, log_msg)
  }
}

check_gt(graph_nodes)
check_gt(graph_edges)


# askYesNo("Finish")

# Closing down so tidy everything up
# log_msg <- paste("Closing Database ")
# info(logger, log_msg)
# dbDisconnect(gds)

# log_msg <- paste("Load Completed")
# info(logger, log_msg)


# build elementary graph with headers

gpg_nodes <- dbReadTable(gds, graph_nodes)
gpg_edges <- dbReadTable(gds, graph_edges)
print(gpg_nodes)
print(gpg_edges)

log_msg <- paste("Starting Shiny Server")
info(logger, log_msg)

server <- function(input, output) {
  output$mynetworkid <- renderVisNetwork({
    # minimal example
    visNetwork(nodes = as.data.frame(gpg_nodes), edges = as.data.frame(gpg_edges))
  })
}

ui <- fluidPage(
  visNetworkOutput("mynetworkid")
)

shinyApp(ui = ui, server = server)

