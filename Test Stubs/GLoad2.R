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
cat("Starting Database Files Load to Graph Data - Logfile is ", log_filename, "at level set to:", log_loglevel, fill = TRUE)

# Set up our file names, table names etc
config_file <- "Test-Yaml.yml"
graph_data_store <- "Project-Graph-DB.db"

# table names for the master data
master_nodes <- "HR-nodes"
master_edges <- "HR-edges"
graph_nodes <- "G-nodes"
graph_edges <- "G-edges"

cat("Taking configuration from:", config_file, fill = TRUE)


node_index <- 1
edge_index <- 1

# Time to start work, load up the configuration file
# The config file points to the files containing our Programme components
# To-Do - create a error handler so that YAML config errors get captured and reported in the log file

levellog(logger, 'DEBUG', paste("Loading Config File: ", config_file))

yaml_data <- read_yaml('PGConf.yml')

# Let's see if we have defined a database name in teh YAML configuration file. If not
# then default

if (has_name(yaml_data, 'database')) {
  graph_data_store <- yaml_data$database
  levellog(logger, 'INFO', paste("Database Configuration set to ", graph_data_store))
} else {
  levellog(logger, 'INFO', paste("Database Configuration not set. Using default", graph_data_store))
}

cat("Taking data from database:", graph_data_store, fill = TRUE)


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
  cat("Checking Master Table from HR Files load:", mt_name, fill = TRUE)
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

# now start to build the node data as a tibble
# id, integer, Node ID - passed to the graph
# label, string, Label for the node - shown by visNetwork when rendering the graph
# title, string, Title for the node - shown by visNetwork when doing a mouseover
# shape, string, Shape that the node will take
# group, string, group definition

add_node <- function(nlabel, ntitle, nshape, ngroup ) {
  levellog(logger, 'INFO', paste("Adding Graph Node number", node_index, 
                                 ":", nlabel, 
                                 "in group", ngroup))
  g_nodes <<- add_row(g_nodes,
                    id = node_index, 
                    label = nlabel, 
                    title = ntitle,
                    shape = nshape,
                    group = ngroup)
  node_index <<- node_index + 1

}

#
# find_node - searchs nodes in g_nodes and returns the number of the node
#
find_node <- function(node_label, node_group){
  levellog(logger, 'DEBUG', paste("Looking up Label: ", node_label, " in group: ", node_group))
  gnode_index <- 1
  gnode_result <- filter(as_tibble(g_nodes), label == node_label & group == node_group)
  gnode_index <- as.numeric(gnode_result['id'])
  levellog(logger, 'DEBUG', paste("Found in node: ", gnode_index))
  return(gnode_index)
}

add_edge <- function(efrom, eto, elabel){
  
  levellog(logger, 'INFO', paste("Creating Edge", edge_index,
                                 " named ", elabel,
                                 "from Node ", efrom,
                                 "to node", eto))
    g_edges <<- add_row( g_edges,
                      id = edge_index,
                      from = efrom ,
                      to = eto,
                      label = elabel)
  edge_index <<- edge_index + 1
}


# Create the nodes tibble by creating the master node 

levellog(logger, 'INFO', paste("Creating Master Node number ", node_index, ":", yaml_data$projectname))
g_nodes <- tibble(id = node_index, 
                  label = yaml_data$projectname, 
                  title = yaml_data$projectname,
                  shape = 'square',
                  group = 'Master')
node_index <- node_index + 1


# Similarly create the edges. To start this create a tibble with an edge from node 1 to node 1
# the key here is that as this walks down the edge files, it has to look up the node numbers
#  (From | From-Type)  and   (To | To-Type) in the EDGE-xxxx tables, against (label | group) in the
# g_nodes data frame 

edge_index <- 1
levellog(logger, 'INFO', paste("Creating Starting Edge", edge_index))
g_edges <- tibble( id = edge_index,
                   from = 1 ,
                   to = 1,
                   label = "Primary")
edge_index <- edge_index +1


# now load the next level nodes from the HR-Nodes table, reading:
#            tnames           | tfiles         | ttypes
#            NODE-Outcome     | outcomes.csv   | Outcome
#  itable -> next_table_name  ! orig file.csv  | type originally taken from YAML
#            yet_another      | ano_file.csv   |  --- " ----

hr_nodes <- dbReadTable(gds, master_nodes)


# now walk down each row of the table / data.frame using itable as an index to point to each table 
# to load

for (itable in 1: nrow(hr_nodes)) {
  # create a 2nd level master node pointing to the project name in node #1 
  # this node is the type of the data being loaded 

  # start by creating the edges back for each master node type in hr_nodes
    
  itype <- hr_nodes$ttypes[itable]
  add_node(itype, itype, 'ellipse', itype)
  
  
  from_node <- find_node(hr_nodes$ttypes[itable], hr_nodes$ttypes[itable])
  to_node <- 1
  add_edge(from_node, to_node, "Master")
  
  # so now go and load that table which holds the node detail
  # the key here is that in the table we have two columns:
  #             Name        |   Type     | other stuff special to that type of data
  # idetail ->  Node Name   |  Node type | All thsi stuff gets ignored
  #
  cat("Processing nodes table:", hr_nodes$tnames[itable], fill = TRUE)
  hr_node_detail <- dbReadTable(gds, hr_nodes$tnames[itable])
  for (idetail in 1: nrow(hr_node_detail)) {
    add_node(hr_node_detail$Name[idetail],
             hr_node_detail$Name[idetail],
             'ellipse',
             hr_node_detail$Type[idetail])
    #
    # now check - if the node is linked to a master, then build the edges
    #
    hr_node_cols <- colnames(hr_node_detail)
    if ('Target.Type' %in% hr_node_cols){
      if(hr_node_detail$Target.Type[idetail] == 'Master') {
        cat("Target Type of Master - " )
        from_node <- find_node(hr_node_detail$Name[idetail], hr_node_detail$Type[idetail])
        to_node <- find_node(hr_node_detail$Type[idetail], hr_node_detail$Type[idetail])
        cat("from node", from_node, "to:", to_node, fill=TRUE)
        add_edge(from_node, to_node, "Key Part")
      } 
    } else {
      cat(" No Target Type - ")
      from_node <- find_node(hr_node_detail$Name[idetail], hr_node_detail$Type[idetail])
      to_node <- find_node(hr_node_detail$Type[idetail], hr_node_detail$Type[idetail])
      cat("from node:", from_node, " - to - ", to_node, fill=TRUE)
      add_edge(from_node, to_node, "Key Part")
    }

  }
  
}


# Now walk down the just loaded nodes and create the edges back to project name
# Open HR-edges - for every entry create the next level edge from the node back to the master
#
# Then for each EDGE-xxxxx file, work down it linking 


#
# Now walk down each set of data in HR-Edges and create the edges needed
for (iedge in 1:nrow(hr_edges)) {
  cat("Processing edges table:", hr_edges$tnames[iedge], fill = TRUE)
  hr_edge_detail <- dbReadTable(gds, hr_edges$tnames[iedge])
  for (idetail in 1: nrow(hr_edge_detail)) {
    from_node <- find_node(hr_edge_detail$From[idetail], hr_edge_detail$From.Type[idetail])
    to_node <- find_node(hr_edge_detail$To[idetail], hr_edge_detail$To.Type[idetail])
    add_edge(from_node, to_node, "Defined")
  }
}


cat("Updating data into database:", graph_data_store, fill = TRUE)
# Now write out the tables
levellog(logger, 'INFO', paste("Writing:", nrow(g_nodes), "rows to table: ", graph_nodes))
dbWriteTable(gds, graph_nodes, g_nodes, overwrite = TRUE )

levellog(logger, 'INFO', paste("Writing:", nrow(g_edges), "rows to table", graph_edges))
dbWriteTable(gds, graph_edges, g_edges, overwrite = TRUE)



# Closing down so tidy everything up

levellog(logger, 'INFO', paste("Closing database", graph_data_store))
dbDisconnect(gds)

log_msg <- paste("Load Completed")
info(logger, log_msg)
