library("igraph")
library("data.table")
library("reshape2")
library("clickstream")

train_file = "/home/souhagaa/Bureau/test/server/UX/UX/data/interm/identified/sessions_user_3.csv"
# test_file = "/home/souhagaa/Bureau/test/session_identification/test_set_no_duplicate.csv"

fitting_model <- function(train_file) {
  cls <- readClickstreams(file = train_file, sep = ",", header = FALSE)
  # clusters <- clusterClickstreams(clickstreamList = cls, order = 1, centers = 3)
  mc <- fitMarkovChain(clickstreamList = cls, order = 1, control = list(optimizer = "quadratic"))
  return(mc)
}

mc <- fitting_model(train_file)
# new data
pattern <- new("Pattern", sequence = c("P128"))
resultPattern <- predict(mc, startPattern = pattern, dist = 1)
resultPattern
saveRDS(mc, "./test_model.rds")
