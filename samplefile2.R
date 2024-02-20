hola que tal

install.packages("politeness")
install.packages("concreteness")
install.packages("quanteda")
library(politeness)
library(quanteda)
library(concreteness)

Death <- read.csv("Death.csv")  

feature_table

data("Death")
polite.data<-politeness(Death$message, parser="none",drop_blank=FALSE)
findPoliteTexts(Death$message,
                polite.data,
                Death$condition,
                type = "most",
                num_docs = 5)
findPoliteTexts(Death$message,
                polite.data,
                Death$condition,
                type = "least",
                num_docs = 10)

politeness(
  text,
  parser = c("none", "spacy"),
  metric = c("count", "binary", "average"),
  drop_blank = FALSE,
  uk_english = FALSE,
  num_mc_cores = 1
)

write.csv(results, "DeathResultsPolite.csv")  