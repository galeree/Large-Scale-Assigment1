# Novel Clustering
This is a novel clustering script using Hadoop written in Python and Java

# Folder
- mapreduce Contains map and reduce code written in Python.
- Remote    Contains java code used to execute Python map-reduce file.

# Map reduce step in each mapreduce#.py
1. Word Count for each file
2. Finding top 100 words in each file
3. Summation of all word counts
4. Finding top 100 words among all files
5. Features of file (Feature for K-Mean) Mapper -> One word is one feature
   finding word count for one feature.
6. Features of file (Feature for K-Mean) Reducer -> One word is one feature
   finding word count for one feature.
7. Clustering with K-Mean
