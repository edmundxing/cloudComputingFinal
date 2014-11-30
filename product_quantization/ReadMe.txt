The PQ algorithm is implemented in java language with mapreduce.
1) Cartesian k-means
   Two versions are implemented:CartesianKmeans.java and Cartkmeansmem.java. Both are tested under EMR platform
2) build code book
   BuildCodeBook.java. This is fully tested on EMR
3) evaluation
   EvaluateSearch.java. This is fully tested on EMR
Other functions:
TestPQSearchBatch.java; This only runs on local node
PQSearch.java: This is called by above mapreduce functions