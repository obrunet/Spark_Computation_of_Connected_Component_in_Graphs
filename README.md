# Computation of Connected Component in Graphs with Spark  

Implementation of the "CCF: Fast and Scalable Connected Component Computation in MapReduce" paper with Spark. Study of its scalability on several datasets using various clusters' sizes on Databricks and Google Cloud Platform (GCP)

TODO:
- EDA data exploration
- barplot in 3D with export in html
- several datasets
- databricks run time
- time data loading & processing
- use graphx of spark
-  both RDD and DataFrames
- Python implementations must be provided
- comparing the RDD and DataFrame versions conducted on graphs of increasing size
- For small graphs use Databricks, for bigger ones use the cluster
- commande spark-submit

## Table of content
- [Abstract]()
- [Description of the CCF algorithm]()
- [Spark Implementation]()
- [Scalability Analysis]()
- [Conclusion]()
- [Appendix]()
- [References]

--- 
# Abstract

# Description of the CCF algorithm
a description of the adopted solution 4 points

# Spark Implementation
designed algorithms plus related global comments/description 4 points; comments to main fragments of code 4 points


# Scalability Analysis

## Datasets

Name	Type	Nodes	Edges	Description
web-BerkStan	Directed	685,230	7,600,595	Web graph of Berkeley and Stanford
web-Google	Directed	875,713	5,105,039	Web graph from Google
web-NotreDame	Directed	325,729	1,497,134	Web graph of Notre Dame
web-Stanford	Directed	281,903	2,312,497	Web graph of Stanford.edu

Stanford web graph
Dataset information
Nodes represent pages from Stanford University (stanford.edu) and directed edges represent hyperlinks between them. The data was collected in 2002.

Berkeley-Stanford web graph
Dataset information
Nodes represent pages from berkely.edu and stanford.edu domains and directed edges represent hyperlinks between them. The data was collected in 2002.

Note Dame web graph
Dataset information
Nodes represent pages from University of Notre Dame (domain nd.edu) and directed edges represent hyperlinks between them. The data was collected in 1999 by Albert, Jeong and Barabasi.

Google web graph
Dataset information
Nodes represent web pages and directed edges represent hyperlinks between them. The data was released in 2002 by Google as a part of Google Programming Contest.

experimental analysis, concerning in particular scalability 3 points

# Conclusion
comments about the experimental analysis outlining weak and strong points of the algorithms. 3 points

# Appendix
including all the code code. 2 points
code + interactive graph

# References
Paper
- https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf

Datasets
- https://snap.stanford.edu/data/web-Stanford.html
- https://snap.stanford.edu/data/web-BerkStan.html
