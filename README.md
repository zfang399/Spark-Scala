Spark-Scala
==============================

Overload Analysis Project
------------------------------
### Given a csv form data with data entries like:<br />
      date   |  time  |  wind_direction  |  wind_speed  |  precip  |  temper  |  humid  |  pressure  |
    20140225 |  1030  |       88         |     0.5      |    0.6   |    11.2  |    93   |    1022.5  |
      cloud  |  weekday  |  holiday  |  load     |
       N/A   |     3     |     0     |  15503.8  |
### Gives predictions of the main reasons for high load.

Algorithms
------------------------------

### 1. Apriori<br />
    A simple approach to attaining the frequent itemsets.
    Main idea: use the (k-1)th frequent itemset to find kth frequent itemset
    Pros: This algorithm is easy to implement.
    Cons: The combinations derived from (k-1)th frequent itemset are sometimes too large in number. And it 
    needs to go through all the records in the dataset in order to judge whether the present itemset is frequent.
### 2. FP-Growth<br />
    A more advanced approach to attaining the frequent itemsets.
    Main idea: use a structure called frequent pattern tree (FP-tree) to store the records, and retrieve the 
    frequent itemset through the tree.
    Pros: It only has to loop through the whole dataset once. Efficient for large datasets.
    Cons: It is more complicated to implement. 
    
### 3. Decision Tree<br />
    A common algorithm to classify records
    Main idea: use a tree structure to classfy the record step by step
    
### 4. ALS(Alternating Least Square)<br />
    An algorithm of collaborative filtering, frequently used for generating recommendations.
    
### 5. K-means<br />
    A popular algorithm to classify objects into K clusters
    Main-idea: choose K points as the center of the K clusters, allocate all points to the K clusters,
    recalculate the center in each clusters, and repeat the process until there is almost no movement of
    the centers.
    
    
Applications
-------------------------------
### 1. Email Classifier<br />
    An email classifier to detect spam emails from normal ones.
    The model is trained from a training set from spamassassin.apache.org
    Written with python and the functions of spark. 
    
### 2. Curve Smoother<br />
    A program to make the curve/scatter point plot smoother.
    Written with python and the functions of spark.
