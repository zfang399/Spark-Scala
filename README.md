Spark-Scala
==============================

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
    
Applications
-------------------------------
### 1. Email Classifier<br />
    An email classifier to detect spam emails from normal ones.
    The model is trained from a training set from spamassassin.apache.org
    Written with python and the functions of spark. 
    
### 2. Curve Smoother<br />
    A program to make the curve/scatter point plot smoother.
    Written with python and the functions of spark.
