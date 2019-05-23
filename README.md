# MapReduce_Apriori_implementation
This project is an implementation of the Apriori algorithm for Mining frequent Itemsets. It makes use of Hadoop for the MapReduce functions.
# Running the code
Use the following command.
$ hadoop jar mr.jar MRMiner MINSUPP CORR TRANS_PER_BLOCK PATH_TO_INPUT
PATH_TO_FIRST_OUT PATH_TO_FINAL_
Where:
• MINSUPP, an integer, is the minimum support threshold;
• CORR, an integer, is the “correction” to the minimum support threshold: 
the  first Map function will mine the set of transactions it receives with a minimum support threshold of MINSUPP-CORR.
• TRANS_PER_BLOCK, an integer, is the number of transactions per “block” of the dataset, which are simultaneously given 
in input to the  first Map function(see below, where implementa- tion details are discussed).
• PATH_TO_INPUT is the path to the HDFS input directory containing the input dataset-a .txt file (format described below );
• PATH_TO_FIRST_OUT is the path to the HDFS output directory for the  first round;
• PATH_TO_FINAL_OUT is the path to the HDFS  final output directory (format described below).

# Input and Output Formats  
The input dataset is a plaintext  file containing one transaction per line. Each transaction is a sequence of non-negative integers separated by a white space, such as
11 8 9 20
The output should contain the frequent itemsets in the input dataset with support at least MINSUPP, one frequent itemset per line with items separated by a space, 
and followed (on the same line) by the support of the itemset. For example
20 8 204532
would denote that the itemset {20, 8} has support 204532.
