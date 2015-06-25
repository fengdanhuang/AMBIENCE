# AMBIENCE
Hadoop MapReduce implementation of AMBIENCE algorithm

1. AMBIENCE algorithm is an algorithm for identifying the informative variables involved in gene–gene (GGI) and gene–environment interactions (GEI) that are associated with disease
phenotypes. It uses  a  novel  information  theoretic  metric  called  phenotype-associated information (PAI) to search for combinations of genetic variants and environmental variables
associated with the disease phenotype. The paper describing AMBIENCE algoritm could be found by the link:
http://www.genetics.org/content/180/2/1191.abstract

2. This project implemented AMBIENCE algoritm in Hadoop MapReduce, in order to execute the program in a distruted way to process
large datasets.

3. Input sample file: testSet_context.txt
    
4. Output sample files: o1\part-r-00000 (result for 1st order); o2\part-r-00000(result for 2nd order) ; o3\part-r-00000(result for 3rd order)
   Intermediate output sample files: o1_job-a\part-r-00000, o1_job-b\part-r-00000;
                                     o2_job-a\part-r-00000, o2_job-b\part-r-00000;
                                     o3_job-a\part-r-00000, o3_job-b\part-r-00000.
