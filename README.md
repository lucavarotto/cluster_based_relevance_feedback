# cluster_based_relevance_feedback

In June 2024, a friend and I developed a superior alternative to BM-25 using a clustering approach with a k-NN algorithm. The effectiveness of this method in improving MAP was confirmed through both a paired t-test and the Wilcoxon log-rank test.

This project was part of the Information Retrieval exam. Our approach was based on the paper by Lee, K. S., Croft, W. B., & Allan, J. (2008). We can summarize it in seven steps:
1. Initial retrieval on the whole collection.
2. Clustering.
3. Identification of the "dominant document" based on the clustering.
4. Aggregation of documents in the clusters of the "dominant document."
5. Retrieval on the aggregated clusters.
6. Query expansion based on the first result (pseudo-RF).
7. Second retrieval on the whole collection.

We then applied this approach on an experimental collection to optimize its hyperparameters and test its effectiveness.

We have uploaded the following files:
- **bulk.py** and **bulk_without_stopwords.py**: Used to index the TREC collection ROBUST 2004, both with and without stopwords.
- **cluster_pseudo_RF.py**: Contains the code for the pseudo RF described above.
- **search.py**: Used to get the results of the queries on ROBUST 2004 and to perform a grid search to optimize some parameters.
- **Quartuccio_Varotto.pdf**: Contains the presentation my friend and I used to present our project.
