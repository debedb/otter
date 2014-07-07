001. Create a new dataset
002. Get its metadata (using ID of created dataset returned in previous call)
003. Load data into dataset
004. Run a query.
005. Get sample set of rows from dataset (50 row count)
006. Load more data into dataset
007. Run a query (count should be 3 times as much as in 004)
008. Delete data from dataset
009. Load data into dataset
010. Run a query (count should be same as in 004). This also saves
     query result and returns Id of dataset.
011. Get data from dataset created in 010.
012. Get metadata from dataset created in 010.