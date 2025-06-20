The codes are relevant to :
1. Notebooks :
     Redis: It is about connectivty from dataricks to azure redis cache  using SPN, Here trying to read the data from Azure redis cache,reading the keys and then creating correspondingg json files for each key into ADLS.
     CosmosDB: It is about reading data from ADLS, using transformations using databricks pyspark,then dumping the transformed data into ADLS and Azure cosmosDB. Then connectivity in between cosmos db and ADLS using MSI.
     External Location: Implementing Unity Catalogued External location creation using databricks SDK.
2. ADF:
   Redis : The Redis notebook being called here using ADF pipeline as part of our regular daily ingestion process.Although there are three more ADF pipelines on top of it, But not mention as those are templates we generally follow for all of the ingestion objects.       
   CosmosDB:Same as above the Redis Onc 
4. Scripts:
5.   cosmosDB:      
