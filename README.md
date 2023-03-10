# MongoDB Online Archival 

Python-based PoV to showcase the archiving of MongoDB data into Amazon S3 bucket, (each document gets stored in a separate parquet file), this will hold the capability to run as a scheduled task in the client's environment. 

Furthermore, reading of the data will be made possible using Atlas Data Federated Queries. 

## Architechture
![image](https://user-images.githubusercontent.com/114057324/224238287-5778fe25-ed40-4a70-a8bf-70b3e085f624.png)


## Steps to follow

- Clone the repository

  ```git clone https://github.com/utsavMongoDB/online-archival.git``` 
  
- Setup data federation : https://www.mongodb.com/docs/atlas/data-federation/deployment/deploy-s3/
- Update the credentials in ```config.py```

- Move inside Online Archival folder and run ```main.py``` file 
  
  ```cd online-archival```

  ```python main.py```

 
 This application will run indefinitely, the data is archived in every 5 minutes interval (configurable).  



## Archival data view

Each document is stored a seperate Parquet file. 

<img width="1064" alt="image" src="https://user-images.githubusercontent.com/114057324/224242040-1dfcfec3-c78b-46b5-835b-16180e121941.png">


## Conclusion

Setup for online archival completed.
