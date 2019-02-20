# PrIMe Data Warehouse API

What is the PrIMe Data Warehouse?
============
* PrIMe Data Warehouse is a central repository for archiving chemical kinetic data relevant to the development of predictive models
* Data spans across gasous and solid-fuels  
* Records include associated measurement uncertainties

How does the API work? 
============
The Warehouse API is a python interface of methods and objects that communicates with the Data Warehouse. The API constructs an Elasticsearch query which is passed and executed on the PrIMe Data Warehouse. Query results are returned to the user.


Methods
============
```
search
getXml
getFile
getCount
exist
getList
getProperty
getPropertyNames
getJSON
getBoundsFromOptVar
getModelBounds
getTarget
```

Example 
============
Can be found in `examples.ipynb`
