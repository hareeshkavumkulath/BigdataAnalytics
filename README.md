# 311-Service-Request Data Analysis using Apache Spark - SOEN 691 Project

## Abstract
Recent advances in the field of Big Data Analytics and Machine Learning have introduced a plethora of open-source tools and technologies for both, 
academia and the growing data analyst community. In this project we try leveraging one such popular distributed data processing framework Apache Spark
to analyse 311 - Service Request Data for the city of New-York. Being updated almost on a daily basis for the last 10 years, massive size of this dataset makes it a suitable candidate for analysis using a distributed data processing framework like Spark. Making use of Spark Ecosystem libraries like Spark SQL and Spark ML, on this dataset, enables us to derive some interesting insights, which might drive better resource planning within the city. Identifying the 3 primary goals for this project we first try answering a few statistical questions like “*most frequent complaints reported(across entire city/borough wise)*”, “*Average time to resolve the request (category/department wise)*”, “*mostly used source for making request(borough wise)*” and “*most busy days/months in terms of request volumes*”.
Arriving at these statistical figures involve making extensive use of Spark SQLs Dataframe API. Secondly we generate a model for predicting the closure time for any new incoming service request, after comparing performance of a set of selected supervised learning algorithms available in Spark ML. As part of our last goal we would be applying K-Means clustering over a selected set of features dividing the dataset into clusters to further analyse them for identifying any underlying service request patterns.


###### Keywords — Spark SQL, Spark ML, Supervised Learning, Clustering.

## Introduction

#### Context
311 in North America, is the non-emergency public hotline that citizens use for requesting services in regard to the basic municipal or infrastructural issue they face on a day-to-day basis. Given the current rise in data driven decision making practices, this service request data getting accumulated on a daily basis, can eventually prove out to be an incredibly valuable data source for better urban planning. Identifying the general trends in this data would help the authorities identify underlying patterns in the way citizens make requests and make use of it to address issues more proactively.

#### Objective and Problem presentation
With an overall motivation to offer a small subset of functionalities, a typical decision support tool would provide urban planners and policy makers; we are presenting two primary elements in our project as follows:

1. We first attempt to present some statistical insights which would allow urban policy makers to better plan their resources. For instance:
	* “City wide and Borough wise distribution of most frequently reported complaints”, would help the authorities identify some recurring issues in specific neighbourhoods.
	* “Most busy days/months in terms of request volumes”, would help the authorities to regulate and plan their resource ahead of time by identifying those specific days or months in a year where they have been receiving higher volume of calls.
	* We would end the analysis by trying to run a clustering algorithm over a selected set of features(latitude and longitude) and analysing the resulting clusters for any specific patterns in neighbourhood based for a specific complaint type.

2. Further we try building a predictive model with an ability to predict the closure time for a particular request. This provides a way for the policy makers to closely scrutinize the operations of the concerned department, thereby allowing easy identification of any ineffective practices.

#### Related Work
Taking reference of 311-data alone, there exists Open311 [1], a standard protocol developed for civic issue tracking. Developers of the Open311 community even offer a rich set of APIs which are being used to create their independent applications, enabling citizens to conveniently raise and track their 311 requests. In terms of analysis Sam Goodgame and Romulo Manzano from UC Berkeley present a similar analysis of NYC 311 data [2] on an AWS EC2 instance with Hadoop. In [3] authors have gone a step further by combining 311 data for the city of Miami with Census Tracts data and analysing how 311 service request patterns are associated with personal attributes and living conditions. With most of the existing studies primarily relying on Python libraries like Numpy, Pandas and Scikit Learn, throughout this paper we would be trying to baseline our results against these existing works, while relying on an Apache Spark based implementation.

## Materials and Methods

#### Datasets

The dataset[5] we are using for analysis is New York City’s non-emergency service request information data. 311 provides access to City government services primarily using: Call Center, Social Media, Mobile App, Text, Video Relay and TTY/text[4].

The complete dataset contains data from 2010-Present and sizes to ~17 GB. To ease the initial development we stripped down the entire dataset to a development set having data from Apr-Aug 2019.

Original dataset has 41 fields in total. Some preliminary cleaning activities done to kickstart the development are as:

	* Dropping any redundant info like Agency Name(abbreviation avaiable in Agency), Community board already captured by Burough.
	* Updating the missing city and borough based on the zip code. 
	* Cols with missing value count more than 1/4th of the total nummber of values.    
	* Removing records with no city values and the issues that are not closed.
	* Calculating the time taken to resolve the issue from the creation date and closing date.

###### *Note: This by far is not an exhaustive list of cleaning activities done and would be updated as we progess in the development.

Cleaned dataset so far has ~33K records with the following cols.

| Column name | Type | Details |
|---|---|---|
| Unique_Key | String | Unique Identifier of the request |
| Created_Date | Date | Creation Date |
| Closed_Date | Date  | Issue Resolving Date |
| Agency | String | Agency Type |
| Complaint_Type | String | Type of complaint |
| Descriptor | String | Description of the issue |
| Location_Type | String | Type of Location |
| Incident_Zip | String | Zip Code |
| Address_Type | String | Type of Address  |
| City | String | City Name  |
| Status | String | Status of the issue |
| Borough | String | Name of the borough  |
| Open_Data_Channel_Type  | String | Channel of the request |
| Latitude | float | Latitude of the location |
| Longitude  | float | Longitude of the location |


#### Technologies and Algorithms used

1. Apache SPARK:
	* Distributed data processing platform used. Actual size of the continuously updated 311 dataset, makes SPARK a good fit for its processing.
	* SPARK-SQL's Dataframe API - offering aggregate functions extensively used during the analysis phase of our study.

2. Unsupervised learning - Custering will also be used as part of our intial trend analysis:
	* K-Means clustering to get clusters based on longitude and latitude. 
	* ELBOW method will be used as a heuristic to identify the appropriate number of clusters in a dataset.

2. Supervised learning will be used to fulfil our second objective to predict the closure time for a request.
	* SPARK-ML offering Regression Algorithms like (Linear Regression, Random Forest, Gradient boosted tree Regression) will be evaluated and the best performing model would be selected.
	* To start with, a 3-Fold Cross-Validation strategy would be use for hyperparameter tuning (for a few selected hyperparameters.)
	* RMSE (Root Mean Squared Error) would initially be used to as our evaluation metric.


#### References
[1] OPEN311 Community. Open311. http://www.open311.org/

[2] Siana Pietri Thomas E Keller Loni Hagen, Hye Seon Yi.
Processes, potential benefits, and limitations of big data
analytics: A case analysis of 311 data from city of
miami. https://dl.acm.org/doi/abs/10.1145/3325112.3325212

[3] Romulo Manzano Sam Goodgame, David Harding.
Analysing nyc 311 requests. http://people.ischool.berkeley.edu/˜samuel.goodgame/311/

[4] 311 NYC Portal https://www.ny.gov/agencies/nyc-311

[5] New York City 311 Open Data https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/data