# 311-Service-Request Data Analysis using Apache Spark - SOEN 691 Project

## Abstract
Recent advances in the field of Big Data Analytics and Machine Learning have introduced a plethora of open-source tools and technologies for both, 
academia and the growing data analyst community. In this project we try leveraging one such popular distributed data processing framework Apache Spark
to analyse 311 - Service Request Data for the city of New-York. Being updated almost on a daily basis for the last 10 years, massive size of this dataset makes it a suitable candidate for analysis using a distributed data processing framework like Spark. Making use of Spark Ecosystem libraries like Spark SQL and Spark ML, on this dataset, enables us to derive some interesting insights, which might drive better resource planning within the city. Identifying the 3 primary goals for this project we first try answering a few statistical questions like “*most frequent complaints reported(across entire city/borough wise)*”, “*Average time to resolve the request (category/department wise)*” and “*most busy days/months in terms of request volumes*”.
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
	* We would end the analysis by trying to run a K-Means clustering algorithm over various zip codes available in the data set(Problem formulation explained in "Technologies and Algorithms used") and analysing the resulting clusters of zip codes to uncover any underlying patterns in the way complaints are being raised.

2. Further we try building a predictive model with an ability to predict the closure time for a particular request. This provides a way for the policy makers to closely scrutinize the operations of the concerned department, thereby allowing easy identification of any ineffective practices.

#### Related Work
Taking reference of 311-data alone, there exists Open311 [1], a standard protocol developed for civic issue tracking. Developers of the Open311 community even offer a rich set of APIs which are being used to create their independent applications, enabling citizens to conveniently raise and track their 311 requests. In terms of analysis Sam Goodgame and Romulo Manzano from UC Berkeley present a similar analysis of NYC 311 data [2] on an AWS EC2 instance with Hadoop. In [3] authors have gone a step further by combining 311 data for the city of Miami with Census Tracts data and analysing how 311 service request patterns are associated with personal attributes and living conditions. With most of the existing studies primarily relying on Python libraries like Numpy, Pandas and Scikit Learn, throughout this paper we would be trying to baseline our results against these existing works, while relying on an Apache Spark based implementation. [6] Has been used as a baseline work for the supervised learning phase of our study.

## Materials and Methods

#### Dataset

The dataset[5] we are using for analysis is New York City’s non-emergency service request information data. 311 provides access to City government services primarily using: Call Center, Social Media, Mobile App, Text, Video Relay and TTY/text[4].

The complete dataset contains data from 2010-Present and sizes to ~17 GB. To ease the initial development we stripped down the entire dataset to a development set having data from Apr-Aug 2019.
However once after a working pipeline was established the final results have been obtained using full dataset from the year 2018 and 2019. 

Original dataset has 41 fields in total. Cleaning activities involved are as follows:

	* Dropping any redundant info like Community board (already captured by Borough) or some location specific fields like Street, Intersection or Highway.
	* Formatting of the zip-codes (Taking only the first five characters).
	* Updating the missing city and borough based on the zip code. 
	* Cols with missing value count more than 1/4th of the total number of values.    
	* Removing records with no city values or records that do not have a closing date.
	* Calculating the "time taken to resolve the issue in hours" after standardizing creation and closing date.
	* Extracting separate columns for Day, Month, Hour of request creation and closing.

Original ***2019*** Dataset has - ***2456832*** rows **(1.27 GB)** -> After Cleaning it had ***1128786*** rows **(315 MB)**

Original ***2018*** Dataset has - ***2741682*** rows **(1.43 GB)** -> After Cleaning it had ***1255981*** rows **(352 MB)**

List of ***25 columns*** after cleaning:

| Column name | Type | Details |
|---|---|---|
| Unique_Key | string | Unique Identifier of the request |
| Closing_timestamp | bigint | Unix timestamp for the request closure |
| Creation_timestamp | bigint | Unix timestamp for the request creation |
| time_to_resolve_in_hrs | double | time to resolve the issue in hours |
| Agency | string | Department Abbreviation |
| Agency_Name | string | Department Full Name |
| Open_Data_Channel_Type  | string | Channel of the request |
| Status | string | Status of the issue |
| Complaint_Type | string | Type of complaint |
| Borough | string | Name of the borough  |
| Creation_Month | int | Month on which the issue was created |
| Creation_Day | int | Day of the Month on which the issue was created |
| Creation_Hour | int | Hour of the day on which the issue was created |
| Closing_Month | int | Month on which the issue was closed |
| Closing_Day | int | Day of the Month on which the issue was closed |
| Closing_Hour | int | Hour of the day on which the issue was closed |
| Issue_Category | string | Household-Cleaning, Noise, Vehicle-Parking |
| Incident_Zip | string | Zip Code |
| City | string | City Name  |
| Latitude | float | Latitude of the location |
| Longitude  | float | Longitude of the location |
| Created_Date | string  | Creation Date of the issue |
| Creation_Time | string  | Creation Time of the issue |
| Closed_Date | string  | Closing Date of the issue |
| Closing_Time | string  | Closing Time of the issue |

#### Technologies and Algorithms used

1. Apache SPARK:
	* Distributed data processing platform used. Actual size of the continuously updated 311 dataset, makes SPARK a good fit for its processing.
	* SPARK-SQL's Dataframe API - offering aggregate functions extensively used during the analysis phase of our study.

2. Unsupervised learning - Clustering was also used as part of our initial trend analysis:
	* Each Unique Zip Code in the cleaned data is mapped on to a 13-D space of Complaint_Type (13 popular complaint types considered).
	* Each Unique Zip Code is then represented as a vector of complaint type with each value being a count of a particular complaint type occuring within that zip code.
	* Standardize the features
	* Run K-Means clustering to get clusters of zip-codes. 
	* ELBOW method used as a heuristic to identify the appropriate number of clusters in a dataset.

2. Supervised learning will be used to fulfil our second objective to predict the closure time for a request.
	* SPARK-ML offering Regression Algorithms like (Linear Regression, Random Forest, Gradient boosted tree Regression) will be evaluated and the best performing model would be selected.
	* To start with, a 3-Fold Cross-Validation strategy would be used for hyperparameter tuning (for a few selected hyperparameters.)
	* RMSE (Root Mean Squared Error) and R-Squared(Co-efficient of Determination) would be used as our evaluation metric.

#### Results and Discussion
For a more meaningful analysis out of the **367** different complaint types we shortlisted **13** popular complaint types under 3 categories:

**(Type-A) HOUSE_HOLD_CLEANING_ISSUES** = *'HEAT/HOT WATER', 'Request Large Bulky Item Collection', 'UNSANITARY CONDITION', 'Water System', 'PLUMBING', 'PAINT/PLASTER', 'WATER LEAK'*

**(Type-B) VEHICLES_AND_PARKING_ISSUE** = *'Illegal Parking', 'Blocked Driveway'* 

**(Type-C) NOISE_ISSUES** = *'Noise - Residential', 'Noise', 'Noise - Commercial', 'Noise - Street/Sidewalk'*

1. **Trend Analysis: Mostly used python package - pyspark.sql** 

To identify any recurring trends we compared results obtained over the dataset from the years 2018 and 2019.

a. City wide and Boroughs wide distribution of complaints
Complaint Type Distribution

![Complaint_type_2018_2019](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Q1/ReportImages/Overall.png?token=AKZR5NQD6LMAATB3NQBTA2C6TCGJU)

* Both 2018 and 2019 saw most number of complaints for Heat/Hot Water issues and Residential Noise.
* In 2018 Requests to collect large bulky items were almost 175,000 which reduced to 100,000 in 2019, indicating an improvement in the services.
* Complaints about illegal parking is similar(Above 100,000) in both years. There were between 50,000 and 75,000 number of complaints related to Noise, Street/Sidewalk noise, Paint/Plaster, Plumbing, Unsanitary Condition and Water System were reported in both years. The least number of complaints obtained in both years are for Water Leak issues and Commercial noise which were less than 50,000.

Following are the major boroughs which reported at least 5000 complaints.

![Borough_Wise_Complaint_type_2018_2019](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Q1/ReportImages/Borough_Grouped.png?token=AKZR5NWQFJ6W7AEVH2MUBJK6TCGL4)

* Except in Queens, in all boroughs there were almost 60,000-70,000 issues related to Heat/Hot water were reported. 
* In Queens the most common complaints were on illegal parking and collection of Large and Bulky items.
		
b. Monthly, Daily and Hourly distribution of complaints

* Hourly Analysis:  Similar hourly trend in call volumes for Type-A, Type-B, Type-C complaints from 2018 to 2019. Maximum volume of Type-A complaints recorded from 9:00 am to 5:00 pm. For Type-C(Noise) an expected U-Shaped plot can be observed where we see an increases after midnight between 1:00 am to 2:00 am and then again starts increasing again after 8:00 pm in the night.
		
![NoiseHourly2018_2019](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Q2/ReportImages/NoiseHourly2018_2019.png?token=AKZR5NQN6TKVQHDKUGWBWCK6TCGPI) 
				
* Daily Analysis: Call volumes have pretty much been consistent on a daily basis. We could not identify any such specific days in a month where the call volume were observed to have a sharp increase or decrease. However (Type-B) Parking in New York City which is often seen as a coveted luxury, had a consistent higher number of complaints on a daily basis along with a rise in complaints from 2018 to 2019.
		
![ParkingDaily2018_2019](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Q2/ReportImages/VehicleDaily2018_2019.png?token=AKZR5NUFX2TGCIGNQJ4MRI26TCGRM)
	
* Monthly Analysis: For Type-A complaints, January as a month saw significantly higher complaints both in 2018 and 2019. However trend obtained shows a reduction during July-December period from 2018 to 2019. For Type-C(Noise) both 2018 and 2019 saw peak during Summers i.e. May-August.
		
![NoiseMonthly2018](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Q2/ReportImages/NoiseMonthly2018_2019.png?token=AKZR5NU32ZMQ6ZKYUOYGGTK6TCGTU)


c. Average time to resolve the request

* On the basis of Complaint Type

	![AverageTimeToResolveIssue](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Q3/ReportImages/Overall.png?token=AKZR5NVMWUYMMQHFTHN4PMK6TCGV4)

	* In both 2018 and 2019 it took on an average more than 400hrs to fix the Unsanitary Condition issues. So authrorities might need to investiagte the reasons for this delay.

	* The time taken for fixing the Water System issues was considerably reduced in the year 2019 from almost 400hrs to slightly above 100hrs, indicating an improvement in the service. 

	* Least time taken for issues like, Commercial, Residential Noise, Street Noise, Heat/Hot water issue, Blocked Driweway, which is less than 100hrs.

* On the basis of 5 major Agencies:

	* Department of Housing Preservation and Development (HPD)

	* New York City Police Department (NYPD)

	* Department of Environmental Protection (DEP)

	* Department of Sanitation (DSNY) 

	* Department of Information Technology and Telecommunications (DOITT)

![AgencyWiseComplaintTypeResolutionTime](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Q3/ReportImages/Agency.png?token=AKZR5NVZOZ6XG5UPKNPWTDC6TCGXO)
		
		
2. **Clustering: Mostly used python package - pyspark.ml** 

Results shown for 2019 Data:

* With each Zipcode represented by a 13-D standardized vector of Complaint_Type count we ran K-Means simulation runs starting from 
**2 Clusters upto 20 Clusters** and tried plotting an Elbow curve shown in the figure below. The **cost(J)** in the plot represents - **Inertia** which is the sum of squared distances of samples to their closest cluster center.
	
	![CostKMeans_Elbow](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Clustering/ReportImages/ClusteringCostAndElbow.JPG?token=AKZR5NRMMAMCASDPMLEBEAS6TCG2Y)
		
	Based on the elbow curve shown above we arrived at **8** being the optimal number of clusters for the given dataset and Re-Ran our clustering with a predefined value of **K set to 8**.
	
	Resulting zipcode clusters obtained are shown in the file:
	[CluteringResults2019](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Clustering/Reference_2019/ClusteringResults.txt?token=AKZR5NWCBW2OLEPGKUKR4S26TCG5E)
	
	As a sanity check for our results we tried analysing one of the clusters (Cluster 2 in results file) to see if there is any recognizable complaint trend among the zipcodes in that cluster.
	
	![ClusterAnalysis](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Clustering/Reference_2019/Analysis_Cluster_2.png?token=AKZR5NRZT56YAJDULNZ774C6TCHBM)
	
	As per our expectations every zipcode within this cluster had the **same top 5 complaints(namely Heat/Hot Water, Illegal Parking, Blocked Driveway, Noise - Residential and Request Large Bulky Item Collection)**.
	
	Moreover the figures for complaint count for any specific complaint type were comparable for all the zipcodes in this cluster. **For instance Complaint_Type - "Request Large Bulky Item" had few hundred cases in each zip code of this cluster while "Heat/Hot Water" had significantly higher number of cases in every zipcode of this cluster.**

	Clustering results based on 2019 data suggest that Muncipal authorities can divide the entire NewYork city zip-codes into 8 Non-Emergency-Service-Groups(based on 8 clusters) and further allocate resources to these groups based on the more frequent and common complaint types within that group of zipcodes.
	
3. **Supervised Learning: Mostly used python package - pyspark.ml** 

	The cleaned data had quite a few categorical columns. Instead of directly using label encoding (as it would simply assign numeric labels to categories and we never wanted the magnitude of that assigned numeric value should influence our model in any way ), we preffered using One-Hot encoding and created new fetaures for each category. 
	
	**Categorical columns use for One-Hot Encoding - "Agency", "Borough", "complaint_type", "open_data_channel_type".**
	
	**Resulting features list after encoding:**
	* Creation_Month
	* Creation_Day
	* Creation_Hour
	* e_AGENCY_HPD
	* e_AGENCY_NYPD
	* e_AGENCY_DEP
	* e_AGENCY_DSNY
	* e_AGENCY_DOITT
	* e_BOROUGH_UNSPECIFIED
	* e_BOROUGH_QUEENS
	* e_BOROUGH_BROOKLYN
	* e_BOROUGH_BRONX
	* e_BOROUGH_MANHATTAN
	* e_BOROUGH_STATEN ISLAND
	* e_COMPLAIN_TYPE_UNSANITARY CONDITION
	* e_COMPLAIN_TYPE_Illegal Parking
	* e_COMPLAIN_TYPE_Noise - Residential
	* e_COMPLAIN_TYPE_Noise - Commercial 
	* e_COMPLAIN_TYPE_Water System 
	* e_COMPLAIN_TYPE_Blocked Driveway
	* e_COMPLAIN_TYPE_HEAT/HOT WATER
	* e_COMPLAIN_TYPE_PAINT/PLASTER
	* e_COMPLAIN_TYPE_Noise
	* e_COMPLAIN_TYPE_Request Large Bulky Item Collection
	* e_COMPLAIN_TYPE_PLUMBING
	* e_COMPLAIN_TYPE_WATER LEAK
	* e_COMPLAIN_TYPE_Noise
	* e_CHANNEL_TYPE_MOBILE
	* e_CHANNEL_TYPE_UNKNOWN
	* e_CHANNEL_TYPE_OTHER
	* e_CHANNEL_TYPE_PHONE
	* e_CHANNEL_TYPE_ONLINE
	
	**Train Test Split Ratio**
	Train-Test Split ratio is **0.8,0.2 where 0.8 is training sample and 0.2 is test sample**.
	
	3-fold cross validation with Grid search was used for Hyper parameters tuning.
	
	**Hyperparameters explored:**
	* Linear Regression regParam= [0.1, 0.01], maxIter= [100, 200, 300]
	where regParam is regularisation parameter with value greater than zero, maxIter correspond to epochs
	* Random Forest numTrees= [70, 120]
	where numTrees are number of trees to be formed in Random Forest
	* Gradient Boost maxIter= [10, 20]
	where maxIter correspond to epochs
	
	**Best Hyperparameters values**
	* Linear Regression regParam= 0.1, maxIter, 100
	* Random Forest numTrees= 120
	* Gradient Boost maxIter= 20

	**Evaluation Metrics used -**
	
	* RMSE(Root Mean Squared) - Lesser the RMSE of a model the better it is. It provides a quantifiable error figure of the same unit as that of the target variable.
	![EvaluationMetricRMESE](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Supervised%20Learning/ReportImages/RMSE.JPG?token=AKZR5NSPV5NME6OHBVSLZVC6TCHGY)
	
	However, RMSE alone cannot determine the goodness of a model. Say an RMSE value obtained is 192 but against what baseline. How good is this 192?
	We therefore used another popular evaluation metric R-Square which basically compares how good our model is to naive model which predicts simply the mean value of the target variable.
	
	* R-Squared - scale free(unit independent) unlike RMSE (Range -infinity to 1); R-squared value .5 means model captures 50% variance of the target variable however a negative value indicates a poorly fit model. 
	![EvaluationMetricR-Square](https://raw.githubusercontent.com/apoorvsemwal/BigdataAnalytics/master/311_Service_Request_Analysis/results/Analysis/Supervised%20Learning/ReportImages/R2.JPG?token=AKZR5NQMN7SXY2DHUU2QXK26TCHEG)
	
	**RMSE AND R2 value for Linear Regressor on train data:**
	* Linear Regression RMSE=197.13 , R2=0.305 	
	
	**RMSE AND R2 value for each model on test data:**
	* Linear Regression RMSE=201.97 , R2=0.294 	
	* Random Forest RMSE=199.18 , R2=0.3136 
	* Gradient Boost RMSE=194.61 , R2=0.345 
	
	**For detailed results refer 311_Service_Request_Analysis/results/Analysis/Supervised Learning/2019_Reference**
	
	Best Regressior model based on RMSE and R2 will be **Gradient Boost**
	
	**Gradient Boost Top 5 Feature Importance Scores:**
	* Creation_Day - 0.098
	* Creation_Month - 0.188
	* Creation_Hour - 0.152
	* e_AGENCY_DOITT - 0.076
	* e_COMPLAIN_TYPE_Noise - Commercial - 0.085
		
	
#### Limitations and Future Work
	
The analysis done as part of this project was limited in some aspects like:

a. Analysis was only carried out only over the dataset from the year 2019 and 2018.  
b. Supervised Learning involved trying hyper-parameter tuning for very few hyper-parameters values.

Both the above mentioned points were largely attributed to the fact that the computational resources used for this project were regular personal laptop machines.  Consequently **were not able to exactly match the results shown in [6]** which we had used as our baseline for the supervised learning part.
 
In order to overcome such limitations a future extension to this project could involve running this same working pipeline over a compute cluster which would allow us to cover more data and may be arrive at better results within our analysis.  

#### Configuration Used

It took us approximately **78 mins** to generate all the results on a single machine with **i5 5th Gen** and **8GB RAM**.
	
#### Instructions to run the project
	* Download the Project Directory to your loacl machine - "311_Service_Request_Analysis"
	
	* Navigate to '\311_Service_Request_Analysis\src' in your terminal
	
	* Run CMD:
		python Launcher.py "./311dataset/311_Cleaned_Data_Small.csv" True
	
	* CMD accepts 2 command line parameters:
		
		a) "./311dataset/311_Service_Requests_Apr-Aug-2019.csv" - Path of the dataset to read.
		
		*Note: The committed dataset is a small development set.
		
		Actual dataset can be downloaded from:
		[Actual 2018 and 2019 Cleaned Dataset](https://drive.google.com/drive/folders/1MJLL9A0rSUKeLnUFSA5x0XfQuCwTV-yG?usp=sharing)

		b) True/False -> True - Given Input CSV is cleaned data and no need to pre-process it again. False - Given Input CSV is un-cleaned data so we need to run it through the pre-processing pipeline.

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

[6] Nickelous Teixeira https://nickeloustex.github.io/NYC-311-Regression-Predicting/