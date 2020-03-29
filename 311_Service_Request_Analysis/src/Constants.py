# Single source for any re-usable constants

CAPITALIZE_COLS = ['City', 'Borough']

DROP_COLS1 = ['Incident Address', 'Street Name', 'Cross Street 1', 'Cross Street 2',
              'Intersection Street 1', 'Intersection Street 2', 'Landmark', 'Facility Type', 'Location_Type',
              'Resolution Description', 'Resolution Action Updated Date', 'Community Board', 'BBL',
              'X Coordinate (State Plane)', 'Y Coordinate (State Plane)', 'Park Facility Name', 'Park Borough',
              'Vehicle Type', 'Taxi Company Borough', 'Taxi Pick Up Location', 'Bridge Highway Name', 'Descriptor',
              'Bridge Highway Direction', 'Road Ramp', 'Bridge Highway Segment', 'Location', 'Address_Type']

FILENAME = "./311dataset/311_Service_Requests_Apr-Aug-2019.csv"

RESULTS_FOLDER_ANALYSIS_Q1 = "./results/Analysis/Q1/"
RESULTS_FOLDER_ANALYSIS_Q2 = "./results/Analysis/Q2/"
RESULTS_FOLDER_ANALYSIS_Q3 = "./results/Analysis/Q3/"
RESULTS_FOLDER_ANALYSIS_CLUSTERING = "./results/Analysis/Clustering/"
RESULTS_FOLDER_ANALYSIS_SUPERVISED_LEARNING = "./results/Analysis/Supervised Learning/"

DROP_THRESHOLD = 0.25

MONTHS = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')

HOUSE_HOLD_CLEANING_ISSUES = ['HEAT/HOT WATER', 'Request Large Bulky Item Collection', 'UNSANITARY CONDITION',
                              'Water System', 'PLUMBING', 'PAINT/PLASTER', 'WATER LEAK']

VEHICLES_AND_PARKING_ISSUE = ['Illegal Parking', 'Blocked Driveway']

NOISE_ISSUES = ['Noise - Residential', 'Noise', 'Noise - Commercial', 'Noise - Street/Sidewalk']
