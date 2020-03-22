# Single source for any re-usable constants

CAPITALIZE_COLS = ['City', 'Borough']

DROP_COLS = ['Agency Name', 'Incident Address', 'Street Name', 'Cross Street 1', 'Cross Street 2',
             'Intersection Street 1', 'Intersection Street 2', 'Landmark', 'Facility Type', 'Location_Type',
             'Resolution Description', 'Resolution Action Updated Date', 'Community Board', 'BBL',
             'X Coordinate (State Plane)', 'Y Coordinate (State Plane)', 'Park Facility Name', 'Park Borough',
             'Vehicle Type', 'Taxi Company Borough', 'Taxi Pick Up Location', 'Bridge Highway Name',
             'Bridge Highway Direction', 'Road Ramp', 'Bridge Highway Segment', 'Latitude', 'Longitude', 'Location']

FILENAME = "./311dataset/311_Service_Requests_Apr-Aug-2019.csv"

RESULTS_FOLDER = "./results/"

DROP_THRESHOLD = 0.5

MONTHS = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')

HOUSE_HOLD_CLEANING_ISSUES = ['HEAT/HOT WATER', 'Request Large Bulky Item Collection', 'UNSANITARY CONDITION',
                              'PLUMBING', 'PAINT/PLASTER', 'WATER LEAK']

VEHICLES_AND_PARKING_ISSUE = ['Illegal Parking', 'Blocked Driveway']

NOISE_ISSUES = ['Noise - Residential', 'Noise', 'Noise - Commercial']
