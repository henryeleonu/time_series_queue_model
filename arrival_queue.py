from math import sin, cos, sqrt, atan2, radians
import pandas as pd
import datetime
from matplotlib import pyplot as plt
from geopy.distance import geodesic
"""
Code developed by Henry Eleonu
"""
def get_distance_between_point(test_long, test_lat, lab_long, lab_lat):
    """
    Calculates the distance between two points on the earth

    Parameters:
        test_long: float
            Longitude of test locatation
        test_lat: float
            Latitude of test locatation
        lab_long: float
            Longitude of lab locatation
        lab_lat: float
            Latitude of lab locatation
    Returns:
        Distance: float
            Disitance between two locations 
    """
    test = (test_lat, test_long)
    lab = (lab_lat, lab_long)
    return geodesic(test, lab).miles

def add_new_column(dataframe, column_name):
    """
    Adds a new column to a dataframe

    Parameters:
        dataframe: DataFrame
            pandas dataframe object
        column_name: str
            name of column to add
    Returns:
        dataframe: DataFrame
            dataframe object with new column
    """
    dataframe[column_name] = ""
    return dataframe

def update_lab_name_with_closest_lab(tests_dataframe, labs_dataframe):
    """
    Update lab_name column with name of lab closest to test location

    Parameters:
        tests_dataframe: DataFrame
            pandas dataframe object
        labs_dataframe: DataFrame
            pandas dataframe object
    Returns:
        tests_dataframe: DataFrame
            dataframe object with updated lab_name column
    """
    tests_dataframe['lab_name'] = tests_dataframe.apply(lambda x: get_closest_lab_to_test(x['long'], x['lat'], labs_dataframe)[0], axis=1)
    return tests_dataframe

def update_distance_from_closest_lab(tests_dataframe, labs_dataframe):
    """
    Update distance_from_lab column 

    Parameters:
        tests_dataframe: DataFrame
            pandas dataframe object
        labs_dataframe: DataFrame
            pandas dataframe object
    Returns:
        tests_dataframe: DataFrame
            dataframe object with updated distance_from_lab column
    """
    tests_dataframe['distance_from_lab'] = tests_dataframe.apply(lambda x: get_closest_lab_to_test(x['long'], x['lat'], labs_dataframe)[1], axis=1)
    return tests_dataframe

def update_time_test_arrives_lab(tests_dataframe, speed):
    """
    Update time_test_arrives_lab column 

    Parameters:
        tests_dataframe: DataFrame
            pandas dataframe object
        speed: Float
            the speed of movement of test form test location to closest lab
    Returns:
        tests_dataframe: DataFrame
            dataframe object with updated time_test_arrives_lab column
    """
    tests_dataframe['time_test_arrives_lab'] = tests_dataframe.apply(lambda x: get_time_test_arrives_lab(x['distance_from_lab'], speed, x['time']), axis=1)
    return tests_dataframe

def update_completion_time(tests_dataframe):
    """
    Creates a new column, completion time, set all values to arrival time + 5hours. 

    Parameters:
        tests_dataframe: DataFrame
            pandas dataframe object
    Returns:
        tests_dataframe: DataFrame
            dataframe object with completion_time column
    """
    tests_dataframe['time_test_arrives_lab'] = pd.to_datetime(tests_dataframe['time_test_arrives_lab'])
    hours = 5
    processing_time = datetime.timedelta(hours = hours)
    tests_dataframe['completion_time'] = tests_dataframe['time_test_arrives_lab'] + processing_time
    return tests_dataframe

def update_server_size(tests_dataframe):
    """
    Creates a new column, server_size, set all values to 0. 

    Parameters:
        tests_dataframe: DataFrame
            pandas dataframe object
    Returns:
        tests_dataframe: DataFrame
            dataframe object with server_size column
    """
    tests_dataframe['server_size'] = 0
    return tests_dataframe

def get_closest_lab_to_test(test_long, test_lat, labs_dataframe):
    """
    This function will find the closet lab to each test 
    Parameters:
        test_long: Float
            longitude of test location
        test_lat: Float
            latitude of test location 
        labs_dataframe: DataFrame
            dataframe object holding lab data
    Returns:
        lab_name: str
            name of closest lab to test location
        distance: Float
            distance of closest lab to test location
    """
    closest_lab = {"lab_name": "", "distance": 0}
    for i in range(len(labs_dataframe)) :
        distance = get_distance_between_point(test_long, test_lat, labs_dataframe.loc[i, "long"], labs_dataframe.loc[i, "lat"])
        if closest_lab['lab_name'] == "" or distance < closest_lab['distance']:
            closest_lab['distance'] = distance
            closest_lab['lab_name'] = labs_dataframe.loc[i, "lab_name"]
    return closest_lab['lab_name'], closest_lab['distance']

def get_time_test_arrives_lab(distance, speed, date_time_of_test):
    """
    This function gets the date and time test arrives lab 
    Parameters:
        distance: Float
            distance between test location and lab
        speed: Float
            speed test moves from test location to lab location 
        date_time_of_test: Datetime
            date and time test was taken
    Returns:
        future_date_and_time: Datetime
            date and time test arrives lab 
    """
    travel_time = distance/speed
    hours_added = datetime.timedelta(hours = travel_time)
    future_date_and_time = pd.to_datetime(date_time_of_test) + hours_added
    return future_date_and_time

def create_dataframe_from_csv(path_to_csv_file):
    """
    This function transforms a csv file to a pandas dataframe 
    Parameters:
        path_to_csv_file: str
            path to csv file
    Returns:
        df: DataFrame
            dataframe object
    """
    df = pd.read_csv(path_to_csv_file)
    return df

def drop_missing_values_in_dataframe(dataframe):
    """
    This function deletes rows with missing values 
    Parameters:
        dataframe: DataFrame
            dataframe object
    Returns:
        dataframe: DataFrame
            dataframe object without missing values
    """
    return dataframe.dropna()

def merge_arrival_and_completion_time(tests_dataframe):
    """
    This function merges the arrival time and complesion time vertically 
    and create a new row, 'add' which 1 for arrival and -1 for completion.
    Arrival increments the server_size by 1 and completion decrements it by 1. 
    Parameters:
        dataframe: DataFrame
            dataframe object
    Returns:
        union: DataFrame
            dataframe object 
    """
    arrival_time_df = tests_dataframe[['time_test_arrives_lab', 'server_size']]
    completion_time_df = tests_dataframe[['completion_time', 'server_size']]
    arrival_time_df['add'] = 1
    completion_time_df['add'] = -1
    arrival_time_df = arrival_time_df.rename(columns={"time_test_arrives_lab":"time"})
    completion_time_df = completion_time_df.rename(columns={"completion_time":"time"})
    union = pd.concat([arrival_time_df, completion_time_df])
    union = union.sort_values(by="time")
    prev_server_size = 0
    for index, row in union.iterrows():
        if index == 0:
            current_server_size= row['server_size'] + row['add']
            prev_server_size = current_server_size
            #union['server_size'] = union['server_size'] + union['add']
        else:
            current_server_size = prev_server_size + row['add']                   
            prev_server_size = current_server_size
        union.at[index,'server_size'] = current_server_size
    #union.to_csv('union.csv')
    return union

def visualise_hourly_arrivals_at_each_lab(tests_dataframe):
    """
    This function visualises the hourly arrivals of tests at each lab with respect to time
    Parameters:
        tests_dataframe: DataFrame
            dataframe object
    """
    labs_df = create_dataframe_from_csv('labs.csv')
    labs_df = drop_missing_values_in_dataframe(labs_df)
    list_of_labs = labs_df['lab_name'].to_list()
    for lab_name in list_of_labs:
        df = tests_dataframe.loc[tests_dataframe['lab_name'] == lab_name]
        df.time_test_arrives_lab = pd.to_datetime(df.time_test_arrives_lab)
        df = df.sort_values(by="time_test_arrives_lab")
        df = df[['time_test_arrives_lab']]
        df = df.reset_index().set_index('time_test_arrives_lab')
        df = df.resample('H').count()
        df.plot(title = 'hourly arrivals at ' + lab_name)
    plt.show()

def visualise_number_of_tests_simultaneously_processed_at_each_lab(tests_dataframe):
    """
    This function visualises the number of tests simultaneously prodessed at each lab with respect to time
    Parameters:
        tests_dataframe: DataFrame
            dataframe object
    """
    labs_df = create_dataframe_from_csv('labs.csv')
    labs_df = drop_missing_values_in_dataframe(labs_df)
    list_of_labs = labs_df['lab_name'].to_list()
    for index in range(len(list_of_labs)):
        df = tests_dataframe.loc[tests_dataframe['lab_name'] == list_of_labs[index]]
        df = merge_arrival_and_completion_time(df)
        df.plot.line(x = 'time', y = 'server_size', rot=70, title="Visualise the number of tests being simultaneously processed at " + list_of_labs[index])
    plt.show()

def run_processes(path_to_tests_file, path_to_labs_file):
    """
    This function serves as the pipeline with functions executed in the right order
    Parameters:
        path_to_tests_file: str
            path to file holding tests data
        path_to_labs_file: str
            path to file holding labs data
    """
    tests_dataframe = create_dataframe_from_csv(path_to_tests_file)
    labs_dataframe = create_dataframe_from_csv(path_to_labs_file)
    tests_dataframe = drop_missing_values_in_dataframe(tests_dataframe)
    labs_dataframe = drop_missing_values_in_dataframe(labs_dataframe)
    tests_dataframe = add_new_column(tests_dataframe, "lab_name")
    tests_dataframe = add_new_column(tests_dataframe, "distance_from_lab")
    tests_dataframe = add_new_column(tests_dataframe, "time_test_arrives_lab")
    tests_dataframe = update_lab_name_with_closest_lab(tests_dataframe, labs_dataframe)
    tests_dataframe = update_distance_from_closest_lab(tests_dataframe, labs_dataframe)
    tests_dataframe = update_time_test_arrives_lab(tests_dataframe, 60)
    tests_dataframe = update_completion_time(tests_dataframe)
    tests_dataframe = update_server_size(tests_dataframe)
    print(tests_dataframe)
    visualise_hourly_arrivals_at_each_lab(tests_dataframe)
    visualise_number_of_tests_simultaneously_processed_at_each_lab(tests_dataframe)

def main():
    """
    This function is the main where program execution starts
    
    """
    run_processes('tests.csv', 'labs.csv')

if __name__ == "__main__":
    main()