# time_series_queue_model
tests.csv: a set of tests listing the time they were taken and their location. 
labs.csv: a set of labs that might process these tests, listing their name and location. 

1. This program finds the closest lab to each test,
2. The test travel to that lab at a speed of 60 miles per hour. Routing is not considered - all travel should be a simple crow fly calculation. 
3. On arrival, a test immediately enters the lab and begins processing which takes five hours to complete. 
4. Visualize the hourly arrivals at each lab with respect to time.  
5. Visualize the number of tests being simultaneously processed at each lab with respect to time, taking into account their arrival and completion times. 
6. A visualization of the average number of arrivals at each lab.
7. A visualization of the average number of tests processed at one time at each lab. 
