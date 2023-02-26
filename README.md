# A pyspark code to generate specific analysis from given datasets

The code is run through a spark-submit and the configparser library is expected to be installed.

The pyspark code expects the path of the config file where the data path is stored

A sample spark-submit is:

spark-submit /path/to/case_study_wrapper.py /path/to/config.ini

Additional spark options can be specified based on resources available and spark configuration

The dataset includes 6 csv files with various details like person-related crash details, accident report details, damages, charges and insurance claims etc.

We are trying to analyse following things:
  1.	Find the number of crashes (accidents) in which number of persons killed are male?
  2.	How many two wheelers are booked for crashes? 
  3.	Which state has highest number of accidents in which females are involved? 
  4.	Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
  5.	For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
  6.	Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
  7.	Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
  8.	Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car     licensed with the Top 25 states with highest number of offences (to be deduced from the data)

