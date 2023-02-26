import subprocess
import sys

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
import configparser
from case_study_main import *


config_file_path = sys.argv[1]

spark = SparkSession \
    .builder \
    .appName("case study") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

## Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?

Analysis(config_file_path, spark).number_of_crashes()

## Analysis 2: How many two wheelers are booked for crashes?

Analysis(config_file_path, spark).booked_two_wheelers()

## Analysis 3: Which state has highest number of accidents in which females are involved?

Analysis(config_file_path, spark).highest_crime_state()

## Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

Analysis(config_file_path, spark).vehicle_mk_id()

## Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

Analysis(config_file_path, spark).top_ethnic_user()

## Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

Analysis(config_file_path, spark).top_zip_codes()

## Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

Analysis(config_file_path, spark).top_distinct_crash_IDs()

## Analysis 8: Determine the Top 5 Vehicle Makers where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

Analysis(config_file_path, spark).top_5_vehicle_makers()
