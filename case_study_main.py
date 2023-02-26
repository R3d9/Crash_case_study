from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
import configparser

class Analysis:

    def __init__(self, config_path, spark: SparkSession):
        config = configparser.ConfigParser()
        config.read(f'{config_path}', encoding = 'utf-8')
        path = config.get('parameters', 'path')
        # print(path)
        
        ## Reading all data into dataframe
        
        self.Units_use = spark.read.format("csv").option("header","true").load(fr"{path}/Units_use.csv").cache()
        
        self.Charges_use = spark.read.format("csv").option("header","true").load(fr"{path}/Charges_use.csv").cache()
        
        self.Damages_use = spark.read.format("csv").option("header","true").load(fr"{path}/Damages_use.csv").cache()

        self.Endorse_use = spark.read.format("csv").option("header","true").load(fr"{path}/Endorse_use.csv").cache()

        self.Primary_Person_use = spark.read.format("csv").option("header","true").load(fr"{path}/Primary_Person_use.csv").cache()

        self.Restrict_use = spark.read.format("csv").option("header","true").load(fr"{path}/Restrict_use.csv").cache()
        
    def number_of_crashes(self):
        number_of_crashes = self.Primary_Person_use.filter("PRSN_GNDR_ID == 'MALE' and PRSN_INJRY_SEV_ID == 'KILLED'").distinct().count()
        print(f"number of crashes (accidents) in which number of persons killed are male are {number_of_crashes}")
    
    def booked_two_wheelers(self):
        booked_two_wheelers = self.Units_use.filter("VEH_BODY_STYL_ID in ('MOTORCYCLE','POLICE MOTORCYCLE')").select("VIN").distinct().count()
        print(f"Two wheelers booked for crashes are: {booked_two_wheelers}")
    
    def highest_crime_state(self):
        highest_crime_state = self.Primary_Person_use.filter("PRSN_GNDR_ID == 'FEMALE'").select("DRVR_LIC_STATE_ID").groupBy("DRVR_LIC_STATE_ID").count().orderBy(col("count").desc()).collect()[0][0]
        print(f"State with highest number of accidents in which females are involed is: {highest_crime_state}")
    
    def vehicle_mk_id(self):
        new_df = self.Units_use.withColumn("Total_injuries",  expr("TOT_INJRY_CNT + DEATH_CNT").cast("integer"))

        result = new_df.select("VEH_MAKE_ID","Total_injuries").groupBy("VEH_MAKE_ID").agg(sum("Total_injuries").alias("Total_injuries")).orderBy(col("Total_injuries").desc()).limit(15).select("VEH_MAKE_ID").rdd.flatMap(lambda x: x).collect()
        print("Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death are:")
        print(result[-12:-1])

    def top_ethnic_user(self):
        Units_use = self.Units_use
        Primary_Person_use = self.Primary_Person_use
        
        combined_df = Units_use.join(Primary_Person_use, Units_use.CRASH_ID == Primary_Person_use.CRASH_ID, "left").select("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").where("(VEH_BODY_STYL_ID not in ('UNKNOWN') and VEH_BODY_STYL_ID is not null)").groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").agg(count("PRSN_ETHNICITY_ID").alias("count"))

        transformed_df = combined_df.withColumn("highest_ethnicity_rank", row_number().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())))
        result = transformed_df.selectExpr("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID as highest_ethnicity_count").where("highest_ethnicity_rank = 1")

        print("top ethnic user group of each unique body style is:")
        result.where("VEH_BODY_STYL_ID not like ALL ('NA', 'NOT REPORTED', 'OTHER%')").show(truncate=False)
    
    def top_zip_codes(self):
        Units_use = self.Units_use
        Primary_Person_use = self.Primary_Person_use
        
        combined_df = Units_use.join(Primary_Person_use, Units_use.CRASH_ID == Primary_Person_use.CRASH_ID, "left").where("VEH_BODY_STYL_ID like ANY('PASSENGER CAR%', 'POLICE CAR/TRUCK') and PRSN_ALC_RSLT_ID == 'Positive'")
        
        result = combined_df.groupBy("VEH_BODY_STYL_ID","DRVR_ZIP").agg(count("DRVR_ZIP").alias("count")).withColumn("highest_crash_number", row_number().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc()))).selectExpr("VEH_BODY_STYL_ID", "DRVR_ZIP as highest_crash_zip_code", "count").where("highest_crash_number < 6")
        
        print("Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash are:")
        result.show(truncate=False)

    def top_distinct_crash_IDs(self):
        Units_use = self.Units_use
        Damages_use = self.Damages_use
        
        combined_df = Units_use.join(Damages_use, Units_use.CRASH_ID == Damages_use.CRASH_ID, "left").where("VEH_DMAG_SCL_1_ID in ('DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 HIGHEST') and VEH_DMAG_SCL_2_ID in ('DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 HIGHEST') and FIN_RESP_TYPE_ID not in ('NA') AND DAMAGED_PROPERTY LIKE ANY ('%NONE%', '%NO DAMAGES%')")

        result = combined_df.distinct().count()

        print(f"Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance is {result}")
        
    def top_5_vehicle_makers(self):
        speeding_crash_ids = self.Charges_use.select("CRASH_ID").where("CHARGE LIKE '%SPEEDING%'").distinct().rdd.flatMap(lambda x: x).collect()

        top_10_used_colors = self.Units_use.groupBy("VEH_COLOR_ID").agg(count("VEH_COLOR_ID")).orderBy(count("VEH_COLOR_ID").desc()).select("VEH_COLOR_ID").limit(10).rdd.flatMap(lambda x: x).collect()

        top_25_states = self.Primary_Person_use.groupBy("DRVR_LIC_STATE_ID").agg(count("DRVR_LIC_STATE_ID")).orderBy(count("DRVR_LIC_STATE_ID").desc()).select("DRVR_LIC_STATE_ID").where("DRVR_LIC_STATE_ID not like ALL ('%NA%','%Unknown%')").limit(25).rdd.flatMap(lambda x: x).collect()

        transformed_units_df = self.Units_use.select("*").filter(col("CRASH_ID").isin(speeding_crash_ids) & col("VEH_COLOR_ID").isin(top_10_used_colors))

        transformed_primary_person_df = self.Primary_Person_use.select("*").filter(col("DRVR_LIC_STATE_ID").isin(top_25_states)).where("DRVR_LIC_TYPE_ID != 'UNLICENCED'")

        combined_df = transformed_units_df.join(transformed_primary_person_df, transformed_units_df.CRASH_ID == transformed_primary_person_df.CRASH_ID, "left").groupBy("VEH_MAKE_ID").agg(count("VEH_MAKE_ID")).orderBy(count("VEH_MAKE_ID").desc()).limit(5).select("VEH_MAKE_ID")

        print("Top 5 Vehicle Makers where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences are:")
        combined_df.show(truncate=False)

