from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt


## 1. Bookings

## Read the file

@dlt.table(
    name = "stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format("delta") \
    .load("/Volumes/project/bronze/bronzevolume/bookings/data/")

    return df

## Transformation

@dlt.view(
    name = "trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table("stage_bookings")   \
       .withColumn("amount", col("amount").cast(DoubleType()))  \
        .withColumn("booking_date" , to_date(col("booking_date"), "yyyy-MM-dd"))    \
        .withColumn("modifiedDate" , current_timestamp())   \
        .drop("_rescued_data")

    return df

## Rules for expectations

rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL"
}

## Load to silver layer

@dlt.table(
    name = "silver_bookings"
)

@dlt.expect_all_or_drop(rules)

def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df

## 2. Flights

@dlt.view(
    name = "trasnform_flights"
)
def transfrom_flights():
    df = spark.readStream.format("delta")  \
        .load("/Volumes/project/bronze/bronzevolume/flights/data/")
    df = df.withColumn("flight_date" , to_date(col('flight_date') , 'yyyy-MM-dd'))  \
        .withColumn("modifiedDate" , current_timestamp())  \
        .drop("_rescued_data")

    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "trasnform_flights",
    keys = ["flight_id"],
    sequence_by = col("modifiedDate"),
    stored_as_scd_type = 1
)


## 3. Passengers

@dlt.view(
    name = "trasnform_passengers"
)
def transfrom_flights():
    df = spark.readStream.format("delta")  \
        .load("/Volumes/project/bronze/bronzevolume/customers/data/")

    df = df.withColumn("modifiedDate" , current_timestamp()).drop('_rescued_data')

    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trasnform_passengers",
    keys = ["passenger_id"],
    sequence_by = col("passenger_id"),
    stored_as_scd_type = 1
)

## 4. Airports

@dlt.view(
    name = "trasnform_airports"
)
def transfrom_flights():
    df = spark.readStream.format("delta")  \
        .load("/Volumes/project/bronze/bronzevolume/airports/data/")
    
    df = df.drop('_rescued_data').withColumn("modifiedDate" , current_timestamp())

    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trasnform_airports",
    keys = ["airport_id"],
    sequence_by = col("airport_id"),
    stored_as_scd_type = 1
)

## Join Transformation

# import dlt
# @dlt.table(
#     name = "silver_business"
# )
# def silver_business():

#     df = dlt.readStream("silver_bookings")  \
#         .join(dlt.readStream("silver_flights"),["flight_id"])   \
#         .join(dlt.readStream("silver_passengers"),["passenger_id"]) \
#         .join(dlt.readStream("silver_airports"),["airport_id"]) \
#         .drop("modifiedDate")

#     return df