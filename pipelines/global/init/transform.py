from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# PostgreSQL variables
JAR_PATH = '/home/luan/projects/covid_dashboard/pipelines/global/init/jar/postgresql-42.4.0.jar'
URL = "jdbc:postgresql://localhost:5433/luandb"
USER = 'luan'
PASSWORD = 'luan_password'

DATASET_PATH = "/home/luan/projects/covid_dashboard/pipelines/global/init/dataset/owid-covid-data.csv"
COLUMN_NAME_LIST = [
    'iso_code', 
    'continent', 
    'location', 
    'date', 
    'total_cases', 
    'new_cases',
    'total_deaths', 
    'new_deaths'
]

def convert_to_integer(number):
    try:
        return int(number)
    except:
        print(f"WARN - Cannot convert [{number}] to INT. Replaced by [-1].")
        return -1

def clean_name(name: str):
    return name.lower().replace(' ', '')

clean_name_udf = udf(clean_name, returnType=StringType())

spark = SparkSession\
    .builder\
    .appName('DimTransformation')\
    .master("local[*]")\
    .config('spark.jars', JAR_PATH)\
    .getOrCreate()

df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option('inferSchema', 'true') \
    .load(DATASET_PATH)\
    .sample(0.01)

# print(df.columns)
ness_df = df.select(COLUMN_NAME_LIST)
# ness_df.show(5)

convert_to_integer_udf = udf(convert_to_integer, returnType=IntegerType())

new_df = ness_df\
    .withColumn('total_cases', convert_to_integer_udf('total_cases'))\
    .withColumn('new_cases', convert_to_integer_udf('new_cases'))\
    .withColumn('total_deaths', convert_to_integer_udf('total_deaths'))\
    .withColumn('new_deaths', convert_to_integer_udf('new_deaths'))

# TRANSFORM DATE
date_df = new_df.select('date')\
    .distinct()\
    .sort('date', ascending=True)\
    .withColumn('year', year('date'))\
    .withColumn('month', month('date'))\
    .withColumn('day', dayofmonth('date'))\
    .drop('date')
# date_df.show(5)
date_df.write\
    .format("jdbc") \
    .option("url", URL) \
    .option('dbtable', 'date_dim')\
    .option("user", USER) \
    .option("password", PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode('append')\
    .save()

# TRANSFORM COUNTRY
country_df = new_df.select('iso_code', 'continent', 'location')\
    .distinct()\
    .withColumnRenamed('iso_code', 'iso_code_3')\
    .withColumnRenamed('location', 'country')\
    .sort(col('country'))
country_df.write\
    .format("jdbc") \
    .option("url", URL ) \
    .option("dbtable", "country_dim") \
    .option("user", USER) \
    .option("password", PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append")\
    .save()



# # TRANSFORM VN PROVINCES
# PROVINCE_DATA_PATH = '/home/luan/projects/covid_dashboard/pipelines/global/init/dataset/vnprovinces.csv'
# vn_provinces_df = spark.read\
#     .format('csv')\
#     .option('header', 'true')\
#     .option('inferSchema', 'true')\
#     .load(PROVINCE_DATA_PATH)
# ness_vn_provinces_df = vn_provinces_df\
#     .select('Province/State', 'Region code', 'Lat', 'Long')\
#     .withColumnRenamed('Province/State', 'province')\
#     .withColumnRenamed('Region code', 'code')\
#     .withColumnRenamed('Lat', 'lat')\
#     .withColumnRenamed('Long', 'long')
# lower_case_vn_provinces_df = ness_vn_provinces_df\
#     .withColumn('province', clean_name_udf('province'))
# lower_case_vn_provinces_df.write\
#     .format("jdbc") \
#     .option("url", URL) \
#     .option("dbtable", "vnprovince_dim") \
#     .option("user", USER) \
#     .option("password", PASSWORD) \
#     .option("driver", "org.postgresql.Driver") \
#     .mode("overwrite")\
#     .save()

# TRANSFORM COVID fact
date_dim_df = spark.read\
    .format('jdbc')\
    .option('url', URL)\
    .option("dbtable", "date_dim") \
    .option("user", USER) \
    .option("password", PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()
separated_date_df = new_df\
    .withColumn('year', year('date'))\
    .withColumn('month', month('date'))\
    .withColumn('day', dayofmonth('date'))\

separated_date_df.createOrReplaceTempView('covid')
date_dim_df.createOrReplaceTempView('date_dim')
join_df = spark.sql("""
    SELECT 
        c.iso_code as iso_code, 
        d._key as date_key, 
        c.new_cases as new_cases, 
        c.total_cases as total_cases,
        c.new_deaths as new_deaths,
        c.total_deaths as total_deaths
    FROM covid c INNER JOIN date_dim d ON 
        c.year = d.year AND c.month = d.month AND c.day = d.day;
""")

covid_df = join_df\
    .select(
        'iso_code', 'date_key', 
        'new_cases', 'total_cases', 
        'new_deaths', 'total_deaths',
    )
covid_df.write\
    .format("jdbc") \
    .option("url", URL) \
    .option("dbtable", "covid") \
    .option("user", USER) \
    .option("password", PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append")\
    .save()