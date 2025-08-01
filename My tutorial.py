# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading ###
# MAGIC

# COMMAND ----------

df = spark.read \
    .format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load("/Volumes/workspace/default/my_volume/BigMart Sales.csv")
    
df.display()


# COMMAND ----------



# COMMAND ----------

df_json1 = spark.read.format("json").option("inferSchema", "true").load('/Volumes/workspace/default/my_volume/drivers.json')


# COMMAND ----------

df_json1.display()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL Schema ###

# COMMAND ----------

"""my_ddl_schema = '''Item_Identifier string, Item_Weight string, Item_Fat_Content string, Item_Visibility double, Item_Type string, Item_MRP double, Outlet_Identifier string, Outlet_Establishment_Year long, Outlet_Size string, Outlet_Location_Type string, Outlet_Type string, Item_Outlet_Sales double'''
df = spark.read \
    .format("csv") \
    .schema(my_ddl_schema) \
    .option("header", "true") \
    .load("/Volumes/workspace/default/my_volume/BigMart Sales.csv")

df.printSchema()"""

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC  %md 
# MAGIC  # read the original data frame for further use cases #

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Command ###

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df.select(col("Item_Identifier"), col("Item_Weight"), col("Item_Fat_Content")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alias ###

# COMMAND ----------

df.select(col("Item_Identifier").alias("Item_Id")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter ###

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario -1 ###
# MAGIC filter item fat content where it is regular

# COMMAND ----------

df.filter(col("Item_Fat_Content") == "Regular").display()


# COMMAND ----------

# MAGIC %md
# MAGIC  ### Scenario - 2 ###
# MAGIC
# MAGIC  filtering using 2 conditions

# COMMAND ----------

df.filter((col("Item_Type") == 'Soft Drinks') & (col('Item_Weight') < 10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 3 ###
# MAGIC filtering with 3 conditions

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### With column renamed ###

# COMMAND ----------

df.withColumnRenamed("Item_Weight", "Item Wt").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn ###
# MAGIC scenario - 1

# COMMAND ----------

df = df.withColumn('flag',lit('new'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('multiply',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### scenario -2 ###
# MAGIC in item fat content replace low fat and regular with lf and reg

# COMMAND ----------

df = df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","Lf"))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TypeCasting ###

# COMMAND ----------

df = df.withColumn('Item_Weight', col('Item_Weight').cast('String'))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort/orderBy ###

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()


# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3 ###
# MAGIC sorting based on multiple columns

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LIMIT Function ###

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop ###
# MAGIC

# COMMAND ----------

df.drop('Item_Visibility','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicates ###
# MAGIC scenario 1

# COMMAND ----------

df.dropDuplicates().display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicates ###
# MAGIC scenario 2

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###UNION and UNION BY NAME###
# MAGIC Preaparing Dataframes
# MAGIC

# COMMAND ----------

data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()
df2.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Union###
# MAGIC

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [('kad','1',),
        ('sid','2',)]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

df1.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Union by Name###
# MAGIC

# COMMAND ----------

df1.unionByName(df2).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###String Functions###
# MAGIC initcap
# MAGIC upper
# MAGIC lower

# COMMAND ----------

df.select(upper('Item_Type').alias('upper_Item_Type')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Date Functions###
# MAGIC current date

# COMMAND ----------

df = df.withColumn('curr_date',current_date())

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Date_Add()
# MAGIC

# COMMAND ----------

df = df.withColumn('week_after',date_add('curr_date',7))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Date_Sub()
# MAGIC

# COMMAND ----------

df.withColumn('week_before',date_sub('curr_date',7)).display()


# COMMAND ----------

df = df.withColumn('week_before',date_add('curr_date',-7)) 

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC DateDIFF
# MAGIC

# COMMAND ----------

df = df.withColumn('datediff',datediff('week_after','curr_date'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Date_Format()
# MAGIC

# COMMAND ----------

df = df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###handling nulls###
# MAGIC dropping nulls

# COMMAND ----------

#it will drop a row when all the columns in that row has null values
df.dropna('all').display()


# COMMAND ----------

#it will drop a row when any one or more of the columns in that row has null values
df.dropna('any').display()

# COMMAND ----------

#it will drop null values in the column 'Item_Weight'
df.dropna('any',subset=['Item_Weight'])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###filling nulls###
# MAGIC

# COMMAND ----------

#fill all null values with 'NotAvailable'
df.fillna('NotAvailable').display()


# COMMAND ----------

#fill null values in the column 'Outlet_Size'
df.fillna('NotAvailable',subset=['Outlet_Size']).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###SPLIT and Indexing###
# MAGIC ##Split##

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Indexing####

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Explode###

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))

df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()


# COMMAND ----------

df_exp.display()


# COMMAND ----------

df_exp.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### GroupBY
# MAGIC Scenario - 1

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC Scenario - 2
# MAGIC

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### SCenario - 3
# MAGIC

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 4
# MAGIC

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###collect list

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Pivot
# MAGIC

# COMMAND ----------

df.select('Item_Type','Outlet_Size','Item_MRP').display()


# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###When-Otherwise

# COMMAND ----------

df = df.withColumn(
    'veg_flag',
    when(col('Item_Type') == 'Meat', 'Non-Veg').otherwise('Veg')
)

display(df)



# COMMAND ----------

# MAGIC %md
# MAGIC scenario-2

# COMMAND ----------

df.withColumn('veg_exp_flag',when(((col('veg_flag')=='Veg') & (col('Item_MRP')<100)),'Veg_Inexpensive')\
                            .when((col('veg_flag')=='Veg') & (col('Item_MRP')>100),'Veg_Expensive')\
                            .otherwise('Non_Veg')).display()

# COMMAND ----------

df = df.withColumn(
    'veg_exp_flag',
    when((col('veg_flag') == 'Veg') & (col('Item_MRP') < 100), 'Veg_Inexpensive')
    .when((col('veg_flag') == 'Veg') & (col('Item_MRP') > 100), 'Veg_Expensive')
    .otherwise('Non_Veg')
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####JOINS

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()
df2.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Inner Join

# COMMAND ----------

df1.join(df2, df1['dept_id']==df2['dept_id'],'inner').display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Left Join
# MAGIC

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Right Join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Anti Join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###WINDOW FUNCTIONS
# MAGIC row_number(())
# MAGIC

# COMMAND ----------

df.display()


# COMMAND ----------

from pyspark.sql.window import Window


# COMMAND ----------

df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Rank vs Dense Rank

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
        .withColumn('denseRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()
     

# COMMAND ----------

df.withColumn('dum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Cumulative Sum
# MAGIC

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()


# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()


# COMMAND ----------

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###USER DEFINED FUNCTIONS (UDF)
# MAGIC Step - 1

# COMMAND ----------

def my_func(x):
    return x*x 

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP - 2
# MAGIC

# COMMAND ----------

my_udf = udf(my_func)


# COMMAND ----------

df.withColumn('mynewcol',my_udf('Item_MRP')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###DATA WRITING
# MAGIC CSV

# COMMAND ----------

df.write.format('csv')\
        .save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ###APPEND

# COMMAND ----------

df.write.format('csv')\
        .mode('append')\
        .save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

df.write.format('csv')\
        .mode('append')\
        .option('path','/FileStore/tables/CSV/data.csv')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Overwrite

# COMMAND ----------

df.write.format('csv')\
.mode('overwrite')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Error
# MAGIC

# COMMAND ----------

df.write.format('csv')\
.mode('error')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ignore

# COMMAND ----------

df.write.format('csv')\
.mode('ignore')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()
     

# COMMAND ----------

# MAGIC %md
# MAGIC ###PARQUET

# COMMAND ----------

df.write.format('parquet')\
.mode('overwrite')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###TABLE

# COMMAND ----------

df.write.format('parquet')\
.mode('overwrite')\
.saveAsTable('my_table')

# COMMAND ----------

df.display()
