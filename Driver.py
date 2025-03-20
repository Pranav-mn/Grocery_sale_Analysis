from Create_dataframe import create_dataframe
import logs.levels as log
from pyspark.sql.functions import col,sum,count,hour,concat_ws,lit,date_format
from Analytical_functions.p1 import visualize

try:
    df_categories = create_dataframe('Data/categories.csv')
    df_cities = create_dataframe('Data/cities.csv')
    df_countries = create_dataframe('Data/countries.csv')
    df_customers = create_dataframe('Data/customers.csv')
    df_employees = create_dataframe('Data/employees.csv')
    df_products = create_dataframe('Data/products.csv')
    df_sales = create_dataframe('Data/sales.csv')
    log.log_info('All Dataframe created Successfully...')
    # df_sales.show()
except Exception as e:
    log.log_error(f'{e}')



# ----------------- products that most persons purchased ------------------
# ---------- Most Sold products, With how many quantity are sold ----------


try:
    df_joined = df_sales.join(df_products, on="ProductID", how="inner")
    df_joined = df_joined.select('ProductID', 'ProductName', 'SalesID', 'Price', 'SalesDate','quantity')
    # df_joined.show()

    df_grouped = df_joined.groupBy('ProductName').agg(count('SalesID').alias('No_of_purchases'))
    most_Per_purchased = df_grouped.orderBy(col("No_of_purchases").desc())
    most_Per_purchased.show()
    log.log_info('Products most Persons purchased list Updated...')
    print('**************************************************************')
    df_grouped = df_joined.groupBy('ProductName').agg(sum('quantity').alias('Total_quantity'))
    Item_sold_More_quantity = df_grouped.orderBy(col("Total_quantity").desc())
    Item_sold_More_quantity.show()

    most_Per_purchased.write.mode("overwrite").option("header", True).csv("Result/Most_persons_purchased_product")
    Item_sold_More_quantity.write.mode("overwrite").option("header", True).csv("Result/Item_sold_More_quantity")
    log.log_info('Products That More quantity list Updated...')

except Exception as e:
    log.log_error(f'products that most persons purchased :{e}')


# ----------------- Find the no.of sales in each hours -----------------


try:
    df = df_sales.withColumn('SalesHour', hour(df_sales['SalesDate']))
    df = df.select('ProductID', 'SalesDate', 'SalesHour')
    df = df.groupBy('SalesHour').count()
    df = df.orderBy(df.SalesHour)
    df = df.filter(col("SalesHour").isNotNull())
    result = df.withColumn("formatted_hour",
                           concat_ws(":", col("SalesHour"), lit("00")))
    df_with_12hr = result.withColumn("time_12hr",
                                     date_format(col("formatted_hour"), "hh:mm a"))

    result = df_with_12hr.select('time_12hr', 'count')
    result.show(n=24)
    result.write.mode("overwrite").option("header", True).csv("Result/Sales_in_Hours")
    log.log_info('No.of sales in each Hour list updated.....')

    visualize(result, 'time_12hr', 'count', 'Visualize_result/result2.pdf')
    log.log_info('Visualization result stored in folder, Visualize_result')

except Exception as e:
    log.log_error(f'No.of sales in each Hour query have error,{e}')

