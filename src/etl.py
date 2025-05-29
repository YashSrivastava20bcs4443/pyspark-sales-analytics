
from pyspark.sql import SparkSession

def load_data(spark):
    customers = spark.read.csv("data/customers.csv", header=True, inferSchema=True)
    products = spark.read.csv("data/products.csv", header=True, inferSchema=True)
    transactions = spark.read.csv("data/transactions.csv", header=True, inferSchema=True)
    return customers, products, transactions

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SalesAnalytics").getOrCreate()
    customers, products, transactions = load_data(spark)
    customers.show()
