
from pyspark.sql.functions import col, to_date

def clean_data(customers, products, transactions):
    transactions = transactions.withColumn("txn_date", to_date("txn_date", "yyyy-MM-dd"))
    df = transactions.join(customers, "customer_id", "left") \
                     .join(products, "product_id", "left")
    df = df.withColumn("total_price", col("quantity") * col("price"))
    return df
