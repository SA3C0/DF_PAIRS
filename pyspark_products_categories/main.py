from pyspark.sql import SparkSession

def create_sample_data(spark):
    products_data = [
        (1, "Молоко"),
        (2, "Хлеб"),
        (3, "Яблоко"),
        (4, "Масло")
    ]
    categories_data = [
        (10, "Напитки"),
        (20, "Выпечка"),
        (30, "Фрукты")
    ]
    product_category_data = [
        (1, 10),
        (2, 20),
        (3, 30)
    ]

    products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
    categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
    product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])
    return products_df, categories_df, product_category_df

if __name__ == "__main__":
    from utils.processing import get_product_category_pairs

    spark = SparkSession.builder.appName("ProductsCategories").getOrCreate()
    products_df, categories_df, product_category_df = create_sample_data(spark)
    result_df = get_product_category_pairs(products_df, categories_df, product_category_df)
    result_df.show(truncate=False)
    spark.stop()
