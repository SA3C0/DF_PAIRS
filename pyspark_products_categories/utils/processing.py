from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def get_product_category_pairs(products_df: DataFrame, categories_df: DataFrame, product_category_df: DataFrame) -> DataFrame:
    prod_cat_with_names = product_category_df.join(categories_df, "category_id", "left").select("product_id", col("category_name"))
    result_df = products_df.join(prod_cat_with_names, "product_id", "left").select(col("product_name"), col("category_name"))
    return result_df
