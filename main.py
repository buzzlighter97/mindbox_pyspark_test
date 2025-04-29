from pyspark.sql import DataFrame


def left_join_method(products_df: DataFrame, product_category_df: DataFrame, categories_df: DataFrame) -> DataFrame:
    return products_df.join(product_category_df, on="product_id", how="left") \
                         .join(categories_df, on="category_id", how="left") \
                         .select("product_name", "category_name")