

if __name__ == "__main__":
    df = spark.table("workspace.default.telco_customer_churn")
    
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=137)

    train_df.write.format("delta").mode("overwrite").saveAsTable("default.telco_churn_train")
    test_df.write.format("delta").mode("overwrite").saveAsTable("default.telco_churn_test")