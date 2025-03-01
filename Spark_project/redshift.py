df = (spark.read.format("jdbc").option("url", "jdbc:redshift://default-workgroup.430118850366.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "dev.test.listing").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Admin1234").load())



df = (spark.read.format("jdbc").option("url", "jdbc:redshift://default-workgroup.430118850366.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("query", "select * from dev.test.listing").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Admin1234").load())


df.write.format("jdbc").option("url", "jdbc:redshift://default-workgroup.430118850366.us-east-1.redshift-serverless.amazonaws.com:5439/dev").option("dbtable", "dev.test.employee").option("driver","com.amazon.redshift.jdbc42.Driver").option("user", "admin").option("password", "Admin1234").mode("append").save()