spark-submit ^
--class "topk.TopKDriver" ^
--driver-java-options "-Dlog4j.configuration=file:./log4j.properties" ^
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" ^
target/scala-2.12/top_k_queries-assembly-1.0.jar ^
-k 3