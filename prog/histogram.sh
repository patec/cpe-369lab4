rm *.class
hadoop fs -rm /user/payuen/lab7/output/prog1/*
hadoop fs -rmdir /user/payuen/lab7/output/prog1 
javac -cp hadoop-core-1.2.1.jar:org.json-20120521.jar:org.json-20120521.jar histogram.java &&
jar cvf histogram.jar *.class &&
hadoop jar histogram.jar histogram -libjars /home/payuen/lab7/prog/org.json-20120521.jar /user/payuen/lab7/data/fudd.json /user/payuen/lab7/output/prog1 &&
hadoop fs -cat /user/payuen/lab7/output/prog1/part-r-00000  &&
printf "Sucess!\n" ||
printf "FAIL!\n"

