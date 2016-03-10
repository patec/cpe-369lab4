rm *.class
hadoop fs -rm /user/payuen/lab7/output/prog3/*
hadoop fs -rm /user/payuen/lab7/output/prog3temp/*
hadoop fs -rmdir /user/payuen/lab7/output/prog3
hadoop fs -rmdir /user/payuen/lab7/output/prog3temp 
javac -cp hadoop-core-1.2.1.jar:org.json-20120521.jar:org.json-20120521.jar activity.java&&
jar cvf activity.jar *.class &&
hadoop jar activity.jar activity -libjars /home/payuen/lab7/prog/org.json-20120521.jar /user/payuen/lab7/data/fudd.json /user/payuen/lab7/output/prog3 &&
hadoop fs -cat /user/payuen/lab7/output/prog3/part-r-00000  &&
printf "Sucess!\n" ||
printf "FAIL!\n"

