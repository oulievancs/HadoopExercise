javac -classpath "$(yarn classpath)" -d ccount_classes fileccount.java
jar -cvf ccount.jar -C ccount_classes/

hadoop jar ccount.jar org.myorg.fileccount input output
