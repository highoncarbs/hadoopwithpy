# Running MapReduce

With our code in place we now need to use the Hadoop Streaming tool to use our Python code to run the MapReduce operation for us.

With the newest hadoop version or Streaming jar file is located at `/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar` (mine is hadoop-streaming-2.8.1.jar)

```
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.8.1.jar -file ./mapper.py -input -mapper ./mapper.py -file ./reducer.py -reducer ./reducer.py -input ./data/purchases.txt -output ./test-output
```

