# spark4-scala-sample

This project is a sample project for a Spark listener that can be used to track Spark jobs and send the data to a remote server.

## How to Build

This project uses Maven to build. To build the project, run the following command:

```bash
mvn clean package
```

This will create a fat jar in the `target` directory.

## How to Run

To run the project, you can use the `spark-submit` command.

```bash
spark-submit \
  --class io.github.amrnablus.sparklineage.MySparkApp \
  --master local[*] \
  target/spark4-scala-sample-1.0.0-jar-with-dependencies.jar
```

## How to Configure the Listener

To configure the listener, you need to add the following to your Spark configuration:

```
spark.extraListeners=io.github.amrnablus.sparklineage.listener.SparkLineageListener
```

You will also need to add the jar file to the Spark classpath.
