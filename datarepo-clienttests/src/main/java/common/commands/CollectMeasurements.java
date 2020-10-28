package common.commands;

import collector.MeasurementCollector;

public class CollectMeasurements {
  public static void main(String[] args) throws Exception {
    if (args.length == 2) { // collect measurements for a test run
      String measurementListFileName = args[0];
      String outputDirName = args[1];
      MeasurementCollector.collectMeasurementsForTestRun(measurementListFileName, outputDirName);
    } else if (args.length == 5) { // collect measurements for a time interval
      String measurementListFileName = args[0];
      String outputDirName = args[1];
      String serverFileName = args[2];
      String startTimestamp = args[3];
      String endTimestamp = args[4];
      MeasurementCollector.collectMeasurements(
          measurementListFileName, outputDirName, serverFileName, startTimestamp, endTimestamp);
    } else { // if no args specified or invalid number of args specified, print help
      PrintHelp.printHelp();
    }
  }
}
