import org.apache.iotdb.tsfile.read.common.*;
import org.apache.iotdb.tsfile.read.*;
import org.apache.iotdb.tsfile.read.expression.*;
import org.apache.iotdb.tsfile.read.expression.impl.*;
import org.apache.iotdb.tsfile.read.filter.*;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.*;
import java.util.*;


public class SingleReplicaBenchmark {
  /*
   * args[0] -> tsfile path
   * args[1] -> physical order file path
   * args[2] -> result file path
   * args[3] -> start time
   * args[4] -> end time
   * args[5...] -> visit measurement
   */
  public static void main(String[] args) {
    if (args.length < 4) {
      System.err.println("Missing args");
      return;
    }
    String tsfilePath = args[0];
    String physicalOrderFilePath = args[1];
    String logFilePath = args[2];
    long startTime = Long.parseLong(args[3]);
    long endTime = Long.parseLong(args[4]);
    List<String> physicalOrder = readPhysicalOrder(physicalOrderFilePath);
    Set<String> measurements = new HashSet<>();
    for (int i = 5; i < args.length; ++i) {
      measurements.add(args[i]);
    }
    List<Path> queryPath = new ArrayList<>();
    for (String measurement : physicalOrder) {
      if (measurements.contains(measurement)) {
        queryPath.add(new Path("root.test.device", measurement));
      }
    }
    File logFile = new File(logFilePath);

    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsfilePath);
         ReadOnlyTsFile readOnlyTsFile = new ReadOnlyTsFile(reader)) {
      IExpression startTimeFilter = new GlobalTimeExpression(TimeFilter.gtEq(startTime));
      IExpression endTimeFilter = new GlobalTimeExpression(TimeFilter.ltEq(endTime));
      QueryExpression queryExpression = QueryExpression.create(queryPath, BinaryExpression.and(startTimeFilter, endTimeFilter));
      long executeStartTime = System.currentTimeMillis();
      QueryDataSet dataSet = readOnlyTsFile.query(queryExpression);
      while (dataSet.hasNext()) {
        RowRecord record = dataSet.next();
      }
      long executeLastTime = System.currentTimeMillis() - executeStartTime;
      System.out.println(executeLastTime);
      BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile, true)));
      out.write(String.valueOf(executeLastTime));
      out.write("\n");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static List<String> readPhysicalOrder(String filepath) {
    File physicalOrderFile = new File(filepath);
    if (!physicalOrderFile.exists()) {
      System.err.println(physicalOrderFile.getAbsoluteFile() + " does not exist");
      return null;
    }
    try {
      InputStream inputStream = new FileInputStream(physicalOrderFile);
      byte[] buffer = new byte[(int) physicalOrderFile.length()];
      inputStream.read(buffer);
      String dataString = new String(buffer);
      String[] measurements = dataString.split(" ");
      List<String> measurementList = Arrays.asList(measurements);
      return measurementList;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}
