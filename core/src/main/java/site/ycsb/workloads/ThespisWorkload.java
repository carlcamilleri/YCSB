/**
 * Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.generator.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Typical RESTFul services benchmarking scenario. Represents a set of client
 * calling REST operations like HTTP DELETE, GET, POST, PUT on a web service.
 * This scenario is completely different from CoreWorkload which is mainly
 * designed for databases benchmarking. However due to some reusable
 * functionality this class extends {@link CoreWorkload} and overrides necessary
 * methods like init, doTransaction etc.
 */
public class ThespisWorkload extends CoreWorkload {

  /**
   * The name of the property for the proportion of transactions that are
   * delete.
   */
  public static final String DELETE_PROPORTION_PROPERTY = "deleteproportion";

  /**
   * The default proportion of transactions that are delete.
   */
  public static final String DELETE_PROPORTION_PROPERTY_DEFAULT = "0.00";

  /**
   * The name of the property for the file that holds the field length size for insert operations.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_FILE_PROPERTY = "fieldlengthdistfile";

  /**
   * The default file name that holds the field length size for insert operations.
   */
  public static final String FIELD_LENGTH_DISTRIBUTION_FILE_PROPERTY_DEFAULT = "fieldLengthDistFile.txt";

  /**
   * In web services even though the CRUD operations follow the same request
   * distribution, they have different traces and distribution parameter
   * values. Hence configuring the parameters of these operations separately
   * makes the benchmark more flexible and capable of generating better
   * realistic workloads.
   */
  // Read related properties.
  private static final String READ_TRACE_FILE = "url.trace.read";
  private static final String READ_TRACE_FILE_DEFAULT = "readtrace.txt";
  private static final String READ_ZIPFIAN_CONSTANT = "readzipfconstant";
  private static final String READ_ZIPFIAN_CONSTANT_DEAFULT = "0.99";
  private static final String READ_RECORD_COUNT_PROPERTY = "readrecordcount";
  // Insert related properties.
  private static final String INSERT_TRACE_FILE = "url.trace.insert";
  private static final String INSERT_TRACE_FILE_DEFAULT = "inserttrace.txt";
  private static final String INSERT_ZIPFIAN_CONSTANT = "insertzipfconstant";
  private static final String INSERT_ZIPFIAN_CONSTANT_DEAFULT = "0.99";
  private static final String INSERT_SIZE_ZIPFIAN_CONSTANT = "insertsizezipfconstant";
  private static final String INSERT_SIZE_ZIPFIAN_CONSTANT_DEAFULT = "0.99";
  private static final String INSERT_RECORD_COUNT_PROPERTY = "insertrecordcount";
  // Delete related properties.
  private static final String DELETE_TRACE_FILE = "url.trace.delete";
  private static final String DELETE_TRACE_FILE_DEFAULT = "deletetrace.txt";
  private static final String DELETE_ZIPFIAN_CONSTANT = "deletezipfconstant";
  private static final String DELETE_ZIPFIAN_CONSTANT_DEAFULT = "0.99";
  private static final String DELETE_RECORD_COUNT_PROPERTY = "deleterecordcount";
  // Delete related properties.
  private static final String UPDATE_TRACE_FILE = "url.trace.update";
  private static final String UPDATE_TRACE_FILE_DEFAULT = "updatetrace.txt";
  private static final String UPDATE_ZIPFIAN_CONSTANT = "updatezipfconstant";
  private static final String UPDATE_ZIPFIAN_CONSTANT_DEAFULT = "0.99";
  private static final String UPDATE_RECORD_COUNT_PROPERTY = "updaterecordcount";

  private Map<Integer, String> readUrlMap;
  private Map<Integer, String> insertUrlMap;
  private Map<Integer, String> deleteUrlMap;
  private Map<Integer, String> updateUrlMap;
  private int readRecordCount;
  private int insertRecordCount;
  private int deleteRecordCount;
  private int updateRecordCount;
  private NumberGenerator readKeyChooser;
  private NumberGenerator insertKeyChooser;
  private NumberGenerator deleteKeyChooser;
  private NumberGenerator updateKeyChooser;
  private NumberGenerator fieldlengthgenerator;
  private DiscreteGenerator operationchooser;
  private List<String> fieldnames;

  @Override
  public void init(Properties p) throws WorkloadException {

    readRecordCount = Integer.parseInt(p.getProperty(READ_RECORD_COUNT_PROPERTY, String.valueOf(Integer.MAX_VALUE)));
    insertRecordCount = Integer
      .parseInt(p.getProperty(INSERT_RECORD_COUNT_PROPERTY, String.valueOf(Integer.MAX_VALUE)));
    deleteRecordCount = Integer
      .parseInt(p.getProperty(DELETE_RECORD_COUNT_PROPERTY, String.valueOf(Integer.MAX_VALUE)));
    updateRecordCount = Integer
      .parseInt(p.getProperty(UPDATE_RECORD_COUNT_PROPERTY, String.valueOf(Integer.MAX_VALUE)));

    readUrlMap = getTrace(p.getProperty(READ_TRACE_FILE, READ_TRACE_FILE_DEFAULT), readRecordCount);
    insertUrlMap = getTrace(p.getProperty(INSERT_TRACE_FILE, INSERT_TRACE_FILE_DEFAULT), insertRecordCount);
    deleteUrlMap = getTrace(p.getProperty(DELETE_TRACE_FILE, DELETE_TRACE_FILE_DEFAULT), deleteRecordCount);
    updateUrlMap = getTrace(p.getProperty(UPDATE_TRACE_FILE, UPDATE_TRACE_FILE_DEFAULT), updateRecordCount);

    operationchooser = createOperationGenerator(p);

    String requestdistrib =
        p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
    int minscanlength =
        Integer.parseInt(p.getProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_PROPERTY_DEFAULT));
    int maxscanlength =
        Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
    String scanlengthdistrib =
        p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);


    writeallfields = Boolean.parseBoolean(
        p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));



    // Common distribution for all operations.
    String requestDistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);

    double readZipfconstant = Double.parseDouble(p.getProperty(READ_ZIPFIAN_CONSTANT, READ_ZIPFIAN_CONSTANT_DEAFULT));
    readKeyChooser = getKeyChooser(requestDistrib, readUrlMap.size(), readZipfconstant, p);
    double updateZipfconstant = Double
        .parseDouble(p.getProperty(UPDATE_ZIPFIAN_CONSTANT, UPDATE_ZIPFIAN_CONSTANT_DEAFULT));
    updateKeyChooser = getKeyChooser(requestDistrib, updateUrlMap.size(), updateZipfconstant, p);
    double insertZipfconstant = Double
        .parseDouble(p.getProperty(INSERT_ZIPFIAN_CONSTANT, INSERT_ZIPFIAN_CONSTANT_DEAFULT));
    insertKeyChooser = getKeyChooser(requestDistrib, insertUrlMap.size(), insertZipfconstant, p);
    double deleteZipfconstant = Double
        .parseDouble(p.getProperty(DELETE_ZIPFIAN_CONSTANT, DELETE_ZIPFIAN_CONSTANT_DEAFULT));
    deleteKeyChooser = getKeyChooser(requestDistrib, deleteUrlMap.size(), deleteZipfconstant, p);

    fieldlengthgenerator = getFieldLengthGenerator(p);

    fieldcount =
        Long.parseLong(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    final String fieldnameprefix = p.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
    fieldnames = new ArrayList<>();
    for (int i = 1; i <= fieldcount; i++) {
      fieldnames.add(fieldnameprefix + i);

    }
    System.out.println(fieldnames.size());
    fieldlengthgenerator = CoreWorkload.getFieldLengthGenerator(p);

    fieldchooser = new UniformLongGenerator(0, fieldcount-1);

    if (scanlengthdistrib.compareTo("uniform") == 0) {
      scanlength = new UniformLongGenerator(minscanlength, maxscanlength);
    } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
      scanlength = new ZipfianGenerator(minscanlength, maxscanlength);
    } else {
      throw new WorkloadException(
          "Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
    }


  }

  public static DiscreteGenerator createOperationGenerator(final Properties p) {
    // Re-using CoreWorkload method.
    final DiscreteGenerator operationChooser = CoreWorkload.createOperationGenerator(p);
    // Needs special handling for delete operations not supported in CoreWorkload.
    double deleteproportion = Double
        .parseDouble(p.getProperty(DELETE_PROPORTION_PROPERTY, DELETE_PROPORTION_PROPERTY_DEFAULT));
    if (deleteproportion > 0) {
      operationChooser.addValue(deleteproportion, "DELETE");
    }
    return operationChooser;
  }

  private static NumberGenerator getKeyChooser(String requestDistrib, int recordCount, double zipfContant,
                                               Properties p) throws WorkloadException {
    NumberGenerator keychooser;

    switch (requestDistrib) {
    case "exponential":
      double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keychooser = new ExponentialGenerator(percentile, recordCount * frac);
      break;
    case "uniform":
      keychooser = new UniformLongGenerator(0, recordCount - 1);
      break;
    case "zipfian":
      keychooser = new ZipfianGenerator(recordCount, zipfContant);
      break;
    case "latest":
      throw new WorkloadException("Latest request distribution is not supported for RestWorkload.");
    case "hotspot":
      double hotsetfraction = Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction = Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(0, recordCount - 1, hotsetfraction, hotopnfraction);
      break;
    default:
      throw new WorkloadException("Unknown request distribution \"" + requestDistrib + "\"");
    }
    return keychooser;
  }

  protected static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
    // Re-using CoreWorkload method. 
    NumberGenerator fieldLengthGenerator = CoreWorkload.getFieldLengthGenerator(p);
    String fieldlengthdistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
        FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
    // Needs special handling for Zipfian distribution for variable Zipf Constant.
    if (fieldlengthdistribution.compareTo("zipfian") == 0) {
      int fieldlength = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
      double insertsizezipfconstant = Double
          .parseDouble(p.getProperty(INSERT_SIZE_ZIPFIAN_CONSTANT, INSERT_SIZE_ZIPFIAN_CONSTANT_DEAFULT));
      fieldLengthGenerator = new ZipfianGenerator(1, fieldlength, insertsizezipfconstant);
    }
    return fieldLengthGenerator;
  }

  /**
   * Reads the trace file and returns a URL map.
   */
  private static Map<Integer, String> getTrace(String filePath, int recordCount)
    throws WorkloadException {
    Map<Integer, String> urlMap = new HashMap<Integer, String>();
    int count = 0;
    String line;
    try {
      FileReader inputFile = new FileReader(filePath);
      BufferedReader bufferReader = new BufferedReader(inputFile);
      while ((line = bufferReader.readLine()) != null) {
        urlMap.put(count++, line.trim());
        if (count >= recordCount) {
          break;
        }
      }
      bufferReader.close();
    } catch (IOException e) {
      throw new WorkloadException(
        "Error while reading the trace. Please make sure the trace file path is correct. "
          + e.getLocalizedMessage());
    }
    return urlMap;
  }

  /**
   * Not required for Rest Clients as data population is service specific.
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    return false;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if (operation == null) {
      return false;
    }

    switch (operation) {
    case "UPDATE":
      doTransactionUpdate(db);
      break;
    case "INSERT":
      doTransactionInsert(db);
      break;
    case "DELETE":
      doTransactionDelete(db);
      break;
    default:
      doTransactionRead(db);
    }
    return true;
  }

  public int doTransactions(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if (operation == null) {
      return -1;
    }

    switch (operation) {
      case "UPDATE":
        return doTransactionsUpdate(db);

      case "INSERT":
        doTransactionInsert(db);
        break;
      case "DELETE":
        doTransactionDelete(db);
        break;
      default:
        return doTransactionsRead(db,threadstate);
    }
    return 1;
  }

  /**
   * Returns next URL to be called.
   */
  private String getNextURL(int opType) {
    if (opType == 1) {
      return readUrlMap.get(readKeyChooser.nextValue().intValue());
    } else if (opType == 2) {
      return insertUrlMap.get(insertKeyChooser.nextValue().intValue());
    } else if (opType == 3) {
      return deleteUrlMap.get(deleteKeyChooser.nextValue().intValue());
    } else {
      return updateUrlMap.get(updateKeyChooser.nextValue().intValue());
    }
  }

  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    return new ArrayList<CompletableFuture<Status>>();
  }



  @Override
  public void doTransactionRead(DB db) {
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();


    db.read(null, getNextURL(1), null, result);
  }


  public Integer doTransactionsRead(DB db, Object threadState) {
    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    Integer res = 0;
    ArrayList<CompletableFuture<Status>> futuresRead = ((ArrayList<CompletableFuture<Status>>) threadState);
    for (int i = 0; i < 32-futuresRead.size(); i++) {
      futuresRead.add(db.readAsync(null, getNextURL(1), null, result));
    }
    while(futuresRead.size()>=32) {
      CompletableFuture[] curFutures = futuresRead.stream().filter(Objects::nonNull).toArray(CompletableFuture[]::new);
      CompletableFuture.anyOf(curFutures).join();
//      try {
//        Thread.sleep(0, 10);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }

      for (int i = 0; i < futuresRead.size(); i++) {
        CompletableFuture<Status> curFut = futuresRead.get(i);
        if (curFut!=null && curFut.isDone()) {
          Status s = null;
          try {
            s = curFut.get();
          } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
          }
          assert s != null;
          if (s.isOk())
            res++;
          futuresRead.remove(i);
          //i--;
        }
      }

    }

    //ArrayList<CompletableFuture<Status>> lstRes = new ArrayList<>();
    for (int i = 0; i < 32-futuresRead.size(); i++) {
      futuresRead.add(db.readAsync(null, getNextURL(1), null, result));
    }
//
//    for (CompletableFuture<Status> c : lstRes
//    ) {
//
//      try {
//        Status s = c.get(10, TimeUnit.SECONDS);
//        if (s.isOk())
//          res++;
//      } catch (InterruptedException | ExecutionException | TimeoutException e) {
//        e.printStackTrace();
//      }
//
//    }
    return res;
  }

  @Override
  public void doTransactionInsert(DB db) {
    HashMap<String, ByteIterator> value = new HashMap<String, ByteIterator>();
    // Create random bytes of insert data with a specific size.
    value.put("data", new RandomByteIterator(fieldlengthgenerator.nextValue().longValue()));
    db.insert(null, getNextURL(2), value);
  }

  public void doTransactionDelete(DB db) {
    db.delete(null, getNextURL(3));
  }

  @Override
  public void doTransactionUpdate(DB db) {
    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues();
    } else {
      // update a random field
      values = buildSingleValue();
    }

    db.update(null, getNextURL(4), values);
  }


  public int doTransactionsUpdate(DB db) {


    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues();
    } else {
      // update a random field
      values = buildSingleValue();
    }

    ArrayList<CompletableFuture<Status>> lstRes = new ArrayList<>();
    for (int i = 0; i < 32; i++) {


      lstRes.add(db.updateAsync(null, getNextURL(4), values));
    }

    int res=0;
    for (CompletableFuture<Status> c : lstRes
    ) {

      try {
        Status s = c.get(10, TimeUnit.SECONDS);
        if (s.isOk())
          res++;
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        e.printStackTrace();
      }

    }
    return res;


  }



  /**
   * Builds a value for a randomly chosen field.
   */
  private HashMap<String, ByteIterator> buildSingleValue() {
    HashMap<String, ByteIterator> value = new HashMap<>();

    String fieldkey = fieldnames.get(fieldchooser.nextValue().intValue());
    ByteIterator data;
    data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
    value.put(fieldkey, data);

    return value;
  }

  /**
   * Builds values for all fields.
   */
  private HashMap<String, ByteIterator> buildValues() {
    HashMap<String, ByteIterator> values = new HashMap<>();

    for (String fieldkey : fieldnames) {
      ByteIterator data;
      data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
      values.put(fieldkey, data);
    }
    return values;
  }

}
