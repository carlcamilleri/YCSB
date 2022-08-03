/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.webservice.thespis;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;


import org.apache.http.config.SocketConfig;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import javax.ws.rs.HttpMethod;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
//import site.ycsb.StringByteIterator;
import site.ycsb.generator.UniformLongGenerator;

/**
 * Class responsible for making web service requests for benchmarking purpose.
 * Using Apache HttpClient over standard Java HTTP API as this is more flexible
 * and provides better functionality. For example HttpClient can automatically
 * handle redirects and proxy authentication which the standard Java API can't.
 */
public class ThespisClient extends DB {

  private static final String URL_PREFIXES = "url.prefixes";
  private static final String SERVER_ENDPOINTS = "server.endpoints";
  private static final String CON_TIMEOUT = "timeout.con";
  private static final String READ_TIMEOUT = "timeout.read";
  private static final String EXEC_TIMEOUT = "timeout.exec";
  private static final String LOG_ENABLED = "log.enable";
  private static final String HEADERS = "headers";
  private static final String COMPRESSED_RESPONSE = "response.compression";
  private boolean compressedResponse;
  private boolean logEnabled;
  private String[] urlPrefixes;
  private String[] serverEndpoints;
  private Properties props;
  private String[] headers;
  private static CloseableHttpClient client;
  private int conTimeout = 100000;
  private int readTimeout = 100000;
  private int execTimeout = 100000;
  private volatile Criteria requestTimedout = new Criteria(false);
  private static UniformLongGenerator serverChooser;
  private static PoolingHttpClientConnectionManager connectionManager;
  private static SocketConfig socketConfig;
  private static ReentrantLock mutex= new ReentrantLock();
  private String serverUrl;

  @Override
  public void init() throws DBException {
    props = getProperties();
    urlPrefixes = props.getProperty(URL_PREFIXES, "http://127.0.0.1:8080").split(",");
    serverEndpoints = props.getProperty(SERVER_ENDPOINTS, "http://127.0.0.1:8080").split(",");
    conTimeout = Integer.valueOf(props.getProperty(CON_TIMEOUT, "10")) * 1000;
    readTimeout = Integer.valueOf(props.getProperty(READ_TIMEOUT, "10")) * 1000;
    execTimeout = Integer.valueOf(props.getProperty(EXEC_TIMEOUT, "10")) * 1000;
    logEnabled = Boolean.valueOf(props.getProperty(LOG_ENABLED, "false").trim());
    compressedResponse = Boolean.valueOf(props.getProperty(COMPRESSED_RESPONSE, "false").trim());
    headers = props.getProperty(HEADERS, "Accept */* Content-Type application/json user-agent Mozilla/5.0 ").trim()
          .split(" ");
    setupClient();


  }

  private void setupClient() {
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder = requestBuilder.setConnectTimeout(conTimeout);
    requestBuilder = requestBuilder.setConnectionRequestTimeout(readTimeout);
    requestBuilder = requestBuilder.setSocketTimeout(readTimeout);
    if(connectionManager==null){
      mutex.lock();
      if(connectionManager==null){

        serverChooser = new UniformLongGenerator(0, serverEndpoints.length-1);
        System.out.println("Initialised connection manager");
        connectionManager = new PoolingHttpClientConnectionManager();

        connectionManager.setMaxTotal(2147483647);
        connectionManager.setDefaultMaxPerRoute(2147483647);
        socketConfig = SocketConfig.custom()
            .setSoKeepAlive(true)
            .setTcpNoDelay(true)
            .build();

        HttpClientBuilder clientBuilder = HttpClientBuilder.create().setDefaultRequestConfig(requestBuilder.build())
            .setDefaultSocketConfig(socketConfig)
            .setConnectionManager(connectionManager);
        client = clientBuilder.setConnectionManagerShared(true).build();
      }
      mutex.unlock();
    }

    serverUrl = serverEndpoints[serverChooser.nextValue().intValue()];
    System.out.println("Initialised client");
  }

  @Override
  public Status read(String table, String endpoint, Set<String> fields, Map<String, ByteIterator> result) {
    int responseCode;

    //String urlPrefix = serverEndpoints[serverChooser.nextValue().intValue()]+urlPrefixes[0];
    String urlPrefix = serverUrl+urlPrefixes[0];
    try {
      responseCode = httpGet(urlPrefix + endpoint, result);
    } catch (Exception e) {
      responseCode = handleExceptions(e, urlPrefix + endpoint, HttpMethod.GET);
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("GET Request: ").append(urlPrefix).append(endpoint)
            .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status insert(String table, String endpoint, Map<String, ByteIterator> values) {
    int responseCode;
    String urlPrefix = serverEndpoints[serverChooser.nextValue().intValue()]+urlPrefixes[0];
    try {
      responseCode = httpExecute(new HttpPost(urlPrefix + endpoint), values.get("data").toString());
    } catch (Exception e) {
      responseCode = handleExceptions(e, urlPrefix + endpoint, HttpMethod.POST);
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("POST Request: ").append(urlPrefix).append(endpoint)
            .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status delete(String table, String endpoint) {
    int responseCode;
    String urlPrefix = serverEndpoints[serverChooser.nextValue().intValue()]+urlPrefixes[0];
    try {
      responseCode = httpDelete(urlPrefix + endpoint);
    } catch (Exception e) {
      responseCode = handleExceptions(e, urlPrefix + endpoint, HttpMethod.DELETE);
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("DELETE Request: ").append(urlPrefix).append(endpoint)
            .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status update(String table, String endpoint, Map<String, ByteIterator> values) {
    int responseCode;
    String urlPrefix = serverEndpoints[serverChooser.nextValue().intValue()]+urlPrefixes[0];
    try {


      JSONObject jsonObject = new JSONObject();


      JSONArray jaValues = new JSONArray();

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        JSONObject curField = new JSONObject();
        curField.put("FieldName", entry.getKey());
        curField.put("FieldValue", entry.getValue().toString());
        jaValues.add(curField);
      }
      jsonObject.put("Values", jaValues);
//
//      StringBuilder stringBuilder = new StringBuilder("{\"Values\":[");
//      values.forEach((key, value) -> stringBuilder.append("{\"FieldName\":\"")
//          .append(key).append("\",\"FieldValue\":\"").append(value).append("\"}"));
//      stringBuilder.append("]}");
//      responseCode = httpExecute(new HttpPut(urlPrefix + endpoint), stringBuilder.toString());
      responseCode = httpExecute(new HttpPut(urlPrefix + endpoint), jsonObject.toJSONString());
    } catch (Exception e) {
      responseCode = handleExceptions(e, urlPrefix + endpoint, HttpMethod.PUT);
    }
    if (logEnabled) {
      System.err.println(new StringBuilder("PUT Request: ").append(urlPrefix).append(endpoint)
            .append(" | Response Code: ").append(responseCode).toString());
    }
    return getStatus(responseCode);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  // Maps HTTP status codes to YCSB status codes.
  private Status getStatus(int responseCode) {
    int rc = responseCode / 100;
    if (responseCode == 400) {
      return Status.BAD_REQUEST;
    } else if (responseCode == 403) {
      return Status.FORBIDDEN;
    } else if (responseCode == 404) {
      return Status.NOT_FOUND;
    } else if (responseCode == 501) {
      return Status.NOT_IMPLEMENTED;
    } else if (responseCode == 503) {
      return Status.SERVICE_UNAVAILABLE;
    } else if (rc == 5) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  private int handleExceptions(Exception e, String url, String method) {
    //if (logEnabled || true)
    //{
    System.err.println(new StringBuilder(method).append(" Request: ").append(url).append(" | ")
        .append(e.getClass().getName()).append(" occured | Error message: ")
        .append(e.getMessage()).toString());
   // }
      
    if (e instanceof ClientProtocolException) {
      return 400;
    }
    return 500;
  }

  // Connection is automatically released back in case of an exception.
  private int httpGet(String endpoint, Map<String, ByteIterator> result) throws IOException {
    requestTimedout.setIsSatisfied(false);
    //Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    //timer.start();
    int responseCode = 200;
    HttpGet request = new HttpGet(endpoint);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }

    CloseableHttpResponse response = client.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    HttpEntity entity = response.getEntity();
    EntityUtils.consumeQuietly(entity);
    //HttpEntity responseEntity = response.getEntity();
//    // If null entity don't bother about connection release.
//    if (responseEntity != null) {
//      InputStream stream = responseEntity.getContent();
//      /*
//       * TODO: Gzip Compression must be supported in the future. Header[]
//       * header = response.getAllHeaders();
//       * if(response.getHeaders("Content-Encoding")[0].getValue().contains
//       * ("gzip")) stream = new GZIPInputStream(stream);
//       */
//      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
//      StringBuffer responseContent = new StringBuffer();
//      String line = "";
//      while ((line = reader.readLine()) != null) {
//        if (requestTimedout.isSatisfied()) {
//          // Must avoid memory leak.
//          reader.close();
//          stream.close();
//          EntityUtils.consumeQuietly(responseEntity);
//          response.close();
//          client.close();
//          throw new TimeoutException();
//        }
//        responseContent.append(line);
//      }
//      timer.interrupt();
//      result.put("response", new StringByteIterator(responseContent.toString()));
//      // Closing the input stream will trigger connection release.
//      stream.close();
//    }
//    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    request.releaseConnection();
    //client.close();

    //client.close();

    return responseCode;
  }

  private int httpExecute(HttpEntityEnclosingRequestBase request, String data) throws IOException {
    requestTimedout.setIsSatisfied(false);
    //Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    //timer.start();
    int responseCode = 200;
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data.getBytes()),
          ContentType.APPLICATION_JSON);
    reqEntity.setChunked(true);
    request.setEntity(reqEntity);
    CloseableHttpResponse response = client.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    HttpEntity responseEntity = response.getEntity();
    // If null entity don't bother about connection release.
//    if (responseEntity != null) {
//      InputStream stream = responseEntity.getContent();
//      if (compressedResponse) {
//        stream = new GZIPInputStream(stream);
//      }
//      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
//      StringBuffer responseContent = new StringBuffer();
//      String line = "";
//      while ((line = reader.readLine()) != null) {
//        if (requestTimedout.isSatisfied()) {
//          // Must avoid memory leak.
//          reader.close();
//          stream.close();
//          EntityUtils.consumeQuietly(responseEntity);
//          response.close();
//          client.close();
//          throw new TimeoutException();
//        }
//        responseContent.append(line);
//      }
//      timer.interrupt();
//      // Closing the input stream will trigger connection release.
//      stream.close();
//    }
    EntityUtils.consumeQuietly(responseEntity);
    response.close();
    request.releaseConnection();
    client.close();
    return responseCode;
  }
  
  private int httpDelete(String endpoint) throws IOException {
    requestTimedout.setIsSatisfied(false);
    Thread timer = new Thread(new Timer(execTimeout, requestTimedout));
    timer.start();
    int responseCode = 200;
    HttpDelete request = new HttpDelete(endpoint);
    for (int i = 0; i < headers.length; i = i + 2) {
      request.setHeader(headers[i], headers[i + 1]);
    }
    CloseableHttpResponse response = client.execute(request);
    responseCode = response.getStatusLine().getStatusCode();
    response.close();
    //client.close();
    return responseCode;
  }


  @Override
  public void cleanup() throws DBException {
    super.cleanup();

  }


  /**
   * Marks the input {@link Criteria} as satisfied when the input time has elapsed.
   */
  class Timer implements Runnable {

    private long timeout;
    private Criteria timedout;

    public Timer(long timeout, Criteria timedout) {
      this.timedout = timedout;
      this.timeout = timeout;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(timeout);
        this.timedout.setIsSatisfied(true);
      } catch (InterruptedException e) {
        // Do nothing.
      }
    }


  }

  /**
   * Sets the flag when a criteria is fulfilled.
   */
  class Criteria {

    private boolean isSatisfied;

    public Criteria(boolean isSatisfied) {
      this.isSatisfied = isSatisfied;
    }

    public boolean isSatisfied() {
      return isSatisfied;
    }

    public void setIsSatisfied(boolean satisfied) {
      this.isSatisfied = satisfied;
    }

  }

  /**
   * Private exception class for execution timeout.
   */
  class TimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    
    public TimeoutException() {
      super("HTTP Request exceeded execution time limit.");
    }

  }

}
