package com.ak.hive.querygrabber.hook;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TestStubHook implements ExecuteWithHookContext {
	private static final Log LOG = LogFactory.getLog(TestStubHook.class.getName());
	private static final Object LOCK = new Object();
	private static ExecutorService executor;
	private enum EventTypes {
		QUERY_SUBMITTED, QUERY_COMPLETED
	};
	private enum OtherInfoTypes {
		QUERY, STATUS, TEZ, MAPRED
	};
	private enum PrimaryFilterTypes {
		user, requestuser, operationid
	};
	private static final int WAIT_TIME = 3;
	private static final List<String> DDL_START_WORDS = Arrays
			.asList(new String[] { "CREATE", "ALTER", "DROP" });
	private static final List<String> DB_START_WORDS = Arrays
			.asList(new String[] { "DATABASE", "SCHEMA" });
	private static final String HIVEHOOK_QUERY_SKIP_PATTERN = "hivehook.query.skipPattern";
	
public TestStubHook() {
    synchronized(LOCK) {
      if (executor == null) {
        executor = Executors.newSingleThreadExecutor(
           new ThreadFactoryBuilder().setDaemon(true).setNameFormat("QueryHook Logger %d").build());
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            try {
              executor.shutdown();
              executor.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
              executor = null;
            } catch(InterruptedException ie) { }
          }
        });
      }
    }
    debugLog("Created Query Hook");
  }

  @Override
  public void run(final HookContext hookContext) throws Exception {
    final long currentTime = System.currentTimeMillis();
    HiveConf configuration = new HiveConf(hookContext.getConf());
    executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            QueryPlan plan = hookContext.getQueryPlan();
            if (plan == null) {
              return;
            }
            String operationName=hookContext.getOperationName();
            String opId = hookContext.getOperationId();
            long queryStartTime = plan.getQueryStartTime();
            String user = hookContext.getUgi().getUserName();
            String requestuser = hookContext.getUserName() == null ? user : hookContext.getUserName();
            int numMrJobs = Utilities.getMRTasks(plan.getRootTasks()).size();
            int numTezJobs = Utilities.getTezTasks(plan.getRootTasks()).size();
            String queryId = plan.getQueryId();
            String queryString = plan.getQueryStr();   
	    boolean skip = false;
	    for (String pattern : configuration.get(HIVEHOOK_QUERY_SKIP_PATTERN, "").split(",")) {
		pattern = pattern.replaceAll("\\s+", "").toUpperCase();
		if (pattern.length() > 0 && !skip) {
		 if (queryString != null&& queryString.replaceAll("\\s+", "").toUpperCase().startsWith(pattern)) {
		  debugLog("Query matches skip pattern. Will not be sent to kafka.");
		  skip = true;
		  break;
		}
	       }
	     }
		if (!skip) {
	            switch(hookContext.getHookType()) {
	            case PRE_EXEC_HOOK:
	              sendNotification(generatePreExecNotification(queryId,
	                   queryStartTime, user, requestuser, numMrJobs, numTezJobs, opId,queryString));
	              break;
	            case POST_EXEC_HOOK:
	              sendNotification(generatePostExecNotification(operationName,queryId, currentTime, user, requestuser, true, opId,queryString,hookContext.getOutputs(), hookContext.getInputs()));
	              break;
	            case ON_FAILURE_HOOK:
	              sendNotification(generatePostExecNotification(operationName,queryId, currentTime, user, requestuser , false, opId, queryString,hookContext.getOutputs(), hookContext.getInputs()));
	              break;
	            default:
	              break;
	            }
			}
            
          } catch (Exception e) {
        	  debugLog("Failed to submit plan: "+ StringUtils.stringifyException(e));
          }
        }
      });
  }

  String generatePreExecNotification(String queryId,
      long startTime, String user, String requestuser, int numMrJobs, int numTezJobs, String opId, String queryString) throws Exception {

	  
    JSONObject queryObj = new JSONObject();
    queryObj.put("hookType", "pre");
    queryObj.put("queryText", queryString);
    
    
    debugLog("Received pre-hook notification for :" + queryId);
    debugLog("Otherinfo: " + queryObj.toString());
    debugLog("Operation id: <" + opId + ">");
   

    
    queryObj.put("queryId", queryId);
    queryObj.put(PrimaryFilterTypes.user.name(), user);
    queryObj.put(PrimaryFilterTypes.requestuser.name(), requestuser);
    
    if (opId != null) {
    	queryObj.put(PrimaryFilterTypes.operationid.name(), opId);
    }
    queryObj.put("eventType", EventTypes.QUERY_SUBMITTED.name());
    queryObj.put("eventTimestamp", startTime);
    queryObj.put(OtherInfoTypes.TEZ.name(), numTezJobs > 0);
    queryObj.put(OtherInfoTypes.MAPRED.name(), numMrJobs > 0);
    
   
    
    return queryObj.toString();
  }

  String generatePostExecNotification(String operationName,String queryId, long stopTime, String user, String requestuser, boolean success,
      String opId, String queryString, Set<WriteEntity> outputs, Set<ReadEntity> inputs) throws JSONException {
   
    JSONObject queryObj = new JSONObject();
    queryObj.put("hookType", success==true?"post":"fail");
    queryObj.put("queryId", queryId);
    queryObj.put("queryText", queryString);
    queryObj.put("isDDL", false);
    queryObj.put(PrimaryFilterTypes.user.name(), user);
    queryObj.put(PrimaryFilterTypes.requestuser.name(), requestuser);
    if (opId != null) {
    	queryObj.put(PrimaryFilterTypes.operationid.name(), opId);
    }
    queryObj.put("eventType",EventTypes.QUERY_COMPLETED.name());
    queryObj.put("eventTimestamp",stopTime);
    queryObj.put(OtherInfoTypes.STATUS.name(), success);
    
    if(success&&isDDL(queryString)){
    	String dbName=null;
    	String tableName=null;
    	queryObj.put("isDDL", true);
    	queryObj.put("dump_time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
    	
    	if(isDBCommand(queryString)){
    		System.out.println("DBCommand");
    		for(WriteEntity output:outputs){
    			dbName=dbName==null?(output.getDatabase() == null ? null : output.getDatabase().getName()):dbName;
    		}
    		tableName="";
    	}
    	
    	if(dbName==null){
    		Table table = null;
    		if(operationName.equalsIgnoreCase("ALTERTABLE_RENAME")){
    			for(WriteEntity output:outputs){
    				if(output.getTable()!=null){
    					table = output.getTable();
    				}
    			}
    		}
    		else{
        		for(WriteEntity output:outputs){
        		table = table==null?(output.getTable()!=null?output.getTable():null):table;
        		}
    		}
    		try{
    			//table is null for macros and functions. Expecting a null pointer in that case
    			tableName = table.getTableName();
    			dbName = table.getDbName();
    		}catch(Exception e){
    			tableName = tableName!=null?tableName:"";
    			dbName=dbName!=null?dbName:"";
    			//ignore
    			debugLog("Error processing query "+queryId+" exception trace "+getTraceString(e));
    		}
    	}
    	queryObj.put("db_name", dbName);
    	queryObj.put("table_name", tableName);
    }
   
    
    return queryObj.toString();
  }

 void sendNotification(String notificationMessage) throws Exception {
	  notificationMessage = notificationMessage.trim().replace("\n", " ").replace("\r", " ").replaceAll(" +", " ").trim();
	  System.out.println("Notif Message "+notificationMessage);
  }
  
	

private boolean isDDL(String query) {
		try{
			return DDL_START_WORDS.contains((query.trim().toUpperCase().split(" "))[0]);
		}catch(Exception e){}
		return false;
	}
  
  private boolean isDBCommand(String query) {
		try{
			return DB_START_WORDS.contains(((query.trim().toUpperCase().split(" "))[1]).trim());
		}catch(Exception e){}
		return false;
	}
  

  
	private String getTraceString(Exception e) {
		StringWriter sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}
	
	private void debugLog(String message){
		 if (LOG.isDebugEnabled()) {
			 LOG.debug(message);
		 }
	}
	
	
}
