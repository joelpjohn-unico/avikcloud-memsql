package com.unicosolution.connector.memsql.runtime;

import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.avikcloud.connectorsdk.common.exceptions.ConnectorException;
import com.avikcloud.connectorsdk.spi.runtime.Reader;
import com.unicosolution.connector.memsql.constants.MemSqlConnectionConstants;

public class MemSqlReader implements Reader {

	@Override
	public int deinitSession(SparkSession sparkSession) throws ConnectorException {
		return 0;
	}

	@Override
	public int initSession(SparkSession sparkSession, Map<String, Object> props) throws ConnectorException {
		return 0;
	}

	@Override
	public Dataset<Row> read(SparkSession sparkSession, Map<String, Object> sourceDetails) throws ConnectorException {
		Dataset<Row> sourceData=null;
		StringBuilder expMsg=new StringBuilder();
		String host = null;
		String port = null;
		String userName = null;
		String password = null;
		String dbName = null;
		boolean useSSL = false;
		boolean useSSLErrorFlag = false;
		String sourceTable=null;
		String srcColumns=null;
		
		Object hostObj=sourceDetails.get(MemSqlConnectionConstants.HOST);
		Object portObj=sourceDetails.get(MemSqlConnectionConstants.PORT);
		Object userNameObj=sourceDetails.get(MemSqlConnectionConstants.USER_NAME);
		Object passwordObj=sourceDetails.get(MemSqlConnectionConstants.PASSWORD);
		Object dbNameObj = sourceDetails.get(MemSqlConnectionConstants.DATABASE_NAME);
		Object useSSLObj = sourceDetails.get(MemSqlConnectionConstants.USE_SSL);
		Object sourceTableObj=sourceDetails.get(MemSqlConnectionConstants.TABLE_NAME);
		Object srcColumnsObj=sourceDetails.get(MemSqlConnectionConstants.FIELDS);
		
		if(hostObj!=null) {
			host=hostObj.toString();
		}else {
			expMsg.append(" host can not be null.");
		}
		if(portObj!=null) {
			port=portObj.toString();
		}else {
			expMsg.append(" port can not be null.");
		}
		if(userNameObj!=null) {
			userName=userNameObj.toString();
		}else {
			expMsg.append(" user name can not be null.");
		}
		if(passwordObj!=null) {
			password=passwordObj.toString();
		}else {
			expMsg.append(" password can not be null.");
		}
		if (dbNameObj != null) {
			dbName = dbNameObj.toString();
		} else {
			expMsg.append(" database name can not be null.");
		}
		if (useSSLObj != null) {
			if (useSSLObj instanceof Boolean) {
				useSSL = (boolean) useSSLObj;
			} else {
				useSSLErrorFlag = true;
				throw new IllegalArgumentException("The connection property 'useSSL' only accepts boolean values of the form: 'true' or 'false'. The value '"+ useSSLObj + "' is not a boolean.");
			}
		}
		if(sourceTableObj!=null) {
			sourceTable=sourceTableObj.toString();
		}else {
			expMsg.append(" table name can not be null.");
		}
		if(srcColumnsObj!=null) {
			srcColumns=srcColumnsObj.toString();
		}else {
			expMsg.append(" columns can not be null.");
		}
		
		if(host!=null && port != null && userName!=null && password!=null && dbName != null && !useSSLErrorFlag && sourceTable!=null && srcColumns!=null) {
			String url = MemSqlConnectionConstants.MEMSQL_URL_PREFIX + host + ":" + port + "/" + dbName + "?useSSL=" + useSSL;
			Properties props=new Properties();
			props.setProperty(MemSqlConnectionConstants.USER_NAME, userName);
			props.setProperty(MemSqlConnectionConstants.PASSWORD, password);
			props.setProperty(MemSqlConnectionConstants.DRIVER, MemSqlConnectionConstants.MEMSQL_DRIVER_CALSS_NAME);
			if(sparkSession!=null) {
				Dataset<Row> fullData=sparkSession.read().jdbc(url, sourceTable, props);
				fullData.createOrReplaceTempView("src");
				sourceData=sparkSession.sql("select "+srcColumns+" from src");
			}else {
				expMsg.append(SparkSession.class+" can not be null.");
			}
		}
		if(expMsg.length()>0) {
			throw new NullPointerException(expMsg.toString());
		}
		return sourceData;
	}
}
