package com.unicosolution.connector.memsql.runtime;

import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.avikcloud.connectorsdk.common.exceptions.ConnectorException;
import com.avikcloud.connectorsdk.spi.runtime.Writer;
import com.unicosolution.connector.memsql.constants.MemSqlConnectionConstants;

public class MemSqlWriter implements Writer {

	@Override
	public int deinitSession(SparkSession sparkSession) throws ConnectorException {
		return 0;
	}

	@Override
	public int initSession(SparkSession sparkSession, Map<String, Object> targetDetails) throws ConnectorException {
		return 0;
	}

	@Override
	public int write(SparkSession sparkSession, Map<String, Object> targetDetails, Dataset<Row> sourceData) throws ConnectorException {
		int count=0;
		StringBuilder expMsg=new StringBuilder();
		String host = null;
		String port = null;
		String userName = null;
		String password = null;
		String dbName = null;
		boolean useSSL = false;
		boolean useSSLErrorFlag = false;
		String targetTableName=null;
		String targetColumns=null;
		
		Object hostObj=targetDetails.get(MemSqlConnectionConstants.HOST);
		Object portObj=targetDetails.get(MemSqlConnectionConstants.PORT);
		Object userNameObj=targetDetails.get(MemSqlConnectionConstants.USER_NAME);
		Object passwordObj=targetDetails.get(MemSqlConnectionConstants.PASSWORD);
		Object dbNameObj = targetDetails.get(MemSqlConnectionConstants.DATABASE_NAME);
		Object useSSLObj = targetDetails.get(MemSqlConnectionConstants.USE_SSL);
		Object targetTableNameObj=targetDetails.get(MemSqlConnectionConstants.TABLE_NAME);
		Object targetColumnsObj=targetDetails.get(MemSqlConnectionConstants.FIELDS);
		
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
		if(targetTableNameObj!=null) {
			targetTableName=targetTableNameObj.toString();
		}else {
			expMsg.append("  table name can not be null.");
		}
		if(targetColumnsObj!=null) {
			targetColumns=targetColumnsObj.toString();
		}else {
			expMsg.append(" columns can not be null.");
		}
		if(host!=null && port != null && userName!=null && password!=null && dbName != null && !useSSLErrorFlag && targetTableName!=null && targetColumns!=null) {
			String url = MemSqlConnectionConstants.MEMSQL_URL_PREFIX + host + ":" + port + "/" + dbName + "?useSSL=" + useSSL;
			Properties props=new Properties();
			props.setProperty(MemSqlConnectionConstants.USER_NAME, userName);
			props.setProperty(MemSqlConnectionConstants.PASSWORD, password);
			props.setProperty(MemSqlConnectionConstants.DRIVER, MemSqlConnectionConstants.MEMSQL_DRIVER_CALSS_NAME);
			if(sourceData!=null) {
				sourceData.write().mode(SaveMode.Overwrite).jdbc(url, targetTableName, props);
				count=1;
			}else {
				expMsg.append(Dataset.class+" can not be null.");
			}
		}
		if(expMsg.length()>0) {
			throw new NullPointerException(expMsg.toString());
		}
		return count;
	}

}
