package com.unicosolution.connector.memsql.connection;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import com.avikcloud.connectorsdk.common.ConnectionProperties;
import com.avikcloud.connectorsdk.common.objects.ConnectionStatus;
import com.avikcloud.connectorsdk.common.objects.ConnectionStatusEnum;
import com.avikcloud.connectorsdk.spi.connection.Connection;
import com.unicosolution.connector.memsql.constants.MemSqlConnectionConstants;

public class MemSqlConnection implements Connection {

	private java.sql.Connection connection;
	private String databaseName;

	public java.sql.Connection getConnection() {
		return connection;
	}
	

	public String getDatabaseName() {
		return databaseName;
	}

	@Override
	public void init(Map<String, Object> config) {

	}

	@Override
	public ConnectionStatus open(ConnectionProperties connectionProperties) {

		ConnectionStatusEnum statusEnum = ConnectionStatusEnum.FAILURE;
		java.sql.Connection con = null;
		Exception exception = null;
		StringBuilder expMsg = new StringBuilder();
		String message = "";
		String host = null;
		String port = null;
		String userName = null;
		String password = null;
		String dbName = null;
		boolean useSSL = false;
		boolean useSSLErrorFlag = false;

		Object hostObj = connectionProperties.getProperty(MemSqlConnectionConstants.HOST);
		Object portObj = connectionProperties.getProperty(MemSqlConnectionConstants.PORT);
		Object userNameObj = connectionProperties.getProperty(MemSqlConnectionConstants.USER_NAME);
		Object passwordObj = connectionProperties.getProperty(MemSqlConnectionConstants.PASSWORD);
		Object dbNameObj = connectionProperties.getProperty(MemSqlConnectionConstants.DATABASE_NAME);
		Object useSSLObj = connectionProperties.getProperty(MemSqlConnectionConstants.USE_SSL);

		if (hostObj != null) {
			host = hostObj.toString();
		} else {
			expMsg.append(" Invalid host ");
		}
		if (portObj != null) {
			port = portObj.toString();
		} else {
			expMsg.append(" Invalid port ");
		}
		if (userNameObj != null) {
			userName = userNameObj.toString();
		} else {
			expMsg.append(" Invalid username ");
		}
		if (passwordObj != null) {
			password =passwordObj.toString();
		} else {
			expMsg.append(" Invalid password ");
		}
		if (dbNameObj != null) {
			dbName = dbNameObj.toString();
		} else {
			expMsg.append(" Invalid database name ");
		}
		if (useSSLObj != null) {
			if (useSSLObj instanceof Boolean) {
				useSSL = (boolean) useSSLObj;
			} else {
				useSSLErrorFlag = true;
				expMsg.append("The connection property 'useSSL' only accepts boolean values of the form: 'true' or 'false'. The value "+ useSSLObj + " is not a boolean.");
			}
		}
		if (host != null && port != null && userName != null && password != null && dbName != null && !useSSLErrorFlag) {
			String url = MemSqlConnectionConstants.MEMSQL_URL_PREFIX + host + ":" + port + "/" + dbName + "?user=" + userName
					+ "&password=" + password + "&useSSL=" + useSSL;
			try {
				Class.forName(MemSqlConnectionConstants.MEMSQL_DRIVER_CALSS_NAME);
				con = DriverManager.getConnection(url);
			} catch (Exception excp) {
				excp.printStackTrace();
				exception = excp;
			}
		}else if(expMsg.length() > 0) {
			exception = new IllegalArgumentException(expMsg.toString());
		}
		if (con != null) {
			statusEnum = ConnectionStatusEnum.SUCCESS;
			message = "MemSql connection opened successfully";
			this.connection = con;
			this.databaseName=dbName;
		} else {
			message = "MemSql connection failed";
		}
		return prepareConnectionStatus(statusEnum, message, exception);
	}

	@Override
	public ConnectionStatus close() {
		ConnectionStatusEnum statusEnum = ConnectionStatusEnum.FAILURE;
		Exception exception=null;
		String message=null;
		if(this.connection!=null) {
			try {
				this.connection.close();
				message="MemSql connection closed successfully";
				statusEnum=ConnectionStatusEnum.SUCCESS;
			} catch (SQLException e) {
				message="Failed to close MemSql connection.";
				e.printStackTrace();
				exception=e;
			}
		}else {
			message="MemSql connection is not opened to close it.";
		}
		
		return prepareConnectionStatus(statusEnum, message, exception);
	}

	@Override
	public boolean isAlive() {
		boolean bAlive = false;
		try {
			bAlive = (null != this.connection && this.connection.isValid(0));
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return bAlive;
	}
	
	private ConnectionStatus prepareConnectionStatus(ConnectionStatusEnum statusEnum,String message,Exception exception) {
		ConnectionStatus connectionStatus = null;
		if (exception != null) {
			connectionStatus = new ConnectionStatus(statusEnum, message, exception);
		} else {
			connectionStatus = new ConnectionStatus(statusEnum, message);
		}
		return connectionStatus;
	}
}
