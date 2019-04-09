package com.unicosolution.connector.memsql.main;

import com.avikcloud.connectorsdk.spi.connection.Connection;
import com.avikcloud.connectorsdk.spi.main.Connector;
import com.avikcloud.connectorsdk.spi.main.ConnectorInfo;
import com.avikcloud.connectorsdk.spi.metadata.MetadataBrowser;
import com.avikcloud.connectorsdk.spi.runtime.Reader;
import com.avikcloud.connectorsdk.spi.runtime.Writer;
import com.unicosolution.connector.memsql.connection.MemSqlConnection;
import com.unicosolution.connector.memsql.metadata.MemSqlMetadataBrowser;
import com.unicosolution.connector.memsql.runtime.MemSqlReader;
import com.unicosolution.connector.memsql.runtime.MemSqlWriter;

public class MemSqlConnector implements Connector {

	@Override
	public Connection getConnection() {
		return new MemSqlConnection();
	}

	@Override
	public ConnectorInfo getConnectorInfo() {
		return new ConnectorInfo() {
			
			@Override
			public String getVersion() {
				return "1.0.0";
			}
			
			@Override
			public String getName() {
				return "MySqlConnector";
			}
			
			@Override
			public String getId() {
				return "3042019";
			}
			
			@Override
			public String getDescription() {
				return "MemSql Connector built using AvikCloud Connector SDK";
			}
		};
	}

	@Override
	public MetadataBrowser getMetadataBrowser() {
		return new MemSqlMetadataBrowser();
	}

	@Override
	public Reader getReader() {
		return new MemSqlReader();
	}

	@Override
	public Writer getWriter() {
		return new MemSqlWriter();
	}

}
