package com.unicosolution.connector.memsql.metadata;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.avikcloud.connectorsdk.common.Browseable;
import com.avikcloud.connectorsdk.common.Factory;
import com.avikcloud.connectorsdk.common.MetadataProperties;
import com.avikcloud.connectorsdk.spi.connection.Connection;
import com.avikcloud.connectorsdk.spi.metadata.MetadataBrowser;
import com.unicosolution.connector.memsql.connection.MemSqlConnection;
import com.unicosolution.connector.memsql.constants.MemSqlConnectionConstants;

public class MemSqlMetadataBrowser implements MetadataBrowser {

	@Override
	public Boolean browseObjects(Connection connection, Browseable browseable, MetadataProperties metadataProps) {
		boolean flag = true;
		boolean isDB = browseable.isRoot();
		Factory factory = browseable.getFactory();
		Browseable child = null;
		int count = 0;
		try {
			if (connection != null) {
				if (connection instanceof MemSqlConnection) {
					MemSqlConnection mySqlCon = (MemSqlConnection) connection;
					java.sql.Connection nativeConnection = mySqlCon.getConnection();
					if (nativeConnection != null) {
						DatabaseMetaData dbMetaData = nativeConnection.getMetaData();
						// if browsable is a Database
						if (isDB) {
							String databaseName=mySqlCon.getDatabaseName();
							if(databaseName!=null && !databaseName.isEmpty()) {
								ResultSet tablesResultSet = dbMetaData.getTables(databaseName, null, null, new String[] { "TABLE" });
								count = 0;
								while (tablesResultSet.next()) {
									child = factory.createBrowseable(browseable);
									child.setName(tablesResultSet.getString(MemSqlConnectionConstants.TABLE_NAME));
									browseable.addChild(child);
									count++;
								}
							} else {
								// if DB name null or empty
								flag=false;
							}
						}
						// if browseable is a Table.
						else {
							String tableName = browseable.getName();
							if (tableName != null && !tableName.isEmpty()) {
								ResultSet columnsResultSet = dbMetaData.getColumns(null, null, tableName, null);
								ResultSet primaryKeysResultSet = dbMetaData.getPrimaryKeys(null, null, tableName);
								List<String> primaryKeys = new ArrayList<>();
								count = 0;
								while (primaryKeysResultSet.next()) {
									primaryKeys.add(primaryKeysResultSet.getString(MemSqlConnectionConstants.COLUMN_NAME));
								}
								while (columnsResultSet.next()) {
									child = factory.createBrowseable(browseable);
									String columnName = columnsResultSet.getString(MemSqlConnectionConstants.COLUMN_NAME);
									boolean isPrimaryKey = false;
									child.setName(columnName);
									child.getProperties().setProperty(MemSqlConnectionConstants.DATA_TYPE,columnsResultSet.getString(MemSqlConnectionConstants.DATA_TYPE));
									child.getProperties().setProperty(MemSqlConnectionConstants.COLUMN_SIZE,columnsResultSet.getString(MemSqlConnectionConstants.COLUMN_SIZE));
									child.getProperties().setProperty(MemSqlConnectionConstants.ORDINAL_POSITION,columnsResultSet.getString(MemSqlConnectionConstants.ORDINAL_POSITION));
									if (primaryKeys.contains(columnName)) {
										isPrimaryKey = true;
									}
									child.getProperties().setProperty(MemSqlConnectionConstants.IS_PRIMARY_KEY, isPrimaryKey);
									browseable.addChild(child);
									count++;
								}
							} else {
								// if table name is null or empty.
								flag = false;
							}
						}
					} else {
						// if native connection is null
						flag = false;
					}
				} else {
					// if Connection does not instance of MySqlConnection.
					flag = false;
				}
			} else {
				// if connection is null
				flag = false;
			}
			if (count <= 0) {
				// if it is wrong table or database names.
				flag = false;
			}
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		}
		return flag;
	}
}
