package com.unicosolution.connector.memsql.runtime.test;


import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.avikcloud.connectorsdk.common.exceptions.ConnectorException;
import com.unicosolution.connector.memsql.constants.MemSqlConnectionConstants;
import com.unicosolution.connector.memsql.runtime.MemSqlReader;

public class TestMemSqlReader {
	private static SparkSession sparkSession;
	private static Map<String, Object> sourceDetails=new HashMap<>();
	

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.setProperty("hadoop.home.dir", "C:\\winutils");
		SparkConf cfg=new SparkConf();
		cfg.setAppName("Elastic ETL");
		cfg.setMaster("local[*]");
		sparkSession = SparkSession.builder().config(cfg).getOrCreate();
		sourceDetails.put(MemSqlConnectionConstants.USER_NAME, "test");
		sourceDetails.put(MemSqlConnectionConstants.PASSWORD, "test");
		sourceDetails.put(MemSqlConnectionConstants.TABLE_NAME, "sample");
		sourceDetails.put(MemSqlConnectionConstants.HOST,"192.168.10.1");
		sourceDetails.put(MemSqlConnectionConstants.PORT,"3306");
		sourceDetails.put(MemSqlConnectionConstants.DATABASE_NAME,"sam");
		sourceDetails.put(MemSqlConnectionConstants.FIELDS, "id,email");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		sparkSession.close();
	}
	@Ignore
	@Test
	public void testReadSuccess() {
		MemSqlReader reader=new MemSqlReader();
		
		try {
			Dataset<Row> sourceData=reader.read(sparkSession, sourceDetails);
			sourceData.show();
		} catch (ConnectorException e) {
			e.printStackTrace();
		}
	}
}
