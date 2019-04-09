package com.unicosolution.connector.memsql.metadata.test;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.avikcloud.connectorsdk.common.Browseable;
import com.avikcloud.connectorsdk.common.ExtensionProperties;
import com.unicosolution.connector.memsql.connection.MemSqlConnection;
import com.unicosolution.connector.memsql.constants.MemSqlConnectionConstants;
import com.unicosolution.connector.memsql.metadata.MemSqlMetadataBrowser;
import com.unicosolution.connector.memsql.test.util.BrowseableImpl;
import com.unicosolution.connector.memsql.test.util.ConnectionPropertiesImpl;

public class TestMemSqlMetadataBrowser {
	private static MemSqlConnection memSqlCon = null;
	private static Map<String, Object> map = new HashMap<>();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		memSqlCon = new MemSqlConnection();
		
		map.put(MemSqlConnectionConstants.HOST, "192.168.1.10");
		map.put(MemSqlConnectionConstants.PORT, "3306");
		map.put(MemSqlConnectionConstants.USER_NAME, "test");
		map.put(MemSqlConnectionConstants.PASSWORD, "test");
		map.put(MemSqlConnectionConstants.DATABASE_NAME, "sam");
		map.put(MemSqlConnectionConstants.USE_SSL, MemSqlConnectionConstants.FALSE);

		
		memSqlCon.open(new ConnectionPropertiesImpl(map));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		memSqlCon=null;
	}
	@Ignore
	@Test
	public void testBrowseObjectsForTablesSuccess() {
		MemSqlMetadataBrowser memSqlBrowser=new MemSqlMetadataBrowser();
		System.out.println("Database name :: " +memSqlCon.getDatabaseName());
		BrowseableImpl browseable=new BrowseableImpl();
		browseable.setIsRoot(true);
		boolean flag=memSqlBrowser.browseObjects(memSqlCon, browseable, null);
		if(flag) {
			for(Browseable b:browseable.getChildren()) {
				System.out.println(b.getName());
			}
		}
		System.out.println(flag);
		assertEquals(true, flag);
	}
	@Ignore
	@Test
	public void testBrowseObjectsForColumnsSuccess() {
		MemSqlMetadataBrowser memSqlBrowser=new MemSqlMetadataBrowser();
		BrowseableImpl browseable=new BrowseableImpl();
		browseable.setName("sample");
		browseable.setIsRoot(false);
		boolean flag=memSqlBrowser.browseObjects(memSqlCon, browseable, null);
		System.out.println(flag);
		assertEquals(true, flag);
	}
	@Ignore
	@Test
	public void testBrowseObjectsForTablesAndColumnsSuccess() {
		MemSqlMetadataBrowser memSqlBrowser=new MemSqlMetadataBrowser();
		BrowseableImpl browseable=new BrowseableImpl();
		browseable.setName(map.get(MemSqlConnectionConstants.DATABASE_NAME).toString());
		browseable.setIsRoot(true);
		boolean actual=false;
		boolean flag=memSqlBrowser.browseObjects(memSqlCon, browseable, null);
		List<Browseable> tablesList=browseable.getChildren();
		for(Browseable table: tablesList) {
			System.out.println("==============================");
			System.out.println("Table name :: "+table.getName());
			System.out.println("==============================");
			BrowseableImpl child=(BrowseableImpl)table;
			child.setIsRoot(false);
			boolean flag2=memSqlBrowser.browseObjects(memSqlCon, child, null);
			System.out.println("Cloumns List ");
			for(Browseable column:table.getChildren()) {
				System.out.println(column.getName());
			}
			if(flag && flag2) {
				actual=true;
			}
		}
		assertEquals(true, actual);
	}

	@Ignore
	@Test
	public void testBrowseObjectsForWorngTableFail() {
		MemSqlMetadataBrowser memSqlBrowser=new MemSqlMetadataBrowser();
		BrowseableImpl browseable=new BrowseableImpl();
		browseable.setName("some");
		browseable.setIsRoot(false);
		boolean flag=memSqlBrowser.browseObjects(memSqlCon, browseable, null);
		System.out.println(flag);
		assertEquals(false, flag);
	}
	@Ignore
	@Test
	public void testBrowseObjectsForEmptyBrowsableifIsRootFalseFail() {
		MemSqlMetadataBrowser memSqlBrowser=new MemSqlMetadataBrowser();
		BrowseableImpl browseable=new BrowseableImpl();
		System.out.println(browseable.getName());
		boolean flag=memSqlBrowser.browseObjects(memSqlCon, browseable, null);
		System.out.println(flag);
		assertEquals(false, flag);
	}
	@Ignore
	@Test
	public void testBrowseObjectsForEmptyBrowsableifIsRootTrueeSuccess() {
		MemSqlMetadataBrowser memSqlBrowser=new MemSqlMetadataBrowser();
		BrowseableImpl browseable=new BrowseableImpl();
		browseable.setIsRoot(true);
		boolean flag=memSqlBrowser.browseObjects(memSqlCon, browseable, null);
		if(flag) {
			for(Browseable b:browseable.getChildren()) {
				System.out.println(b.getName());
			}
		}
		System.out.println(flag);
		assertEquals(true, flag);
	}
	@Ignore
	@Test
	public void testBrowseObjectsForPerfomenceTestForTables() {
		MemSqlMetadataBrowser memSqlBrowser=new MemSqlMetadataBrowser();
		BrowseableImpl browseable=new BrowseableImpl();
		browseable.setName(map.get(MemSqlConnectionConstants.DATABASE_NAME).toString());
		browseable.setIsRoot(true);
		Long startTime=System.currentTimeMillis();
		boolean flag=memSqlBrowser.browseObjects(memSqlCon, browseable, null);
		Long endTime=System.currentTimeMillis();
		System.out.println("Time taken for browseTables :: "+(endTime-startTime));
		assertEquals(true, flag);
	}
	@Ignore
	@Test
	public void testBrowseObjectsForColumnsMetaData() {
		MemSqlMetadataBrowser memSqlBrowser=new MemSqlMetadataBrowser();
		BrowseableImpl browseable=new BrowseableImpl();
		browseable.setIsRoot(false);
		browseable.setName("sample");
		boolean flag=memSqlBrowser.browseObjects(memSqlCon, browseable, null);
		System.out.println(flag);
		List<Browseable> colMetadata=browseable.getChildren();
		for(Browseable meta:colMetadata) {
			System.out.println("Column Name :: "+meta.getName());
			ExtensionProperties metaProps=meta.getProperties();
			System.out.println("DATA_TYPE :: "+metaProps.getProperty("DATA_TYPE"));
			System.out.println("COLUMN_SIZE :: "+metaProps.getProperty("COLUMN_SIZE"));
			System.out.println("ORDINAL_POSITION :: "+metaProps.getProperty("ORDINAL_POSITION"));
			System.out.println("IS_PRIMARY_KEY :: "+metaProps.getProperty("IS_PRIMARY_KEY"));
	
			
	
		}
		assertEquals(true, flag);
	}
}
