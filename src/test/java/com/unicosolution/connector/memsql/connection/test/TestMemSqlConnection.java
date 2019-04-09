package com.unicosolution.connector.memsql.connection.test;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.avikcloud.connectorsdk.common.ConnectionProperties;
import com.avikcloud.connectorsdk.common.objects.ConnectionStatus;
import com.avikcloud.connectorsdk.common.objects.ConnectionStatusEnum;
import com.unicosolution.connector.memsql.connection.MemSqlConnection;
import com.unicosolution.connector.memsql.constants.MemSqlConnectionConstants;
import com.unicosolution.connector.memsql.test.util.ConnectionPropertiesImpl;

public class TestMemSqlConnection {
	private static MemSqlConnection memSqlCon = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		memSqlCon = new MemSqlConnection();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		memSqlCon = null;
	}
	@Ignore
	@Test
	public void testOpenSuccess() {
		Map<String, Object> map = new HashMap<>();

		map.put(MemSqlConnectionConstants.HOST, "192.168.1.10");
		map.put(MemSqlConnectionConstants.PORT, "3306");
		map.put(MemSqlConnectionConstants.USER_NAME, "test");
		map.put(MemSqlConnectionConstants.PASSWORD, "test");
		map.put(MemSqlConnectionConstants.DATABASE_NAME, "sam");
		map.put(MemSqlConnectionConstants.USE_SSL, MemSqlConnectionConstants.TRUE);

		ConnectionProperties props = new ConnectionPropertiesImpl(map);
		ConnectionStatus status = memSqlCon.open(props);
		System.out.println("old version " +status.getMessage());
		ConnectionStatusEnum expectedResust = ConnectionStatusEnum.SUCCESS;
		ConnectionStatusEnum actualResult = status.getStatus();
		assertEquals(expectedResust, actualResult);
	}
	//@Ignore
	@Test
	public void testOpenForMySqlNewVersionSuccess() {
		
		Map<String, Object> map = new HashMap<>();
		map.put(MemSqlConnectionConstants.HOST, "192.168.10.1");
		map.put(MemSqlConnectionConstants.PORT, "3306");
		map.put(MemSqlConnectionConstants.USER_NAME, "test");
		map.put(MemSqlConnectionConstants.PASSWORD, "test");
		map.put(MemSqlConnectionConstants.DATABASE_NAME, "sam");
		map.put(MemSqlConnectionConstants.USE_SSL, MemSqlConnectionConstants.FALSE);
		
		ConnectionProperties props = new ConnectionPropertiesImpl(map);
		ConnectionStatus status = memSqlCon.open(props);

		System.out.println("new version "+status.getMessage());
		System.out.println(status.getDescription());
		ConnectionStatusEnum expectedResust = ConnectionStatusEnum.FAILURE;
		ConnectionStatusEnum actualResult = status.getStatus();
		assertEquals(expectedResust, actualResult);
	}
	@Ignore
	@Test
	public void testOpenIllegalArgumentException() {
		
		Map<String, Object> map = new HashMap<>();
		map.put(MemSqlConnectionConstants.HOST, "192.168.10.1");
		map.put(MemSqlConnectionConstants.PORT, "3306");
		map.put(MemSqlConnectionConstants.USER_NAME, null);
		map.put(MemSqlConnectionConstants.PASSWORD, "test");
		map.put(MemSqlConnectionConstants.DATABASE_NAME, null);
		
		ConnectionProperties props = new ConnectionPropertiesImpl(map);
		ConnectionStatus status = memSqlCon.open(props);

		System.out.println("Illigal arg excp test "+status.getMessage());
		System.out.println(status.getDescription());
		ConnectionStatusEnum expectedResust = ConnectionStatusEnum.FAILURE;
		ConnectionStatusEnum actualResult = status.getStatus();
		Connection con=memSqlCon.getConnection();
		System.out.println(con);
		assertEquals(expectedResust, actualResult);
	}
	@Ignore
	@Test
	public void testOpenWrongCredentials() {
		
		Map<String, Object> map = new HashMap<>();
		map.put(MemSqlConnectionConstants.HOST, "192.168.1.10");
		map.put(MemSqlConnectionConstants.PORT, "3306");
		map.put(MemSqlConnectionConstants.USER_NAME, "root");
		map.put(MemSqlConnectionConstants.PASSWORD, "admin");
		map.put(MemSqlConnectionConstants.DATABASE_NAME, "sam");
		
		ConnectionProperties props = new ConnectionPropertiesImpl(map);
		ConnectionStatus status = memSqlCon.open(props);

		System.out.println("Credentials test :: "+status.getMessage());
		System.out.println(status.getDescription());
		
		ConnectionStatusEnum expectedResust = ConnectionStatusEnum.FAILURE;
		ConnectionStatusEnum actualResult = status.getStatus();
		Connection con=memSqlCon.getConnection();
		System.out.println(con);
		assertEquals(expectedResust, actualResult);
	}
	@Ignore
	@Test
	public void testOpenIllegalArgumentException2() {
		
		Map<String, Object> map = new HashMap<>();
		map.put(MemSqlConnectionConstants.HOST, "192.168.1.10");
		map.put(MemSqlConnectionConstants.PORT, "3306");
		map.put(MemSqlConnectionConstants.USER_NAME, "test");
		map.put(MemSqlConnectionConstants.PASSWORD, "test");
		map.put(MemSqlConnectionConstants.DATABASE_NAME, "sam");
		map.put(MemSqlConnectionConstants.USE_SSL, "TEST");
		
		ConnectionProperties props = new ConnectionPropertiesImpl(map);
		ConnectionStatus status = memSqlCon.open(props);

		System.out.println("Illigal arg excp test "+status.getMessage());
		System.out.println(status.getDescription());
		ConnectionStatusEnum expectedResust = ConnectionStatusEnum.FAILURE;
		ConnectionStatusEnum actualResult = status.getStatus();
		Connection con=memSqlCon.getConnection();
		System.out.println(con);
		assertEquals(expectedResust, actualResult);
	}
	@Ignore
	@Test
	public void testCloseSuccess() {
		Map<String, Object> map = new HashMap<>();
		map.put(MemSqlConnectionConstants.HOST, "192.168.1.10");
		map.put(MemSqlConnectionConstants.PORT, "3306");
		map.put(MemSqlConnectionConstants.USER_NAME, "test");
		map.put(MemSqlConnectionConstants.PASSWORD, "test");
		map.put(MemSqlConnectionConstants.DATABASE_NAME, "sam");
		map.put(MemSqlConnectionConstants.USE_SSL, MemSqlConnectionConstants.FALSE);
		ConnectionProperties props = new ConnectionPropertiesImpl(map);
		MemSqlConnection myCon = new MemSqlConnection();
		myCon.open(props);
		ConnectionStatus closeStatus=myCon.close();
		System.out.println(closeStatus.getMessage());
		ConnectionStatusEnum expectedResust = ConnectionStatusEnum.SUCCESS;
		ConnectionStatusEnum actualResult = closeStatus.getStatus();
		assertEquals(expectedResust, actualResult);
	}
	//@Ignore
	@Test
	public void testCloseFailure() {
		MemSqlConnection myCon = new MemSqlConnection();
		ConnectionStatus closeStatus=myCon.close();
		System.out.println(closeStatus.getMessage());
		ConnectionStatusEnum expectedResust = ConnectionStatusEnum.FAILURE;
		ConnectionStatusEnum actualResult = closeStatus.getStatus();
		assertEquals(expectedResust, actualResult);
	}
	@Ignore
	@Test
	public void testIsAliveTrue() {
		Map<String, Object> map = new HashMap<>();
		map.put(MemSqlConnectionConstants.HOST, "192.168.1.10");
		map.put(MemSqlConnectionConstants.PORT, "3306");
		map.put(MemSqlConnectionConstants.USER_NAME, "test");
		map.put(MemSqlConnectionConstants.PASSWORD, "test");
		map.put(MemSqlConnectionConstants.DATABASE_NAME, "sam");
		map.put(MemSqlConnectionConstants.USE_SSL, MemSqlConnectionConstants.FALSE);
		ConnectionProperties props = new ConnectionPropertiesImpl(map);
		MemSqlConnection myCon = new MemSqlConnection();
		myCon.open(props);
		boolean actualResult=myCon.isAlive();
		boolean expectedResult=true;
		assertEquals(expectedResult, actualResult);
	}
	//@Ignore
	@Test
	public void testIsAliveFalse() {
		MemSqlConnection myCon = new MemSqlConnection();
		boolean actualResult=myCon.isAlive();
		boolean expectedResult=false;
		assertEquals(expectedResult, actualResult);
	}
	@Test
	//@Ignore
	public void testOpenMemSqlSuccess() {
		Map<String, Object> map = new HashMap<>();

		map.put(MemSqlConnectionConstants.HOST, "192.168.1.10");
		map.put(MemSqlConnectionConstants.PORT, "3306");
		map.put(MemSqlConnectionConstants.USER_NAME, "test");
		map.put(MemSqlConnectionConstants.PASSWORD,"test");
		map.put(MemSqlConnectionConstants.DATABASE_NAME, "sam");
		map.put(MemSqlConnectionConstants.USE_SSL, MemSqlConnectionConstants.FALSE);

		ConnectionProperties props = new ConnectionPropertiesImpl(map);
		ConnectionStatus status = memSqlCon.open(props);
		System.out.println("MEM SQL " +status.getMessage());
		ConnectionStatusEnum expectedResust = ConnectionStatusEnum.SUCCESS;
		ConnectionStatusEnum actualResult = status.getStatus();
		assertEquals(expectedResust, actualResult);
	}
}
