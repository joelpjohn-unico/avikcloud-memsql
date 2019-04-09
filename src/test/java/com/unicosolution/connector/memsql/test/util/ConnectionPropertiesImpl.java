package com.unicosolution.connector.memsql.test.util;


import java.util.HashMap;
import java.util.Map;

import com.avikcloud.connectorsdk.common.ConnectionProperties;

public class ConnectionPropertiesImpl implements ConnectionProperties {

	private Map<String, Object> properties;
	
	public ConnectionPropertiesImpl() {
		this.properties = new HashMap<>();
	}

	public ConnectionPropertiesImpl(Map<String, Object> properties) {
		this.properties = properties;
	}

	@Override
	public Object getProperty(String key) {
		return properties.get(key);
	}

} 