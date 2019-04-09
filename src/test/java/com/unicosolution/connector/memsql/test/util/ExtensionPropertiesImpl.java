package com.unicosolution.connector.memsql.test.util;

import java.util.HashMap;
import java.util.Map;

import com.avikcloud.connectorsdk.common.ExtensionProperties;

public class ExtensionPropertiesImpl implements ExtensionProperties {

	private Map<String, Object> extensionProperties;

	public ExtensionPropertiesImpl() {
		this.extensionProperties = new HashMap<>();
	}

	public ExtensionPropertiesImpl(Map<String, Object> extensionProperties) {
		this.extensionProperties = extensionProperties;
	}

	@Override
	public Object getProperty(String key) {
		return this.extensionProperties.get(key);
	}

	@Override
	public boolean setProperty(String key, Object value) {
		try {
			this.extensionProperties.put(key, value);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

}