package com.unicosolution.connector.memsql.test.util;

import com.avikcloud.connectorsdk.common.Browseable;
import com.avikcloud.connectorsdk.common.Factory;

public class FactoryImpl implements Factory {

	private static FactoryImpl instance;

	private FactoryImpl() {
	}

	public static FactoryImpl getInstance() {
		if (null == instance) {
			instance = new FactoryImpl();
		}
		return instance;
	}


	@Override
	public Browseable createBrowseable(Browseable root) {
		Browseable b = new BrowseableImpl();
		((BrowseableImpl)b).setIsRoot(true); //TODO: check if this should be false instead
		return b;
	}


} 