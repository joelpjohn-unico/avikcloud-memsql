package com.unicosolution.connector.memsql.test.util;

import java.util.ArrayList;
import java.util.List;

import com.avikcloud.connectorsdk.common.Browseable;
import com.avikcloud.connectorsdk.common.ExtensionProperties;
import com.avikcloud.connectorsdk.common.Factory;

public class BrowseableImpl implements Browseable {

	private Factory factory;
	private String name;
	private String nativeName;
	private String description;
	private ExtensionProperties extension;
	private boolean isRoot = false;
	private List<Browseable> children;

	public BrowseableImpl() {
		this.factory = FactoryImpl.getInstance();
		this.children = new ArrayList<>();
		this.extension=new ExtensionPropertiesImpl();
	}
	
	public BrowseableImpl(String name, String nativeName, String description) {
		this.factory = FactoryImpl.getInstance();
		this.children = new ArrayList<>();
		this.name = name;
		this.nativeName = nativeName;
		this.description = description;
		this.extension=new ExtensionPropertiesImpl();
	}

	@Override
	public Factory getFactory() {
		return factory;
	}

	public void setIsRoot(Boolean isRoot) { //For API use
		this.isRoot = isRoot;
	}
	
	@Override
	public Boolean isRoot() {
		return isRoot;
	}

	@Override
	public List<Browseable> getChildren() {
		return this.children;
	}

	@Override
	public Boolean addChild(Browseable child) {
		try {
			this.children.add(child);
		} catch(Exception e) {
			return false;
		}
		return true;
	}

	@Override
	public Boolean addChildren(List<Browseable> children) {
		try {
			this.children.addAll(children);
		} catch(Exception e) {
			return false;
		}
		return true;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String getNativeName() {
		return this.nativeName;
	}

	@Override
	public void setNativeName(String nativeName) {
		this.nativeName = nativeName;
	}

	@Override
	public String getDescription() {
		return this.description;
	}

	@Override
	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public ExtensionProperties getProperties() {
		return extension;
	}

} 