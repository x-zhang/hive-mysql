package com.tenddata.hive.mysql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

public class ConfigurationUtil {
	public static final String DB_URL = "mapred.jdbc.url";
	public static final String DB_USERNAME = "mapred.jdbc.username";
	public static final String DB_PASSWORD = "mapred.jdbc.password";
	public static final String TABLE_NAME = "mapred.jdbc.input.table.name";
	public static final String COLUMN_MAPPING = "mapred.jdbc.column.mapping";
	public static final String COLUMN_IGNORED = "mapred.jdbc.onupdate.columns.ignore";
	public static final String COLUMN_SUM = "mapred.jdbc.onupdate.columns.sum";
	
	
	public static final Set<String> ALL_PROPERTIES = ImmutableSet.of(DB_URL,
			DB_USERNAME, DB_PASSWORD, TABLE_NAME, COLUMN_MAPPING, COLUMN_IGNORED, COLUMN_SUM);

	public final static String getDBUrl(Configuration conf){
		return conf.get(DB_URL);
	}
	
	public final static String getDBUserName(Configuration conf) {
		return conf.get(DB_USERNAME);
	}

	public final static String getDBPassword(Configuration conf) {
		return conf.get(DB_PASSWORD);
	}

	public final static String getTableName(Configuration conf) {
		return conf.get(TABLE_NAME);
	}

	public final static String getColumnMapping(Configuration conf) {
		return conf.get(COLUMN_MAPPING);
	}

	public final static String getColumnIgnored(Configuration conf) {
		return conf.get(COLUMN_IGNORED);
	}
	
	public final static String getColumnSum(Configuration conf) {
		return conf.get(COLUMN_SUM);
	}
	
	public static void copyMySQLProperties(Properties from,
			Map<String, String> to) {
		for (String key : ALL_PROPERTIES) {
			String value = from.getProperty(key);
			if (value != null) {
				to.put(key, value);
			}
		}
	}

	public static String[] getAllColumns(String columnMappingString) {
		if(Strings.isNullOrEmpty(columnMappingString)){
			return new String[0];
		}
		return columnMappingString.split(",");
	}
	
	public static List<String> getIngoredColumns(String columnString) {
		if(Strings.isNullOrEmpty(columnString)){
			return new ArrayList<String>();
		}
		return Arrays.asList(columnString.split(","));
	}
	
	public static List<String> getSumColumns(String columnString) {
		if(Strings.isNullOrEmpty(columnString)){
			return new ArrayList<String>();
		}
		return Arrays.asList(columnString.split(","));
	}
}
