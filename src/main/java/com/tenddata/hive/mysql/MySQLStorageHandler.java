package com.tenddata.hive.mysql;

import static com.tenddata.hive.mysql.ConfigurationUtil.DB_URL;
import static com.tenddata.hive.mysql.ConfigurationUtil.DB_USERNAME;
import static com.tenddata.hive.mysql.ConfigurationUtil.DB_PASSWORD;
import static com.tenddata.hive.mysql.ConfigurationUtil.TABLE_NAME;
import static com.tenddata.hive.mysql.ConfigurationUtil.copyMySQLProperties;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

@SuppressWarnings("unchecked")
public class MySQLStorageHandler implements HiveStorageHandler {
	private Configuration mConf = null;

	public MySQLStorageHandler() {
	}

	@Override
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		Properties properties = tableDesc.getProperties();
		copyMySQLProperties(properties, jobProperties);
	}

	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return MySQLInputFormat.class;
	}

	@Override
	public HiveMetaHook getMetaHook() {
		return new DummyMetaHook();
	}

	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return MySQLOutputFormat.class;
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return MySQLSerDe.class;
	}

	@Override
	public Configuration getConf() {
		return this.mConf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.mConf = conf;
	}

	private class DummyMetaHook implements HiveMetaHook {

		@Override
		public void commitCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void commitDropTable(Table tbl, boolean deleteData)
				throws MetaException {
//			boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
//			if (deleteData && isExternal) {
//				// nothing to do...
//			} else if (deleteData && !isExternal) {
//				String dbUrl = tbl.getParameters().get(DB_URL);
//				String dbUserName = tbl.getParameters().get(DB_USERNAME);
//				String dbPassword = tbl.getParameters().get(DB_PASSWORD);
//				String dbTableName = tbl.getParameters().get(TABLE_NAME);
//				MySQLTable table = new MySQLTable(dbUrl, dbUserName,
//						dbPassword, dbTableName);
//				table.drop();
//			}
		}

		@Override
		public void preCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void preDropTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void rollbackCreateTable(Table tbl) throws MetaException {
			// nothing to do...
		}

		@Override
		public void rollbackDropTable(Table tbl) throws MetaException {
			// nothing to do...
		}

	}
}
