package com.tenddata.hive.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.log4j.Logger;

public class MySQLTable {
	Log log = org.apache.commons.logging.LogFactory.getLog(MySQLTable.class);
	private String dbUrl;
	private String dbUserName;
	private String dbPassword;
	private String tblName;

	private Connection conn;
	private PreparedStatement stat;
	private ResultSet rs;

	private String ingenoredColumn;
	private String sumColumn;

	public MySQLTable() {

	}

	public MySQLTable(String dbUrl, String dbUserName, String dbPassword,
			String tblName) {
		this.dbUrl = dbUrl;
		this.dbUserName = dbUserName;
		this.dbPassword = dbPassword;
		this.tblName = tblName;
	}

	public void setIngenoredColumn(String ingenoredColumn) {
		this.ingenoredColumn = ingenoredColumn;
	}

	public void setSumColumn(String sumColumn) {
		this.sumColumn = sumColumn;
	}

	public Connection getConn() {
		try {
			if (conn != null && !conn.isClosed()) {
				return conn;
			}
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(dbUrl, dbUserName, dbPassword);
			return conn;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void close(Connection conn, Statement stat, ResultSet rs) {
		try {
			if (rs != null && !rs.isClosed()) {
				rs.close();
				rs = null;
			}
			if (stat != null && !stat.isClosed()) {
				stat.close();
				stat = null;
			}
			if (conn != null && !conn.isClosed()) {
				conn.close();
				conn = null;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public ResultSet findAll(String[] readColumns) {
		StringBuffer columns = new StringBuffer(readColumns[0]);
		if (readColumns.length > 1) {
			for (int i = 1; i < readColumns.length; i++) {
				columns.append(",").append(readColumns[i]);
			}
		}
		StringBuffer sql = new StringBuffer();
		sql.append("select ").append(columns).append(" from ").append(tblName);

		conn = getConn();
		try {
			stat = conn.prepareStatement(sql.toString());
			rs = stat.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}

	public int batchSave(List<Map<String, Object>> bufferList) {
		if (bufferList == null || bufferList.isEmpty()) {
			return 0;
		}
		StringBuffer cols = new StringBuffer();
		StringBuffer vals = new StringBuffer();
		Iterator<String> it = bufferList.get(0).keySet().iterator();
		
		List<String> ingenoredCols = ConfigurationUtil  //更新时忽略的列
				.getIngoredColumns(ingenoredColumn);
		List<String> sumCols = ConfigurationUtil  //更新时要求和的列
				.getSumColumns(sumColumn);
		
		List<String> keys = new ArrayList<String>();
		List<String> updateKeys = new ArrayList<String>(); //记录更新时存给定值的列
		List<String> sumKeys = new ArrayList<String>(); //记录更新时求和的列
		
		String key;
		while (it.hasNext()) {
			key = it.next();
			if (!ingenoredCols.contains(key)) {
				if(sumCols.contains(key)){
					sumKeys.add(key);
				}else{
					updateKeys.add(key);
				}
			}
			cols.append(",").append(key);
			vals.append(",").append("?");
			keys.add(key);
		}
		String columns = cols.substring(1);
		String values = vals.substring(1);
		StringBuffer sql = new StringBuffer();
		// sql.append("replace ").append(tblName).append(" (").append(columns)
		// .append(") values (").append(values).append(")");

		sql.append("insert into ").append(tblName).append(" (").append(columns)
				.append(") values (").append(values).append(") ");
		
		if(!updateKeys.isEmpty() || !sumKeys.isEmpty()) {
			sql.append("ON DUPLICATE KEY UPDATE ");
			for(String updateKey : updateKeys) {
				sql.append(updateKey).append("=").append("?,");
			}
			for(String sumKey : sumKeys) {
				sql.append(sumKey).append("=").append(sumKey).append("+?,");
			}
		}

		conn = getConn();
		int count = 0;
		try {
			stat = conn.prepareStatement(sql.toString().substring(0, sql.length()-1));
			for (int k = 0; k < bufferList.size(); k++) {
				for (int i = 0; i < keys.size(); i++) {
					if (keys.get(i).equals("id")
							&& (bufferList.get(k).get(keys.get(i)) == null || bufferList
									.get(k).get(keys.get(i)).toString()
									.equals("0"))) {
						stat.setObject(i + 1, null);
					} else {
						stat.setObject(i + 1, bufferList.get(k)
								.get(keys.get(i)));
					}
				}
				int size = keys.size();
				for(String updateKey : updateKeys) {
					stat.setObject(++size, bufferList.get(k).get(updateKey));
				}
				for(String sumKey : sumKeys) {
					stat.setObject(++size, bufferList.get(k).get(sumKey));
				}
				
				stat.addBatch();
			}
			int[] rets = stat.executeBatch();
			for (int ret : rets) {
				count += ret;
			}
		} catch (SQLException e) {
			log.error(e);
			e.printStackTrace();
		} finally {
			close(null, stat, rs);
		}
		return count;
	}

	/**
	 * @param dbo
	 */
	public void save(Map<String, Object> dbo) {
		StringBuffer cols = new StringBuffer();
		Iterator<String> it = dbo.keySet().iterator();
		List<Object> vals = new ArrayList<Object>();
		String key;
		while (it.hasNext()) {
			key = it.next();
			cols.append(",").append(key);
			vals.add(dbo.get(key));
		}
		String columns = cols.substring(1);
		StringBuffer sql = new StringBuffer();
		sql.append("replace ").append(tblName).append(" (").append(columns)
				.append(") values (");
		for (int i = 0; i < vals.size(); i++) {
			if (i == vals.size() - 1) {
				sql.append("?");
			} else {
				sql.append("?,");
			}
		}
		sql.append(")");

		conn = getConn();
		try {
			stat = conn.prepareStatement(sql.toString());
			for (int i = 0; i < vals.size(); i++) {
				stat.setObject(i + 1, vals.get(i));
			}
			stat.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(null, stat, rs);
		}
	}

	public void drop() {
		String sql = "drop table " + tblName;
		conn = getConn();
		try {
			stat = conn.prepareStatement(sql);
			stat.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(conn, stat, null);
		}
	}

	public long count() {
		try {
			if (this.rs == null) {
				this.findAll(new String[] { "id" });
			}
			int rowCount;
			int currentRow = rs.getRow();
			rowCount = rs.last() ? rs.getRow() : 0;
			if (currentRow == 0)
				rs.beforeFirst();
			else
				rs.absolute(currentRow);
			return rowCount;
		} catch (SQLException e) {
			e.printStackTrace();
			return 0;
		}
	}

	public static void main(String[] args) {
		MySQLTable table = new MySQLTable("jdbc:mysql://localhost/db_bitmap",
				"hive", "hive", "hourdata");
		table.drop();
	}

}
