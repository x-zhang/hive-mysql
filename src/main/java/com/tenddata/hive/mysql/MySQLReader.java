package com.tenddata.hive.mysql;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

public class MySQLReader implements RecordReader<LongWritable, MapWritable> {
	final static int FETCH_SIZE = 8192;
	LongWritable key = new LongWritable();
	MapWritable val = new MapWritable();
	MySQLTable table;
	MySQLSplit split;
	ResultSet rs;
	long pos;
	String[] readColumns;

	public MySQLReader(String dbUrl, String dbUserName, String dbPassword,
			String tblName, MySQLSplit split, String[] readColumns) {
		this.table = new MySQLTable(dbUrl, dbUserName, dbPassword, tblName);
		this.split = split;
		this.readColumns = readColumns;
		
		this.rs = table.findAll(readColumns);
		try {
			this.rs.setFetchSize(FETCH_SIZE);
			this.rs.absolute((int)split.getStart());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
//		if (!split.isLastSplit())
//			this.cursor.limit((int) split.getLength());// if it's the last
		// split,it will read
		// all records since
		// $start

	}

	@Override
	public void close() {
		if (table != null)
			table.close(null,null,rs);
	}

	@Override
	public LongWritable createKey() {
		return key;
	}

	@Override
	public MapWritable createValue() {
		val.clear();
		return val;
	}

	@Override
	public long getPos() throws IOException {
		return this.pos;
	}

	@Override
	public float getProgress() throws IOException {
		return split.getLength() > 0 ? pos / (float) split.getLength() : 1.0f;
	}

	@Override
	public boolean next(LongWritable keyHolder, MapWritable valueHolder)
			throws IOException {
		try {
			if (!rs.next()) {
				return false;
			}
			
//			DBObject record = cursor.next();
			keyHolder.set(pos);
			for (int i = 0; i < this.readColumns.length; i++) {
				String key = readColumns[i];
				Object vObj = rs.getObject(key);
//				Object vObj = ("id".equals(key)) ? record.get("_id") : record
//						.get(key);
				Writable value = (vObj == null) ? NullWritable.get() : new Text(
						vObj.toString());
				valueHolder.put(new Text(key), value);
			}
			pos++;
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return true;
	}

}
