package com.tenddata.hive.mysql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

public class MySQLWriter implements RecordWriter {
	static final int BATCH_SIZE = 1000;
	List<Map<String, Object>> bufferList = new ArrayList<Map<String, Object>>();
	MySQLTable table;
	long base;
	int inputCount = 0;
	int outCount = 0;

	public MySQLWriter(String dbUrl, String dbUserName, String dbPassword,
			String tblName) {
		this.table = new MySQLTable(dbUrl, dbUserName, dbPassword, tblName);
	}
	
	public void setIngenoredColumn(String ingenoredColumn){
		this.table.setIngenoredColumn(ingenoredColumn);
	}
	
	public void setSumColumn(String sumColumn){
		this.table.setSumColumn(sumColumn);
	}
	
	@Override
	public void close(boolean abort) throws IOException {
		if (bufferList != null && !bufferList.isEmpty()) {
			outCount += table.batchSave(bufferList);
			bufferList.clear();
		}
	}

	@Override
	public void write(Writable w) throws IOException {
		inputCount++;
		MapWritable map = (MapWritable) w;
		Map<String, Object> dbo = new HashMap<String, Object>();
		for (final Map.Entry<Writable, Writable> entry : map.entrySet()) {
//			 System.err.println("Write: key=" + entry.getKey().toString()
//			 + ", val=" + entry.getValue().toString());
			String key = entry.getKey().toString();
			dbo.put(key, getObjectFromWritable(entry.getValue()));
		}
//		table.save(dbo);
		
		bufferList.add(dbo);
		if (bufferList.size() >= BATCH_SIZE) {
			outCount += table.batchSave(bufferList);
			bufferList.clear();
		}
		
	}

	private Object getObjectFromWritable(Writable w) {
		if (w instanceof IntWritable) {
			// int
			return ((IntWritable) w).get();
		} else if (w instanceof ShortWritable) {
			// short
			return ((ShortWritable) w).get();
		} else if (w instanceof ByteWritable) {
			// byte
			return ((ByteWritable) w).get();
		} else if (w instanceof BooleanWritable) {
			// boolean
			return ((BooleanWritable) w).get();
		} else if (w instanceof LongWritable) {
			// long
			return ((LongWritable) w).get();
		} else if (w instanceof FloatWritable) {
			// float
			return ((FloatWritable) w).get();
		} else if (w instanceof DoubleWritable) {
			// double
			return ((DoubleWritable) w).get();
		} else if (w instanceof NullWritable) {
			// null
			return null;
		} else {
			// treat as string
			return w.toString();
		}

	}

	@Override
	protected void finalize() throws Throwable {
		// TODO Auto-generated method stub
		super.finalize();
	}

}
