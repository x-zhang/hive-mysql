package com.tenddata.hive.mysql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

public class MySQLSplit extends FileSplit implements InputSplit {
	private static final String[] EMPTY_ARRAY = new String[] {};
	private long start, end;
	private boolean isLastSplit = false;
	
	public MySQLSplit() {
	    super((Path) null, 0, 0, EMPTY_ARRAY);
	  }
	public MySQLSplit(long start, long end, Path dummyPath){
		super(dummyPath, 0, 0, EMPTY_ARRAY);
		this.start = start;
		this.end = end;
	}
	
	@Override
	  public void readFields(final DataInput input) throws IOException {
	    super.readFields(input);
	    start = input.readLong();
	    end = input.readLong();
	  }

	  @Override
	  public void write(final DataOutput output) throws IOException {
	    super.write(output);
	    output.writeLong(start);
	    output.writeLong(end);
	  }

	  public long getStart() {
	    return start;
	  }

	  public long getEnd() {
	    return end;
	  }
	  
	  public boolean isLastSplit(){
		  return this.isLastSplit;
	  }

	  public void setStart(long start) {
	    this.start = start;
	  }

	  public void setEnd(long end) {
	    this.end = end;
	  }
	  
	  public void setLastSplit(){
		  this.isLastSplit = true;
	  }

	  @Override
	  public long getLength() {
	    return end - start;
	  }

	  /* Data is remote for all nodes. */
	  @Override
	  public String[] getLocations() throws IOException {
	    return EMPTY_ARRAY;
	  }

	  @Override
	  public String toString() {
	    return String.format("MySQLSplit(start=%s,end=%s)", start, end);
	  }

	public static MySQLSplit[] getSplits(JobConf conf,
			String dbUrl, String dbUserName, String dbPassword, String tblName, int numSplits) {
		
//		MySQLTable table = new MySQLTable(dbUrl, dbUserName, dbPassword, tblName);
//		long total = table.count();
//		final long splitSize = total / numSplits;
//		MySQLSplit[] splits = new MySQLSplit[numSplits];
		final Path[] tablePaths = FileInputFormat.getInputPaths(conf);
//		for (int i = 0; i < numSplits; i++) {
//			if ((i + 1) == numSplits) {
//		        splits[i] = new MySQLSplit(i * splitSize, total, tablePaths[0]);
//		        splits[i].setLastSplit();
//		      } else {
//		        splits[i] = new MySQLSplit(i * splitSize, (i + 1) * splitSize, tablePaths[0]);
//		      }
//		}
		MySQLSplit[] splits = new MySQLSplit[1];
		splits[0] = new MySQLSplit(0, 0, tablePaths[0]);
		return splits;
	}

}
