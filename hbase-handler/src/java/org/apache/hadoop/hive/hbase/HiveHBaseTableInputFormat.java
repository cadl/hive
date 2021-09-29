package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

public class HiveHBaseTableInputFormat extends InputFormat<ImmutableBytesWritable, ResultWritable>
    implements org.apache.hadoop.mapred.InputFormat<ImmutableBytesWritable, ResultWritable> {

  private HiveHBaseTableMapredInputFormat tableInputFormat = new HiveHBaseTableMapredInputFormat();

  @Override public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf jobConf, int i)
      throws IOException {
    return tableInputFormat.getSplits(jobConf, i);
  }

  @Override public org.apache.hadoop.mapred.RecordReader<ImmutableBytesWritable, ResultWritable> getRecordReader(
      org.apache.hadoop.mapred.InputSplit inputSplit, JobConf jobConf, Reporter reporter)
      throws IOException {
    return tableInputFormat.getRecordReader(inputSplit, jobConf, reporter);
  }

  @Override public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    return tableInputFormat.getSplits(jobContext);
  }

  @Override public RecordReader<ImmutableBytesWritable, ResultWritable> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException {
    final RecordReader<ImmutableBytesWritable, Result> recordReader =
        tableInputFormat.createRecordReader(inputSplit, taskAttemptContext);

    return new RecordReader<ImmutableBytesWritable, ResultWritable>() {
      @Override public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
        recordReader.initialize(inputSplit, taskAttemptContext);
      }

      @Override public boolean nextKeyValue() throws IOException, InterruptedException {
        return recordReader.nextKeyValue();
      }

      @Override public ImmutableBytesWritable getCurrentKey()
          throws IOException, InterruptedException {
        return recordReader.getCurrentKey();
      }

      @Override public ResultWritable getCurrentValue() throws IOException, InterruptedException {
        return new ResultWritable(recordReader.getCurrentValue());
      }

      @Override public float getProgress() throws IOException, InterruptedException {
        return recordReader.getProgress();
      }

      @Override public void close() throws IOException {
        recordReader.close();
      }
    };
  }
}
