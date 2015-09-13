import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HTable, HBaseAdmin}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext


object test2 {
  def main(args: Array[String]) {
    //Initiate spark context with spark master URL. You can modify the URL per your environment. 
    val sc = new SparkContext("my app")

    val tableName = "test"

    val conf = HBaseConfiguration.create()

    conf.set(TableInputFormat.INPUT_TABLE, tableName)


    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable(tableName)) {
      print("Creating test Table")
      val tableDesc =  new HTableDescriptor(TableName.valueOf(tableName) )
      tableDesc.addFamily(new HColumnDescriptor("cf1".getBytes()));
      admin.createTable(tableDesc)
    }else{
      print("Table already exists!!")
      val columnDesc = new HColumnDescriptor("cf1");
      admin.disableTable(Bytes.toBytes(tableName));
      admin.addColumn(tableName, columnDesc);
      admin.enableTable(Bytes.toBytes(tableName));
    }

    //put data into table
    val myTable = new HTable(conf, tableName);
    for (i <- 0 to 5) {
      //var p = new Put();
     var  p = new Put(new String("row" + i).getBytes());
      p.add("cf1".getBytes(), "column-1".getBytes(), new String(
        "value " + i).getBytes());
      myTable.put(p);
    }
    myTable.flushCommits();

    //create rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //get the row count
    val count = hBaseRDD.count()
    print("HBase RDD count:"+count)
    System.exit(0)
  }
}
