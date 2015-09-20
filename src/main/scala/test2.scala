import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HTable, HBaseAdmin}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}


object test2 {
  def main(args: Array[String]) {
    //Initiate spark context with spark master URL. You can modify the URL per your environment. 
    val conf1= new SparkConf().setMaster("local[2]").setAppName("CountingSheep")
    val sc = new SparkContext(conf1)

    val tableName = "test"

    val conf = HBaseConfiguration.create()

    //basically connecting to hbase server from local cloudera vm for test
    conf.addResource("hbase-site.xml")
    conf.set("hbase.zookeeper.quorum","quickstart.cloudera")
    conf.set("hbase.zookeeper.property.clientPort","2181")
    conf.set("hbase.master", "quickstart.cloudera:60010")
    HBaseAdmin.checkHBaseAvailable(conf)


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
