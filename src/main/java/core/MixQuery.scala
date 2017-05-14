package core

import java.util

import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by lzz on 17/4/30.
  */
class MixQuery {
  var queryParam = null: QueryParam
  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("clean app")
    .set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def startJob(queryParam: QueryParam): DataFrame ={
    this.queryParam = queryParam
    val sqlList = queryParam.getSqlList
    val size = sqlList.size() - 1
    var rdd:DataFrame = null
    for( i <- 1 to size ){
      var schema = sqlList.get(i).get("schema")
      var table = sqlList.get(i).get("table")
      var join = sqlList.get(i).get("join")
      var sql = sqlList.get(i).get("sql")
      var alias = sqlList.get(i).get("alias")

      rdd = getRdd(sqlList.get(i-1).get("schema"),sqlList.get(i-1).get("table"))
      if( !"".equals(sqlList.get(i-1).get("sql")) ){
        rdd.registerTempTable( sqlList.get(i-1).get("table") )
        rdd = rdd.sqlContext.sql( sqlList.get(i-1).get("sql") )
      }else{
        rdd.registerTempTable( sqlList.get(i-1).get("table") )
      }
      rdd.show()
      rdd = joinRDD(rdd, getRdd(schema, table), sqlList.get(i-1).get("alias"), alias, join)
      rdd.show()
    }
    // 如果只有一行数据那么是但表查询
    if( size ==  0 ){
      rdd=getRdd(sqlList.get(0).get("schema"),sqlList.get(0).get("table"))
    }
    // 最外层的 slect
    rdd.registerTempTable( "all_table" )
    rdd = rdd.sqlContext.sql( queryParam.getParentSql )
    rdd.show()
    // 如果有插入操作
    insertTable(rdd)
    return rdd
  }

  // 插入操作
  def insertTable(rdd: DataFrame): Unit ={
    var insertHm: util.HashMap[String, String] = queryParam.getInsertHm
    if( !insertHm.isEmpty && ""!=insertHm.get("mode") && ""!=insertHm.get("schema") && ""!=insertHm.get("table") ){
      var mode = insertHm.get("mode")
      var schema = insertHm.get("schema")
      var table = insertHm.get("table")
      // 插入操作
      var dfWriter:DataFrameWriter = null
      if( "into".equals( mode ) ){
        dfWriter = rdd.write.mode("append")
      }else {
        dfWriter = rdd.write.mode("overwrite")
      }
      var jdbc = getJdbc( schema )
      val prop = new java.util.Properties
      prop.put("user", jdbc.get("user") )
      prop.put("password", jdbc.get("password"))
      dfWriter.jdbc( jdbc.get("host"), table, prop)
    }
  }

  /**
    * 根据 schema 和 table 获取 rdd
    * @param schema
    * @param table
    * @return
    */
  def getRdd(schema: String, table: String): DataFrame = {
    var jdbcList = queryParam.getJdbcList()
    var jdbc = getJdbc( schema )
    if( jdbc == null ){
      return null
    }

    val prop = new java.util.Properties
    prop.put("user", jdbc.get("user"))
    prop.put("password", jdbc.get("password"))
    var host = jdbc.get("host")
    var rdd = sqlContext.read.jdbc( host, table, prop)
    return rdd
  }

  def getJdbc(schema: String): util.HashMap[String, String] = {
    var jdbcList = queryParam.getJdbcList()
    var size = jdbcList.size() -1
    for( i <- 0 to size ){
      var jdbc = jdbcList.get(i).get( schema )
      if( jdbc != null ){
        return jdbc
      }
    }
    return null
  }

  //关联 rdd
  def joinRDD(rDD: DataFrame, rDD2: DataFrame, leftSchema: String, rightSchema: String, join_type: String): DataFrame = {
    if( rDD.rdd == null || rDD2.rdd == null ){
      return null
    }
    var con:Column = getCondition( rDD, rDD2, leftSchema, rightSchema)
    var temRDD:DataFrame = null
    if( "".equals( join_type ) ){
      temRDD = rDD.join(rDD2, con)
    }else{
      temRDD = rDD.join(rDD2, con, join_type)
    }
    return temRDD
  }

  // 生成关联条件
  def getCondition(rDD: DataFrame, tempRDD: DataFrame, leftSchema: String, rightSchema: String) :Column ={
    var keyList = List(leftSchema + "_" + rightSchema, leftSchema + "_", rightSchema + "_")
    var con: Column = null
    for( item <- keyList ){
      var key = item
      var condition = queryParam.getCondition
      var size = condition.size() - 1
      var left = ""
      var right = ""
      var flag = true
      for( i <- 0 to size if flag){
        var item = condition.get(i);
        var schema :String = item.get("schema").trim
        if( schema.equals( key ) ){
          left = item.get("leftCol").trim
          right = item.get("rightCol").trim
          if( !"".equals(left) && !"".equals(right) ){
            con =  rDD(left) === tempRDD(right)
          }else if ( !"".equals(left) && "".equals(right)){  // 这边有很多逻辑应该抽成一个函数
            if( con != null ){
              var tag = item.get("tag").trim
              var rightVal = item.get("rightVal").trim
              if ( "=".equals(tag) ){
                con = con && rDD(left) == rightVal
              }else if( "<".equals(tag) ){
                con = con && rDD(left) < rightVal
              }else if( ">".equals(tag) ){
                con = con && rDD(left) > rightVal
              }
            }
          }
          flag = false
        }
      }
    }
    return con
  }


  /*
  def getTag(tag:String) : String ={
    var result =
      tag match {
        case "<>"   => "!==";
        case "thisValue"   => tag + " B";
      }
    result
  }
  */

}


//rdd1.join(rdd2, rdd1("id")===rdd2("id") && (rdd1("id") !== 1), "inner").show
//rdd2.distinct().show()