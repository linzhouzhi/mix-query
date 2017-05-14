package core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by lzz on 17/4/30.
 */
public class SqlParser {
    public static String lastSql;  // sql 语句的最后面部分

    /**
     * 获取 join 的 on 条件如 t1 join t2 on t1.id = t2.id where id > 10
     */
    public String getStrJoinOn(String rightSql) {
        List<String> sqlCon = new ArrayList<>();
        sqlCon.add("where");
        sqlCon.add("group");
        sqlCon.add("order");
        sqlCon.add("limit");

        String left = "";
        for (int i = 0; i < sqlCon.size(); i++){
            String tag = sqlCon.get(i);
            left = leftStr( rightSql, tag );
            if( !"".equals(left) ){
                // 这个就是 sql 最后面部分
                String right = rightStr( rightSql, tag );
                lastSql = tag + " " + right;
                break;
            }
        }
        // 如果存在以上标示那么就取分割后的左边字符
        String strCon = rightSql;
        if( !"".equals(left) ){
            strCon = left;
        }
        return strCon;
    }

    /**
     * 将 join on 由字符串类型转成  hashmap 类型
     * @param rightSql
     * @return
     */
    public List<HashMap<String, String>> getHmJoinOn(String rightSql){
        String strJoinON = getStrJoinOn(rightSql);
        List<String> tempArr = new ArrayList<>();
        String right = strJoinON;
        while ( true ){
            String left = leftStr( right, "and" );
            if( "".equals( left ) ){
                tempArr.add( right );
                break;
            }
            tempArr.add( left );
            right = rightStr( right, "and" );

        }
        List<HashMap<String, String>> listHm = changeSqlArrToHm( tempArr );
        return listHm;
    }

    /**
     * 将字符串数据转成 hashmap 列表结构
     * @param joinArr
     * @return
     */
    private List<HashMap<String, String>> changeSqlArrToHm(List<String> joinArr) {
        System.out.println(joinArr);
        List<String>  tagList = new ArrayList<>();
        tagList.add("<>");
        tagList.add("<=");
        tagList.add(">=");
        tagList.add("<");
        tagList.add(">");
        tagList.add("=");

        List<HashMap<String, String>> resultList = new ArrayList<>();
        for (int i = 0; i < joinArr.size(); i++){
            String joinCon = joinArr.get(i);
            for(int j = 0; j < tagList.size(); j++){
                int index = joinCon.indexOf( tagList.get(j) );
                // 如果以上的标识那么就处理，处理完成后可以马上退出
                if( index > 0 ){
                    HashMap hm = getHmOneCon( joinCon, tagList.get(j) );
                    resultList.add( hm );
                    break;
                }
            }
        }
        return resultList;
    }

    /**
     *  将单个的字符串转成 hashmap
     * @param joinCon
     * @param tag
     * @return
     */
    private HashMap<String, String> getHmOneCon(String joinCon, String tag) {
        String left = leftStr( joinCon, tag);
        String right = rightStr( joinCon, tag);

        String key = leftStr( left, "." ) + "_" + leftStr( right, "." );
        String leftCol = rightStr( left, "." );
        HashMap<String, String>  hm = new HashMap<>();
        String rightValue = right;
        String rightCol = rightStr( right, "." );
        if( "".equals( rightCol ) ){
            hm.put( "rightVal", rightValue );
        }
        hm.put( "schema", key.trim() );
        hm.put( "leftCol", leftCol );
        hm.put( "rightCol", rightCol );
        hm.put( "tag", tag );
        return  hm;
    }

    /**
     * 将 insert部分的数据转成 hashmap 结构
     * @param sql
     * @return
     */
    public HashMap<String, String> insertSql(String sql){
        HashMap<String, String> hashMap = new HashMap();
        String left = leftStr( sql, "select").trim();
        String right = rightStr( left, " ").trim();
        String mode = leftStr( right, " " ).trim();
        if( !"".equals( mode) ){
            hashMap.put("mode", mode);
        }
        String schemaTable = rightStr( right, " " ).trim();
        String schema = leftStr( schemaTable, "." ).trim();
        if( !"".equals(schema) ){
            hashMap.put("schema", schema);
        }
        String table = rightStr( schemaTable, "." ).trim();
        if( !"".equals(table) ){
            hashMap.put("table", table);
        }
        return hashMap;
    }

    /**
     * 获取 select 最左边的 sql
     * @param sql
     * @return
     */
    public String leftSql( String sql ){
        String selectSql = "select " + rightStr( sql, "select");
        String left = leftStr( selectSql, "from" );
        String parentSqlLeft = left + " " + "from";
        return  parentSqlLeft;
    }

    /**
     * 将 sql 切分成小 sql
     * @param sql
     * @return
     */
    public List<String> sqlListTemp( String sql ){
        String originSql = sql;
        List<String> arr = new ArrayList<>();
        // 判断是不是单表操作
        if( sql.indexOf("(") < 0 ){
            arr.add( sql );
            return arr;
        }
        sql = rightStr( sql, "from");
        // 拼配 as t1 join | as t2 right join
        Pattern p = Pattern.compile( "as\\s+\\w+((\\s+join)|(\\s+\\w+\\s+join))" );
        Matcher m = p.matcher(sql);

        int index = 0;
        while(m.find()) {
            int index2 = m.end() - "join".length();
            String temSql = sql.substring(index, index2);
            arr.add( temSql );
            index = index2 + "join".length();
        }
        // 最后一段 子 sql
        String lastSql = sql.substring(index);
        int onIndex = lastSql.indexOf("on");
        if( onIndex < 0 ){ // 如果没有 on 说明是单表操作
            arr.add( originSql );
            return arr;
        }
        arr.add( lastSql.substring(0, onIndex) );
        // 获取 on 后面的内容
        String onSql = lastSql.substring( onIndex + "on".length() );
        arr.add( onSql );
        return arr;
    }

    /**
     * 标准化 sql
     */
    public List<HashMap<String, String>> sqlListHm(List<String> sqlListTemp) {
        List<HashMap<String, String>> resultList = new ArrayList<>();
        HashMap<String, String> hm = new HashMap<>();
        int size = sqlListTemp.size();
        if( size == 1 ){ // 如果只有一个那么说明是但表操作
            String sql = sqlListTemp.get(0);
            String fromStr = rightStr(sql, "from").trim();
            String schema = leftStr(fromStr, ".").trim();
            schema = schema.trim();
            hm.put("schema", schema);
            String table = this.getTableName( fromStr );
            hm.put("table", table);
            hm.put("sql", this.getTrueSql(sql, schema) );
            resultList.add( hm );
        }
        // 多表操作
        for( int i = 0; i < size - 1; i++ ){
            // 如果有 ) 括号就一定是 sql
            String sql = sqlListTemp.get(i);
            int index = sql.indexOf(")");
            if( index > 0 ){
                hm = getSqlTableStruct( sql );
            }else{
                // 如果没有就是表
                hm = getTableStruct(sql);
            }
            resultList.add(hm);
        }
        return resultList;
    }

    /**
     * 获取 有 sql 的表结构，比如 (select * from db1.user where id>0) as t1 join ...
     * @param sql
     * @return
     */
    private HashMap<String,String> getSqlTableStruct(String sql) {
        HashMap<String, String> hm = new HashMap<>();

        String temSql = leftStr(sql, ")");
        temSql = rightStr( temSql, "(");

        String fromRight = rightStr( temSql, "from");
        String schema = leftStr(fromRight, ".").trim();
        hm.put("schema", schema);

        String trueSql = this.getTrueSql(temSql, schema);
        hm.put("sql", trueSql );

        String table = this.getTableName(fromRight);
        hm.put("table", table);

        String lastRight = rightStr( sql, ")").trim();
        lastRight = rightStr( lastRight, "as").trim();
        String alias = "";
        if( lastRight.indexOf("inner") > 0 || lastRight.indexOf("right") > 0 || lastRight.indexOf("left") > 0 || lastRight.indexOf("full") > 0){  //有链接操作
            alias = leftStr( lastRight, " ").trim();
        }else{
            alias = leftStr( lastRight, "").trim();
        }
        hm.put("alias", alias);
        String join = rightStr( lastRight, alias).trim();
        hm.put("join", join);
        return hm;
    }

    /**
     * 获取表名
     * @param fromRight
     * @return
     */
    private String getTableName(String fromRight) {
        String temTaple = rightStr( fromRight, ".").trim();
        String table = leftStr( temTaple, " ").trim();
        if( "".equals( table ) ){
            table = temTaple;
        }
        return table;
    }

    /**
     * 获取表结构，比如 left join db1.user as t1 on t1.id = t2.id
     * @param sql
     * @return
     */
    private HashMap<String,String> getTableStruct(String sql) {
        HashMap<String, String> hm = new HashMap<>();
        hm.put("sql", "");
        String schema = leftStr(sql, ".").trim();
        hm.put("schema", schema);
        String rightSql = rightStr( sql, ".").trim();
        String table = leftStr( rightSql, " ").trim();
        hm.put("table", table);
        rightSql = rightStr( rightSql, "as").trim();
        String alias = "";
        if( rightSql.indexOf("inner") > 0 || rightSql.indexOf("right") > 0 || rightSql.indexOf("left") > 0 || rightSql.indexOf("full") > 0){ //有链接操作
            alias = leftStr( rightSql, " ").trim();
        }else{
            alias = leftStr( rightSql, "").trim();
        }

        hm.put("alias", alias);
        String join = rightStr( rightSql, alias).trim();
        hm.put("join", join);
        return  hm;
    }

    /**
     * 获取真实的 sql, 比如 select ＊ from schema1.db -> select * from db,
     * @param schema
     * @return
     */
    private String getTrueSql(String sql, String schema) {
        String trueSql = sql.replaceAll( schema + ".", "");
        return trueSql;
    }

    /**
     * 获取 sql tag 左边的字符串
     * @param sql
     * @param tag
     * @return
     */
    public String leftStr(String sql, String tag){
        String leftStr = this.sideStr(sql, tag, "left", false);
        return leftStr;
    }

    public String lastLeftStr(String sql, String tag){
        String leftStr = this.sideStr(sql, tag, "left", true);
        return leftStr;
    }

    /**
     * 获取 sql tag 右边的字符串
     * @param sql
     * @param tag
     * @return
     */
    public String rightStr(String sql, String tag){
        String rightStr = this.sideStr(sql, tag, "right", false);
        return rightStr;
    }

    public String lastRightStr(String sql, String tag){
        String rightStr = this.sideStr(sql, tag, "right", true);
        return rightStr;
    }

    /**
     * 获取字符串 str 中 tag 的左边字符串或右边字符串
     * @param str
     * @param tag
     * @param dir left or right
     * @return
     */
    private String sideStr(String str, String tag, String dir, boolean last ){
        if( "".equals(tag) ){
            return str.substring(0);
        }
        int index;
        if( last ){
            index = str.lastIndexOf( tag );
        }else{
            index = str.indexOf( tag );
        }
        if( index < 0 ){
            return "";
        }
        String restltStr = "";
        if( "left".equals(dir) ){
            restltStr = str.substring(0, index);
        }else{
            restltStr = str.substring(index+tag.length());
        }
        return restltStr;
    }

}
