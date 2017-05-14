package core;

import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lzz on 17/4/30.
 */
public class QueryParam {
    // 转换前的参数
    private List<Map<String, HashMap<String, String>>> jdbcList;
    private String sql;

    // 转化后的参数
    // 解析后的 sql 列表
    private List<HashMap<String, String>> sqlList;
    // 最外层 sql
    private String parentSql;
    // 链接的条件
    private List<HashMap<String, String>> condition;
    // insert  操作
    private HashMap<String, String> insertHm;

    public QueryParam(JSONObject requestBody) {
        List<Map<String, HashMap<String, String>>> jdbcList = new ArrayList<>();
        List<Map<String, String>> jdbcs = requestBody.getJSONArray("jdbcs");
        for( int i = 0; i < jdbcs.size(); i++ ){
            Map<String, String> hm = new HashMap();
            String host = "jdbc:"+jdbcs.get(i).get("host");
            String user = jdbcs.get(i).get("user");
            String password = jdbcs.get(i).get("password");
            String schema = jdbcs.get(i).get("schema");
            hm.put("host", host);
            hm.put("user", user);
            hm.put("password", password);
            Map<String, HashMap<String, String>> itemHm = new HashMap();
            itemHm.put(schema, (HashMap<String, String>) hm);
            jdbcList.add(itemHm);
        }
        this.jdbcList = jdbcList;
        this.sql = requestBody.getString("sql");
        // 参数格式转化
        adaptParam();
    }

    public QueryParam(){
        List<Map<String, HashMap<String, String>>> jdbcList = new ArrayList<>();
        HashMap hm = new HashMap();
        hm.put("host", "jdbc:"+"mysql://192.168.1.101:3306/test");
        hm.put("user", "root");
        hm.put("password", "root");
        HashMap hm2 = new HashMap();
        hm2.put("db1", hm);
        jdbcList.add( hm2 );
        HashMap hm3 = new HashMap();
        hm3.put("host", "jdbc:"+"mysql://192.168.1.101:3306/test");
        hm3.put("user", "root");
        hm3.put("password", "root");
        HashMap hm4 = new HashMap();
        hm4.put("db2", hm3);
        jdbcList.add( hm4 );
        HashMap hm5 = new HashMap();
        hm5.put("host", "jdbc:"+"mysql://192.168.1.101:3306/finebi");
        hm5.put("user", "root");
        hm5.put("password", "root");
        HashMap hm6 = new HashMap();
        hm6.put("db3", hm5);
        jdbcList.add( hm6 );

        // 设置 jdbc
        this.jdbcList = jdbcList;
        //this.sql = "insert overwrite db1.user select id, real_name from (select id as id1  from db1.user2) as t1 left join db3.user as t2 on t1.id1=t2.id where id1 >= 2";
        //this.sql = "insert into db1.user select age, real_name from (select * from db1.user2) as t1 left join (select * from db3.user) as t2 on t1.id=t2.id where age >= 2";
        this.sql = "select * from (select id as id1, real_name  from db1.user where id > 0) as t1 join db1.user2 as t2 right join (select * from db3.user) as t3 on t1.id1=t2.id and t2.id=t3.id and t3.id>32 where age > 1";
        // 子查询里面不能有链接，因为我们建立 rdd 是根据表的，所以没法搞，如果要链接可以改成写在外链接
        //this.sql = "select * from (select * from db1.user left join db1.user2 on user.id=user2.id) as t1 join db1.user2 as t2 on t1.id=t2.id";
        //this.sql = "select * from (select * from db1.user) as t1 left join db2.user as t3 join db1.user2 as t2 on t1.id=t3.id and t3.id=t2.id";
        // 目前不支持但表的 insert 操作 this.sql = "insert into db1.user select * from db1.user";
        //this.sql = "select * from db1.user as t1 left join db1.user as t2 on t1.id=t2.id";
        // 参数格式转化
        adaptParam();
    }

    public void adaptParam(){
        //String sql = "select * from (select id as id1, name  from schema1.user where id > 0) as t1 join schema2.user2 as t2 right join scema3.user as t3 on t1.id1=t2.id and t2.id=t3.id and t3.id>1 where t3.id>0";
        String sql = this.getSql();
        SqlParser sqlParser = new SqlParser();
        // 获取 insert 部分的 sql
        HashMap<String, String> insertHm = sqlParser.insertSql( sql );
        System.out.println( insertHm );
        this.setInsertHm( insertHm );

        // 获取 最左边 sql
        String parentSqlLeft = sqlParser.leftSql( sql );

        // sql 预处理
        List<String> sqlListTemp = sqlParser.sqlListTemp( sql );
        System.out.println(sqlListTemp);
        // 结构化的 sql
        List<HashMap<String, String>>  sqlList = sqlParser.sqlListHm( sqlListTemp );
        System.out.println( sqlList );
        this.setSqlList( sqlList);

        String str = sqlListTemp.get( sqlListTemp.size()-1 );
        List<HashMap<String, String>> hmJoinOn = sqlParser.getHmJoinOn( str );
        System.out.println(hmJoinOn);
        this.setCondition( hmJoinOn );

        if( "".equals( SqlParser.lastSql ) || null == SqlParser.lastSql ){
            this.setParentSql( parentSqlLeft + " all_table" );
        }else{
            this.setParentSql( parentSqlLeft + " all_table " + SqlParser.lastSql);
        }

        System.out.println( this );
    }

    public List<Map<String, HashMap<String, String>>> getJdbcList() {
        return jdbcList;
    }

    public void setJdbcList(List<Map<String, HashMap<String, String>>> jdbcList) {
        this.jdbcList = jdbcList;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<HashMap<String, String>> getSqlList() {
        return sqlList;
    }

    public void setSqlList(List<HashMap<String, String>> sqlList) {
        this.sqlList = sqlList;
    }

    public String getParentSql() {
        return parentSql;
    }

    public void setParentSql(String parentSql) {
        this.parentSql = parentSql;
    }

    public List<HashMap<String, String>> getCondition() {
        return condition;
    }

    public void setCondition(List<HashMap<String, String>> condition) {
        this.condition = condition;
    }

    public HashMap<String, String> getInsertHm() {
        return insertHm;
    }

    public void setInsertHm(HashMap<String, String> insertHm) {
        this.insertHm = insertHm;
    }

    @Override
    public String toString() {
        return "core.QueryParam{" +
                "jdbcList=" + jdbcList +
                ", sql='" + sql + '\'' +
                ", sqlList=" + sqlList +
                ", parentSql='" + parentSql + '\'' +
                ", condition=" + condition +
                ", insertHm=" + insertHm +
                '}';
    }
}
