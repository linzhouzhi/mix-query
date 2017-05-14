package logic;

import core.MixQuery;
import core.QueryParam;
import net.sf.json.JSONObject;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lzz on 17/5/1.
 */
public class QueryLogic {

    public JSONObject process(JSONObject requestBody){
        MixQuery mixQuery = new MixQuery();
        QueryParam queryParam = new QueryParam( requestBody );
        DataFrame df = mixQuery.startJob( queryParam );
        df.show();
        Row[] resdf = df.take(2);
        JSONObject jsonObject = new JSONObject();
        String[] header = df.columns();
        List<Map> list = new ArrayList<>();
        for( int i = 0; i < resdf.length; i++ ){
            Map hm = changeRowHm(header, resdf[i]);
            list.add( hm );
        }
        jsonObject.put("result", list);
        return jsonObject;
    }

    private Map changeRowHm(String[] header, Row row) {
        int size = row.length();
        Map hm = new HashMap();
        for( int i = 0; i < size; i++ ){
            hm.put( header[i], row.get(i) );
        }
        return hm;
    }
}
