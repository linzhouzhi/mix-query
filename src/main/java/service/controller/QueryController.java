package service.controller;

/**
 * Created by lzz on 17/5/14.
 */

import net.sf.json.JSONObject;
import org.springframework.web.bind.annotation.*;
import logic.QueryLogic;

@RestController
public class QueryController {

    @RequestMapping(value="/query", method = RequestMethod.POST)
    public JSONObject greeting(@RequestBody JSONObject requestBody) {
        QueryLogic queryLogic = new QueryLogic();
        JSONObject jsonObject = queryLogic.process( requestBody );
        return jsonObject;
    }

    @RequestMapping(value="/test", method = RequestMethod.GET)
    public JSONObject test() {
        JSONObject jsonObject = new JSONObject();
        return jsonObject;
    }
}