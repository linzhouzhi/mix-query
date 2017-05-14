package service; /**
 * Created by lzz on 17/5/11.

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IndexController {

    @RequestMapping("/index")
    public void greeting(@RequestParam(value="name", defaultValue="World") String name) {
        util.Service service = new util.Service();
        service.process();
        System.out.println( "hello " + name );
    }
}
 */
