package service.controller;

/**
 * Created by lzz on 17/5/14.
 */

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import util.Service;

@RestController
public class IndexController {

    @RequestMapping("/index")
    public void greeting(@RequestParam(value="name", defaultValue="World") String name) {
        Service service = new Service();
        service.process();
        System.out.println( "hello " + name );
    }
}