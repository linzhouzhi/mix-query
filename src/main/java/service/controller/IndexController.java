package service.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Created by lzz on 17/5/14.
 */

@Controller
public class IndexController {
    @RequestMapping("/add/sql")
    public String addSql(Model model) {
        model.addAttribute("name", "hello world");
        return "add_sql";
    }
}
