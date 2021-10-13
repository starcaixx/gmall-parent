package com.star.gmall.gmalllogger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller
@RestController
public class FirstController {

    @RequestMapping("/first")
//    @ResponseBody
    public String test() {
        return "this is first controller";
    }
}
