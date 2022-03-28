package com.atguigu.demo.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** @Author Hana @Date 2022-03-18-23:24 @Description : */

// @Controller
@RestController // = @Controller + @ResponseBody
public class ControllerTest {
  @RequestMapping("test1")
  // @ResponseBody
  public String test() {
    System.out.println("123");
    return "success";
  }

  @RequestMapping("testParam")
  public String test2(@RequestParam("name") String na,
                      @RequestParam("age") Integer a) {
    System.out.println("22222");
    return "name: " + na + "age: " + a;
  }
}
