package com.atguigu.gmall0105.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

//@RestController     //@RestController = @Controller + @ResponseBody
@Controller
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    //@ResponseBody 决定方法的返回值是网页还是文本，加上返回的文本。
    @RequestMapping("/applog")
    @ResponseBody
    public String applog(@RequestBody String logString){
        System.out.println(logString);
        log.info(logString);

        //分流
        JSONObject jsonObject = JSON.parseObject(logString);

        if (jsonObject.getString("start") != null && jsonObject.getString("start").length() > 0){
            //启动日志
            kafkaTemplate.send("GMALL_STARTUP_0105",logString);
        }else {
            //事件日志
            kafkaTemplate.send("GMALL_EVENT_0105",logString);
        }



        return logString;
    }
}
