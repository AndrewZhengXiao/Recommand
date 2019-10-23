package com.emotibot.application;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RecommendController {


    @PostMapping("/")
    public String recommendNews(@RequestParam String userId){
        System.out.println(userId);
        return userId;
    }

}
