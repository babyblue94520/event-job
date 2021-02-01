package pers.clare.demo.service;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
public class TestService {
    @Autowired
    private UserService userService;

    private Random random = new Random();

//    @Scheduled(cron = "*/1 * * * * ?")
    public void createUser() {
        int id = random.nextInt(100);
        userService.delete(id);
        userService.find(id);
    }
}
