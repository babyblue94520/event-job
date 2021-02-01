package pers.clare.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pers.clare.demo.data.User;
import pers.clare.demo.service.UserService;

@RestController
@RequestMapping("user")
public class UserController {

    @Autowired
    private UserService userService;

    @GetMapping
    public User find(Integer id){
        return userService.find(id);
    }

    @PutMapping
    public User modify(Integer id){
        return userService.update(id);
    }

    @DeleteMapping
    public void remove(Integer id){
        userService.delete(id);
    }
}
