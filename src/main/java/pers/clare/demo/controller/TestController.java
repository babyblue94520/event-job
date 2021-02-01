package pers.clare.demo.controller;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.web.bind.annotation.*;
import pers.clare.demo.data.User;
import pers.clare.demo.service.UserService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RestController
@RequestMapping("test")
public class TestController {

    @Autowired
    private UserService userService;

    @ApiOperation("效能測試")
    @GetMapping
    public String stringTest(
            @ApiParam(value = "執行緒數量", example = "8")
            @RequestParam(required = false, defaultValue = "8") final int thread
            , @ApiParam(value = "緩存數量", example = "1000000")
            @RequestParam(required = false, defaultValue = "1000000") final int max

    ) throws Exception {
        ExecutorService executors = Executors.newFixedThreadPool(thread);
        StringBuilder sp = new StringBuilder();
        long start = System.currentTimeMillis();
        List<Callable<Integer>> tasks = new ArrayList<>();
        for (int t = 0; t < thread; t++) {
            tasks.add(() -> {
                for (int i = 0; i < max; i++) {
                    userService.find(i);
                }
                return max;
            });
        }
        long total = 0;
        List<Future<Integer>> futures = executors.invokeAll(tasks);
        for (Future<Integer> f : futures) {
            total += f.get();
        }
        long ms = System.currentTimeMillis() - start;
        sp.append("total time:" + ms + '\n');
        sp.append("average time:" + (total / (ms) * 1000) + '\n');
        executors.shutdown();
        return sp.toString();
    }

}
