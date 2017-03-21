package org.beroot.beans;

import org.beroot.loader.RxBanLoader;
import org.beroot.service.RxBanService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StopWatch;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ReactorApplicationTests {

    @Autowired
    private RxBanService rxjavaService;

    @Test
    public void loadWithRxjava() throws InterruptedException {
        StopWatch start = new StopWatch();
        start.start();
        RxBanLoader banLoader = new RxBanLoader();
        banLoader.loadBan();
        start.stop();
        System.out.println(start.getTotalTimeSeconds());
    }

    @Test
    public void loadWithParallelRxjava() throws InterruptedException {
        StopWatch start = new StopWatch();
        start.start();
        RxBanLoader banLoader = new RxBanLoader();
        banLoader.loadParallelBan();
        start.stop();
        System.out.println(start.getTotalTimeSeconds());
    }
}
