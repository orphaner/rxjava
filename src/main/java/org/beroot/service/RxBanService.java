package org.beroot.service;

import org.beroot.loader.RxBanLoader;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class RxBanService {

    public void loadBan() throws InterruptedException {
        RxBanLoader banLoader = new RxBanLoader();
        banLoader.loadBan();
    }

    public void loadParallelBan() throws InterruptedException {
        RxBanLoader banLoader = new RxBanLoader();
        banLoader.loadParallelBan();
    }
}
