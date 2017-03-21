package org.beroot.loader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;

import org.beroot.beans.Ban;
import org.beroot.beans.GpsPoint;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.util.StopWatch;
import org.springframework.web.client.RestTemplate;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RxBanLoader {
    protected static final int BATCH_SIZE = 20_000;
    protected static final int MAX_CONCURRENCY = 4;
    private static final String BANO_CSV_FILE = "/home/nicolas/src/es-dataloading/data/bano/bano-59.csv";

    protected AtomicInteger count = new AtomicInteger(0);
    private ObjectMapper objectMapper = new ObjectMapper();
    private RestTemplate restTemplate = new RestTemplate();

    public void BanLoader() {
        restTemplate
            .getMessageConverters()
            .add(0, new StringHttpMessageConverter(Charset.forName("UTF-8")));
    }

    public void loadBan() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        getFlowable()
            .doOnComplete(() -> {
                stopWatch.stop();
                log.info("Finished reading {} elements in {} s", count.get(), stopWatch.getTotalTimeSeconds());
            })
            .buffer(BATCH_SIZE)
            .subscribe(getFinalConsumer());
    }

    public void loadParallelBan() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int threadCount = Runtime.getRuntime().availableProcessors() + 1;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        getFlowable()
            .doOnComplete(() -> {
                stopWatch.stop();
                log.info("Finished reading {} elements in {} s", count.get(), stopWatch.getTotalTimeSeconds());
                //latch.countDown();
            })
            .buffer(BATCH_SIZE)
            .flatMap(
                (List<Ban> bans) -> Flowable.just(bans)
                    .subscribeOn(Schedulers.from(executor))
                    .map(bulk -> doBulk(bulk)),
                MAX_CONCURRENCY
            )
            .doFinally(() -> executor.shutdown())
            .subscribe(val -> log.info("Subscriber received on {}", Thread.currentThread().getName()));

        latch.await();
    }

    protected Flowable<Ban> getFlowable() {
        return Flowable.fromIterable(getCsvReader())
            .map(csvLine -> Ban.builder()
                .id(csvLine[0])
                .streetNumber(csvLine[1])
                .street1(csvLine[2])
                .zipCode(csvLine[3])
                .city(csvLine[4])
                .location(new GpsPoint(Double.parseDouble(csvLine[6]), Double.parseDouble(csvLine[7])))
                .build());
    }

    private CSVReader getCsvReader() {
        CSVReader csvReader = null;
        try {
            Reader reader = new InputStreamReader(new FileInputStream(BANO_CSV_FILE), "UTF-8");
            csvReader = new CSVReader(reader);
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            log.error("unable to read csv file", e);
        }
        return csvReader;
    }

    protected Consumer<List<Ban>> getFinalConsumer() {
        return (List<Ban> bulk) -> postBulk(getBulkJsonBody(bulk));
    }

    protected int doBulk(List<Ban> bulk) {
        postBulk(getBulkJsonBody(bulk));
        return 0;
    }

    private String getBulkJsonBody(List<Ban> bulk) {
        StringBuffer sb = new StringBuffer();
        bulk.forEach(st -> {
            try {
                sb.append("{\"index\":{\"_index\":\"bano-rx\",\"_type\":\"bano\",\"_id\":\"" + st.getId() + "\"}}\n")
                    .append(objectMapper.writeValueAsString(st))
                    .append("\n");
                count.incrementAndGet();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
        return sb.toString();
    }

    private void postBulk(String jsonBody) {
        log.info("before post");
        HttpHeaders headers = new HttpHeaders();
        MediaType mediaType = new MediaType("application", "json", StandardCharsets.UTF_8);
        headers.setContentType(mediaType);
        HttpEntity<String> entity = new HttpEntity<>(jsonBody, headers);
        HttpEntity<Map> response = restTemplate.exchange("http://localhost:9200/_bulk", HttpMethod.POST, entity, Map.class);
        log.info("after post");
    }
}
