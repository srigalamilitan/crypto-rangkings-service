package com.putracode.crypto.controller;

import com.putracode.crypto.model.CoinInfo;
import com.putracode.crypto.model.HistoryData;
import com.putracode.crypto.service.CoinsDataService;
import com.putracode.crypto.utils.Utility;
import io.github.dengliming.redismodule.redistimeseries.Sample;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/coins")
@RequiredArgsConstructor
public class CoinsRangkingController {

    private final CoinsDataService coinsDataService;
    @GetMapping
    public ResponseEntity<List<CoinInfo>> fetchAllCoins(){
        return ResponseEntity.ok().body(coinsDataService.fetchAllCoinsFromRedisJSON());
    }

    @GetMapping("/{symbol}/{timePeeriod}")
    public List<HistoryData> fecthCoinHistoryPerTimePeriod(@PathVariable String symbol,@PathVariable String timePeeriod){
        List<Sample.Value> coinsTsData=coinsDataService.fetchCoinsHistoryPerTimePeriodFromRedisTS(symbol,timePeeriod);
        List<HistoryData> coinsHistory=coinsTsData.stream()
                .map(value -> new HistoryData(Utility.convertUnixTimeToDate(value.getTimestamp()),Utility.round(value.getValue(),2))).collect(Collectors.toList());
        return coinsHistory;
    }
}
