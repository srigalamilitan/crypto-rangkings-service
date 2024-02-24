package com.putracode.crypto.service;

import com.putracode.crypto.model.*;
import com.putracode.crypto.utils.HttpUtils;
import io.github.dengliming.redismodule.redisjson.RedisJSON;
import io.github.dengliming.redismodule.redisjson.args.GetArgs;
import io.github.dengliming.redismodule.redisjson.args.SetArgs;
import io.github.dengliming.redismodule.redisjson.utils.GsonUtils;
import io.github.dengliming.redismodule.redistimeseries.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@RequiredArgsConstructor
@Slf4j
public class CoinsDataService {
    public static final String GET_COINS_API="https://coinranking1.p.rapidapi.com/coins?referenceCurrencyUuid=yhjMzLPhuIDl&timePeriod=24h&tiers%5B0%5D=1&orderBy=marketCap&orderDirection=desc&limit=50&offset=0";
    public static final String REDIS_KEY_COINS="coins";
    public static final String GET_COIN_HISTORY_API = "https://coinranking1.p.rapidapi.com/coin/";
    public static final String COIN_HISTORY_TIME_PERIOD_PARAM = "/history?timePeriod=";
    public static final List<String> timePeriods = List.of("24h", "7d", "30d", "3m", "1y", "3y", "5y");

    private final RestTemplate restTemplate;
    private final RedisJSON redisJSON;
    private final RedisTimeSeries redisTimeSeries;
    public void fecthCoins(){
        log.info("Inside FetchCoins");
        ResponseEntity<Coins> coinsResponseEntity= restTemplate.exchange(GET_COINS_API, HttpMethod.GET, HttpUtils.getHttpEntity(), Coins.class);
        log.info(GsonUtils.toJson(coinsResponseEntity.getBody()));
        storeCoinsToRedisJson(coinsResponseEntity.getBody());
        log.info("Fetch Data SuccessFully" );
    }

    private void storeCoinsToRedisJson(Coins coins) {
        redisJSON.set(REDIS_KEY_COINS, SetArgs.Builder.create(".", GsonUtils.toJson(coins)));
    }

    public void fetchCoinsHistory(){
        log.info("Inside fetchCoinsHistory");
        List<CoinInfo> allCoins=getAllCoinsFromRedisJSON();
        allCoins.forEach(coinInfo -> {
            timePeriods.forEach(s -> {
                try {
                    fetchCoinHistoryForTimePeriod(coinInfo,s);
                    Thread.sleep(200); // To Avoid Rate Limit of rapid API of 5 Request/Sec
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        });
    }

    private void fetchCoinHistoryForTimePeriod(CoinInfo coinInfo, String timeperiod) {
        log.info("Fetching Coin History of {} for Time Period {}", coinInfo.getName(), timeperiod);
        String url=GET_COIN_HISTORY_API+coinInfo.getUuid()+COIN_HISTORY_TIME_PERIOD_PARAM+timeperiod;
        ResponseEntity<CoinPriceHistory> coinPriceHistoryResponseEntity= restTemplate.exchange(url,HttpMethod.GET,HttpUtils.getHttpEntity(), CoinPriceHistory.class);
        log.info("Data Fetched From API for Coin History of {} for Time Period {}", coinInfo.getName(), timeperiod);
        storeCoinHistoryToRedisTS(coinPriceHistoryResponseEntity.getBody(),coinInfo.getSymbol(),timeperiod);
    }

    private void storeCoinHistoryToRedisTS(CoinPriceHistory coinPriceHistory, String symbol, String timeperiod) {
        log.info("Storing Coin history of {} for time Period {} into Redis TS",symbol,timeperiod);
        List<CoinPriceHistoryExchangeRate> coinPriceHistoryExchangeRates=coinPriceHistory.getData().getHistory();
        coinPriceHistoryExchangeRates.stream()
                .filter(ch-> ch.getPrice()!=null && ch.getTimestamp()!=null)
                .forEach(ch->{
                    redisTimeSeries.add(new Sample(symbol+":"+timeperiod,Sample.Value.of(Long.valueOf(ch.getTimestamp()),Double.valueOf(ch.getPrice()))),
                            new TimeSeriesOptions()
                                    .unCompressed().duplicatePolicy(DuplicatePolicy.LAST));
                });
        log.info("Complete : Stored Coin History of {} for Time Period {} into Redis TS",symbol, timeperiod);

    }

    private List<CoinInfo> getAllCoinsFromRedisJSON() {
        CoinData coinData=redisJSON.get(REDIS_KEY_COINS, CoinData.class,new GetArgs().path(".data").indent("\t").newLine("\n").space(" "));
        return coinData.getCoins();
    }

    public List<CoinInfo> fetchAllCoinsFromRedisJSON() {
        return getAllCoinsFromRedisJSON();
    }

    public List<Sample.Value> fetchCoinsHistoryPerTimePeriodFromRedisTS(String symbol, String timePeeriod) {
        Map<String, Object> tsInfo=fetchTSInfoForSymbol(symbol,timePeeriod);
        Long firstTimeStamp=Long.valueOf(tsInfo.get("firstTimestamp").toString());
        Long lastTimeStamp=Long.valueOf(tsInfo.get("lastTimestamp").toString());
        List<Sample.Value> coinsTSData= fetchTSdataForCoin(symbol,timePeeriod,firstTimeStamp,lastTimeStamp);
        return  coinsTSData;
        
    }

    private List<Sample.Value> fetchTSdataForCoin(String symbol, String timePeeriod, Long firstTimeStamp, Long lastTimeStamp) {
        String key=symbol+":"+timePeeriod;
        return redisTimeSeries.range(key,firstTimeStamp,lastTimeStamp);

    }

    private Map<String, Object> fetchTSInfoForSymbol(String symbol, String timePeeriod) {
        return redisTimeSeries.info(symbol+":"+timePeeriod);
    }
}
