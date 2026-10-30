package com.trading.marketdataservice.service;

import com.trading.marketdataservice.dto.StockPrice;
import com.trading.marketdataservice.entity.Stock;

import java.util.List;

public interface MarketDataService {

    public void initializePrices();
    public void updatePrices();
    public StockPrice getPrice(String symbol);
    public List<StockPrice> getAllPrices();
    public List<Stock> getAllStocks();
}
