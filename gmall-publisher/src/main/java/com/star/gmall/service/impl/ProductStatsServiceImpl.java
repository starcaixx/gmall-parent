package com.star.gmall.service.impl;

import com.star.gmall.bean.ProductStats;
import com.star.gmall.mapper.ProductStatsMapper;
import com.star.gmall.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class ProductStatsServiceImpl implements ProductStatsService {

    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }
    @Override
    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit) {
        return productStatsMapper.getProductStatsGroupBySpu(date,  limit);
    }

    @Override
    public List<ProductStats> getProductStatsGroupByCategory3(int date, int limit) {
        return productStatsMapper.getProductStatsGroupByCategory3(date,  limit);
    }

    @Override
    public List<ProductStats> getProductStatsByTrademark(int date,int limit) {
        return productStatsMapper.getProductStatsByTrademark(date,  limit);
    }
}
