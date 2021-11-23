package com.star.gmall.service.impl;

import com.star.gmall.bean.KeywordStats;
import com.star.gmall.mapper.KeywordStatsMapper;
import com.star.gmall.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class KeywordStatsServiceImpl implements KeywordStatsService {
    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}
