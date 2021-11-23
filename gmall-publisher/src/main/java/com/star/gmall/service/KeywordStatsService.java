package com.star.gmall.service;

import com.star.gmall.bean.KeywordStats;

import java.util.List;

public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}
