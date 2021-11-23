package com.star.gmall.service;

import com.star.gmall.bean.ProvinceStats;

import java.util.List;

public interface ProvinceStatsService {
    public List<ProvinceStats> getProvinceStats(int date);
}
