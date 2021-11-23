package com.star.gmall.service.impl;

import com.star.gmall.bean.ProvinceStats;
import com.star.gmall.mapper.ProvinceStatsMapper;
import com.star.gmall.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    @Autowired
    ProvinceStatsMapper provinceStatsMapper;
    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
