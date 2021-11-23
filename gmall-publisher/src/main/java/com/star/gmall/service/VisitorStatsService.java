package com.star.gmall.service;

import com.star.gmall.bean.VisitorStats;

import java.util.List;

public interface VisitorStatsService {
    public List<VisitorStats> getVisitorStatsByNewFlag(int date);

    public List<VisitorStats> getVisitorStatsByHour(int date);

    public Long getPv(int date);

    public Long getUv(int date);
}
