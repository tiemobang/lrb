//
// Created by Zhenyu Song on 10/30/18.
//

#ifndef WEBCACHESIM_SIMULATION_H
#define WEBCACHESIM_SIMULATION_H

#include <string>
#include <chrono>
#include <fstream>
#include <vector>
#include <bsoncxx/builder/basic/document.hpp>
#include <unordered_set>
#include <map>
#include <random>
#include "cache.h"
#include "bsoncxx/document/view.hpp"
#include "bloom_filter.h"


/*
 * single thread simulation. Returns results.
 */
bsoncxx::builder::basic::document simulation(const vector<string> trace_files, std::string cache_type,
                                             uint64_t cache_size, std::map<std::string, std::string> params);

using namespace webcachesim;

class FrameWork {
public:
    bool uni_size = false;
    uint64_t segment_window = 1000000;
    //unit: second
    uint64_t real_time_segment_window = 600;
    uint n_extra_fields = 0;
    bool is_metadata_in_cache_size = false;
    unique_ptr<Cache> webcache = nullptr;
    int64_t n_early_stop = -1;  //-1: no stop
    int64_t seq_start = 0;

    std::string _cache_type;
    uint64_t _cache_size;
    const unordered_set<string> offline_algorithms = {"Belady", "BeladySample", "RelaxedBelady", "BinaryRelaxedBelady",
                                                      "PercentRelaxedBelady"};
    bool is_offline;

    /*
     * bloom filter
     */
    bool bloom_filter = false;
    AkamaiBloomFilter *filter;

    //=================================================================
    //simulation parameter
    int64_t t, id, size, usize, next_seq;
    int same_chunk;

    struct Stats {
        //measure every segment
        int64_t byte_req = 0, byte_miss = 0, obj_req = 0, obj_miss = 0;
        //rt: real_time
        int64_t rt_byte_req = 0, rt_byte_miss = 0, rt_obj_req = 0, rt_obj_miss = 0;
        //global statistics
        std::vector<int64_t> seg_byte_req, seg_byte_miss, seg_object_req, seg_object_miss;
        std::vector<int64_t> seg_rss;
        std::vector<int64_t> seg_byte_in_cache;
        //rt: real_time
        std::vector<int64_t> rt_seg_byte_req, rt_seg_byte_miss, rt_seg_object_req, rt_seg_object_miss;
        std::vector<int64_t> rt_seg_rss;

        void update_real_time_stats(const size_t &metadata_overhead) {
            rt_seg_byte_miss.emplace_back(rt_byte_miss);
            rt_seg_byte_req.emplace_back(rt_byte_req);
            rt_seg_object_miss.emplace_back(rt_obj_miss);
            rt_seg_object_req.emplace_back(rt_obj_req);
            rt_byte_miss = rt_obj_miss = rt_byte_req = rt_obj_req = 0;
            rt_seg_rss.emplace_back(metadata_overhead);
        }

        void update_stats(const size_t &metadata_overhead) {
            seg_byte_miss.emplace_back(byte_miss);
            seg_byte_req.emplace_back(byte_req);
            seg_object_miss.emplace_back(obj_miss);
            seg_object_req.emplace_back(obj_req);
            byte_miss = obj_miss = byte_req = obj_req = 0;
            seg_rss.emplace_back(metadata_overhead);
        }
    };

    //measure every segment
    int64_t byte_req = 0, byte_miss = 0, obj_req = 0, obj_miss = 0;
    //rt: real_time
    int64_t rt_byte_req = 0, rt_byte_miss = 0, rt_obj_req = 0, rt_obj_miss = 0;
    //global statistics
    std::vector<int64_t> seg_byte_req, seg_byte_miss, seg_object_req, seg_object_miss;
    std::vector<int64_t> seg_rss;
    std::vector<int64_t> seg_byte_in_cache;
    //rt: real_time
    std::vector<int64_t> rt_seg_byte_req, rt_seg_byte_miss, rt_seg_object_req, rt_seg_object_miss;
    std::vector<int64_t> rt_seg_rss;
    uint64_t time_window_end;
    std::vector<uint16_t> extra_features;
    // Stats by first extra feature
    std::map<int64_t, Stats> stats_by_extra_feature;
    //use relative seq starting from 0
    uint64_t seq = 0;
    std::chrono::system_clock::time_point t_now;
    //decompose miss
//    int64_t byte_miss_cache = 0;
//    int64_t byte_miss_filter = 0;


    FrameWork(const vector<string> &trace_files, const std::string &cache_type, const uint64_t &cache_size,
              std::map<std::string, std::string> &params);

    bsoncxx::builder::basic::document simulate();

    bsoncxx::builder::basic::document simulation_results();

    void adjust_real_time_offset();

    void update_real_time_stats();

    void update_stats();

private:
    // State for reading input files
    std::vector<std::ifstream> infiles;
    std::vector<std::string> _trace_files;
    std::vector<size_t> files_choosable;
    std::vector<size_t> files_suitable_time;
    struct TempInput {
        int64_t seq;
        int64_t time;
        std::streampos pos;
    };
    std::vector<TempInput> temp_input;
    // TODO: Set more sophisticated random generator
    std::default_random_engine gen;

    void update_metrics_req(const int64_t &size);
    void update_metrics_req(const int64_t &size, const std::vector<uint16_t> &extra_features);

    void update_metrics_miss(const int64_t &size);
    void update_metrics_miss(const int64_t &size, const std::vector<uint16_t> &extra_features);

    bool read_trace(int64_t &next_seq, int64_t &t, int64_t &id, int64_t &size);
};



#endif //WEBCACHESIM_SIMULATION_H
