//
// Created by zhenyus on 1/19/19.
//

#include <string>
#include <regex>
#include "simulation.h"
#include <map>
#include <unordered_set>
#include <cstdlib>
#include <iostream>
#include <vector>
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/json.hpp"
#include "mongocxx/client.hpp"
#include "mongocxx/instance.hpp"
#include "mongocxx/uri.hpp"

using namespace std;
using namespace chrono;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::sub_array;

string current_timestamp() {
    time_t now = system_clock::to_time_t(std::chrono::system_clock::now());
    char buf[100] = {0};
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
    return buf;
}

int main(int argc, char *argv[]) {
    // output help if insufficient params
    if (argc < 4) {
        cerr
                << "webcachesim_cli traceFile cacheType cacheSize [--param=value]"
                << endl;
        return 1;
    }

    for (int i = 0; i < argc; ++i) {
        cerr<<argv[i]<<" ";
    }
    cerr<<endl;

    map<string, string> params;

    // parse cache parameters
    regex opexp ("--([^=]*)=(.*)");
    cmatch opmatch;
    for(int i=4; i<argc; i++) {
        regex_match (argv[i],opmatch,opexp);
        if(opmatch.size()!=3) {
            continue;
        } else {
            params[opmatch[1]] = opmatch[2];
        }
    }


    auto webcachesim_trace_dir = getenv("WEBCACHESIM_TRACE_DIR");
    if (!webcachesim_trace_dir) {
        cerr << "error: WEBCACHESIM_TRACE_DIR is not set, can not find trace" << endl;
        abort();
    }

    string task_id;

    bsoncxx::builder::basic::document key_builder{};
    bsoncxx::builder::basic::document value_builder{};

    for (auto &k: params) {
        //don't store authentication information
        if (unordered_set<string>({"dburi"}).count(k.first)) {
            continue;
        }
        if (!unordered_set<string>({"dbcollection", "task_id", "enable_trace_format_check"}).count(k.first)) {
            key_builder.append(kvp(k.first, k.second));
        } else {
            value_builder.append(kvp(k.first, k.second));
        }
    }

    mongocxx::instance inst;

    auto cacheType = argv[1];
    auto cacheSize = std::stoull(argv[2]);
    std::vector<string> traceFiles;
    for(int i = 3; i < argc; ++i) {
        if (string(argv[i]).find_first_of("=") == std::string::npos) {
            traceFiles.emplace_back(string(webcachesim_trace_dir) + '/' + argv[i]);
        }
    }

    key_builder.append(kvp("trace_file", [&traceFiles](sub_array child) {
            std::vector<string> sortedTraceFiles = traceFiles;
            std::sort(sortedTraceFiles.begin(), sortedTraceFiles.end());
            for(const auto& element: traceFiles)
                child.append(element);
    }));
    key_builder.append(kvp("cache_type", argv[1]));
    key_builder.append(kvp("cache_size", argv[2]));

    if (traceFiles.empty()) {
        cerr << "error: no trace file found" << endl;
        return 1;
    }

    for (bsoncxx::document::element ele: key_builder.view())
        value_builder.append(kvp(ele.key(), ele.get_value()));

    auto timeBegin = chrono::system_clock::now();
    
    auto res = simulation(traceFiles, cacheType, cacheSize,
                          params);
    auto simulation_time = chrono::duration_cast<chrono::seconds>(chrono::system_clock::now() - timeBegin).count();
    auto simulation_timestamp = current_timestamp();

    for (bsoncxx::document::element ele: res.view())
        value_builder.append(kvp(ele.key(), ele.get_value()));
    value_builder.append(kvp("simulation_time", to_string(simulation_time)));
    value_builder.append(kvp("simulation_timestamp", simulation_timestamp));

    cout << bsoncxx::to_json(value_builder.view()) << endl;

    try {
        mongocxx::client client = mongocxx::client{mongocxx::uri(params["dburi"])};
        auto db = client[mongocxx::uri(params["dburi"]).database()];
        mongocxx::options::replace option;
        db[params["dbcollection"]].replace_one(key_builder.extract(), value_builder.extract(), option.upsert(true));
    } catch (const std::exception &xcp) {
        cerr << "warning: db connection failed: " << xcp.what() << std::endl;
    }
    return EXIT_SUCCESS;
}