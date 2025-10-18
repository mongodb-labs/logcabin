/* Copyright (c) 2012-2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * This is a basic latency/bandwidth benchmark of LogCabin.
 */

// std::atomic header file renamed in gcc 4.5.
// Clang uses <atomic> but has defines like gcc 4.2.
#if __GNUC__ == 4 && __GNUC_MINOR__ < 5 && !__clang__
#include <cstdatomic>
#else
#include <atomic>
#endif
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <ctime>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <LogCabin/Client.h>
#include <LogCabin/Debug.h>
#include <LogCabin/Util.h>

using LogCabin::Client::Cluster;
using LogCabin::Client::Result;
using LogCabin::Client::Status;
using LogCabin::Client::Tree;
using LogCabin::Client::Util::parseNonNegativeDuration;

enum OperationType
{
    READ,
    WRITE,
};

std::ostream &operator<<(std::ostream &os, const OperationType &operationType)
{
    switch (operationType)
    {
    case READ:
        return os << "read";
    case WRITE:
        return os << "write";
    default:
        return os << "UNKNOWN";
    }
}

/**
 * Parses argv for the main function.
 */
class OptionParser
{
public:
    OptionParser(int &argc, char **&argv)
        : argc(argc)
        , argv(argv)
        , cluster("logcabin:5254")
        , logPolicy("")
        , size(1024)
        , readOperations(6666)
        , writeOperations(3333)
        , timeout(parseNonNegativeDuration("30s"))
        , resultsFileName("")
    {
        while (true)
        {
            static struct option longOptions[] = {{"cluster", required_argument, NULL, 'c'},
                                                  {"help", no_argument, NULL, 'h'},
                                                  {"size", required_argument, NULL, 's'},
                                                  {"timeout", required_argument, NULL, 't'},
                                                  {"reads", required_argument, NULL, 'r'},
                                                  {"writes", required_argument, NULL, 'w'},
                                                  {"verbose", no_argument, NULL, 'v'},
                                                  {"verbosity", required_argument, NULL, 256},
                                                  {"resultsFile", required_argument, NULL, 'f'},
                                                  {0, 0, 0, 0}};
            int c = getopt_long(argc, argv, "c:hs:t:r:w:vf:", longOptions, NULL);

            // Detect the end of the options.
            if (c == -1)
                break;

            switch (c)
            {
            case 'c':
                cluster = optarg;
                break;
            case 't':
                timeout = parseNonNegativeDuration(optarg);
                break;
            case 'h':
                usage();
                exit(0);
            case 's':
                size = uint64_t(atol(optarg));
                break;
            case 'r':
                readOperations = uint64_t(atol(optarg));
                break;
            case 'w':
                writeOperations = uint64_t(atol(optarg));
                break;
            case 'v':
                logPolicy = "VERBOSE";
                break;
            case 256:
                logPolicy = optarg;
                break;
            case 'f':
                resultsFileName = optarg;
                break;
            case '?':
            default:
                // getopt_long already printed an error message.
                usage();
                exit(1);
            }
        }
    }

    void usage()
    {
        std::cout << "Reads and writes repeatedly to LogCabin. Stops once it reaches "
                  << "the given number of" << std::endl
                  << "operations or the timeout, whichever comes first." << std::endl
                  << std::endl
                  << "This program is subject to change (it is not part of "
                  << "LogCabin's stable API)." << std::endl
                  << std::endl

                  << "Usage: " << argv[0] << " [options]" << std::endl
                  << std::endl

                  << "Options:" << std::endl

                  << "  -c <addresses>, --cluster=<addresses>  "
                  << "Network addresses of the LogCabin" << std::endl
                  << "                                         "
                  << "servers, comma-separated" << std::endl
                  << "                                         "
                  << "[default: logcabin:5254]" << std::endl

                  << "  -h, --help              "
                  << "Print this usage information" << std::endl

                  << "  --size <bytes>          "
                  << "Size of value in each write [default: 1024]" << std::endl

                  << "  --timeout <time>        "
                  << "Time after which to exit [default: 30s]" << std::endl

                  << "  --reads <num>      "
                  << "Number of read operations [default: 6666]" << std::endl

                  << "  --writes <num>     "
                  << "Number of write operations [default: 3333]" << std::endl

                  << "  --resultsFile <file>    "
                  << "Output file for operations/sec value" << std::endl

                  << "  -v, --verbose           "
                  << "Same as --verbosity=VERBOSE" << std::endl

                  << "  --verbosity=<policy>    "
                  << "Set which log messages are shown." << std::endl
                  << "                          "
                  << "Comma-separated LEVEL or PATTERN@LEVEL rules." << std::endl
                  << "                          "
                  << "Levels: SILENT, ERROR, WARNING, NOTICE, VERBOSE." << std::endl
                  << "                          "
                  << "Patterns match filename prefixes or suffixes." << std::endl
                  << "                          "
                  << "Example: Client@NOTICE,Test.cc@SILENT,VERBOSE." << std::endl;
    }

    int &argc;
    char **&argv;
    std::string cluster;
    std::string logPolicy;
    uint64_t size;
    uint64_t readOperations;
    uint64_t writeOperations;
    uint64_t timeout;
    std::string resultsFileName;
};

uint64_t getPercentile(const std::vector<uint64_t> &sortedLatencies, double percentile)
{
    if (sortedLatencies.size() == 0)
        return 0;
    size_t index = static_cast<size_t>(percentile * sortedLatencies.size());
    return sortedLatencies[std::min(index, sortedLatencies.size() - 1)];
}

void calculatePercentiles(std::vector<uint64_t> latencies, uint64_t &p50, uint64_t &p90,
                          uint64_t &p95)
{
    std::sort(latencies.begin(), latencies.end());
    p50 = getPercentile(latencies, 0.50);
    p90 = getPercentile(latencies, 0.90);
    p95 = getPercentile(latencies, 0.95);
}

/**
 * Return the time since the Unix epoch in nanoseconds.
 */
uint64_t timeNanos()
{
    struct timespec now;
#ifdef DEBUG
    int r = clock_gettime(CLOCK_REALTIME, &now);
    assert(r == 0);
#else
    clock_gettime(CLOCK_REALTIME, &now);
#endif
    return uint64_t(now.tv_sec) * 1000 * 1000 * 1000 + uint64_t(now.tv_nsec);
}

template <typename T> std::string toString(const T &t)
{
    std::stringstream ss;
    ss << t;
    return ss.str();
}

int main(int argc, char **argv)
{
    try
    {
        OptionParser options(argc, argv);
        LogCabin::Client::Debug::setLogPolicy(
            LogCabin::Client::Debug::logPolicyFromString(options.logPolicy));

        uint64_t readOperations = options.readOperations;
        uint64_t writeOperations = options.writeOperations;
        NOTICE("Performing %lu reads and %lu writes", readOperations, writeOperations);
        uint64_t startNanos = timeNanos();
        uint64_t nowNanos = startNanos;
        uint64_t logIntervalNanos = 1000000000; // 1 second
        uint64_t nextLogTimeNanos = startNanos + logIntervalNanos;
        uint64_t nextReadTimeNanos = 
            readOperations > 0 ? startNanos : std::numeric_limits<uint64_t>::max();
        uint64_t nextWriteTimeNanos = 
            writeOperations > 0 ? startNanos : std::numeric_limits<uint64_t>::max();
        uint64_t whenToStopNanos = startNanos + options.timeout;
        std::mutex resultsMutex;
        std::vector<uint64_t> readLatencies;
        std::vector<uint64_t> writeLatencies;
        uint64_t writesSucceeded = 0, writesFailed = 0, readsSucceeded = 0, readsFailed = 0;
        {
            Cluster cluster = Cluster(options.cluster);
            Tree tree = cluster.getTree();
            tree.setTimeout(100000000); // 100 ms

            std::string key("/bench");
            std::string value(options.size, 'v');

            while (nowNanos < whenToStopNanos)
            {
                if (nowNanos >= nextReadTimeNanos)
                {
                    tree.asyncRead(key,
                                   [&](Result result, uint64_t startNanos, uint64_t stopNanos)
                                   {
                                       if (result.status == Status::OK)
                                       {
                                           ++readsSucceeded;
                                           std::lock_guard<std::mutex> lock(resultsMutex);
                                           readLatencies.push_back(stopNanos - startNanos);
                                       }
                                       else
                                       {
                                           ++readsFailed;
                                       }
                                   });
                    nextReadTimeNanos += uint64_t(options.timeout / readOperations);
                }
                if (nowNanos >= nextWriteTimeNanos)
                {
                    tree.asyncWrite(key, value,
                                    [&](Result result, uint64_t startNanos, uint64_t stopNanos)
                                    {
                                        if (result.status == Status::OK)
                                        {
                                            ++writesSucceeded;
                                            std::lock_guard<std::mutex> lock(resultsMutex);
                                            writeLatencies.push_back(stopNanos - startNanos);
                                        }
                                        else
                                        {
                                            ++writesFailed;
                                        }
                                    });
                    nextWriteTimeNanos += uint64_t(options.timeout / writeOperations);
                }

                if (nowNanos >= nextLogTimeNanos)
                {
                    nextLogTimeNanos += logIntervalNanos;
                    NOTICE("writes ok: %lu\twrites failed: %lu\treads ok: %lu\treads failed: %lu",
                           writesSucceeded, writesFailed, readsSucceeded, readsFailed);
                }

                uint64_t nextOperationTimeNanos = std::min(nextReadTimeNanos, nextWriteTimeNanos);
                if (nextOperationTimeNanos > nowNanos)
                {
                    uint64_t sleepNanos = nextOperationTimeNanos - nowNanos;
                    __useconds_t sleep_us = static_cast<__useconds_t>(sleepNanos / 1000);
                    usleep(sleep_us);
                }

                nowNanos = timeNanos();
            }
            
            NOTICE("closing client");
        } // destroy cluster and tree

        std::cout << "Benchmark took " << static_cast<double>(timeNanos() - startNanos) / 1e6
                  << " ms to do " << (readLatencies.size() + writeLatencies.size()) << " operations"
                  << std::endl;

        if (options.resultsFileName != "")
        {
            std::ofstream f(options.resultsFileName);
            f << "operationType,opsPerSec,p50latencyNanos,p90latencyNanos,p95latencyNanos"
              << std::endl;
            auto sec = static_cast<double>(timeNanos() - startNanos) / 1e9;
            f << "read," << (static_cast<double>(readsSucceeded) / sec) << ",";
            uint64_t p50, p90, p95;
            calculatePercentiles(readLatencies, p50, p90, p95);
            f << p50 << "," << p90 << "," << p95 << std::endl;

            f << "write," << (static_cast<double>(writesSucceeded) / sec) << ",";
            calculatePercentiles(writeLatencies, p50, p90, p95);
            f << p50 << "," << p90 << "," << p95 << std::endl;
        }

        return 0;
    }
    catch (const LogCabin::Client::Exception &e)
    {
        std::cerr << "Exiting due to LogCabin::Client::Exception: " << e.what() << std::endl;
        exit(1);
    }
}
