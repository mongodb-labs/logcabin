/* Permission to use, copy, modify, and distribute this software for any
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
 * Continuously read and write to LogCabin to test its availability.
 */

#include <chrono>
#include <cmath>
#include <cstring>
#include <ctime>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <LogCabin/Client.h>
#include <LogCabin/Debug.h>
#include <LogCabin/Util.h>
#include <cassert>

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;
using std::chrono::steady_clock;
using std::chrono::time_point;

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
        , timeoutNanos(parseNonNegativeDuration("30s"))
        , resultsFileName("")
        , writesPerMs(10)
        , readsPerMs(20)
    {
        while (true)
        {
            static struct option longOptions[] = {{"cluster", required_argument, NULL, 'c'},
                                                  {"help", no_argument, NULL, 'h'},
                                                  {"size", required_argument, NULL, 's'},
                                                  {"timeout", required_argument, NULL, 'd'},
                                                  {"verbose", no_argument, NULL, 'v'},
                                                  {"verbosity", required_argument, NULL, 256},
                                                  {"out", required_argument, NULL, 'o'},
                                                  {"writesPerMs", required_argument, NULL, 'w'},
                                                  {"readsPerMs", required_argument, NULL, 'r'},
                                                  {0, 0, 0, 0}};
            int c = getopt_long(argc, argv, "c:hs:t:o:n:vw:r:", longOptions, NULL);

            // Detect the end of the options.
            if (c == -1)
                break;

            switch (c)
            {
            case 'c':
                cluster = optarg;
                break;
            case 'd':
                timeoutNanos = parseNonNegativeDuration(optarg);
                break;
            case 'h':
                usage();
                exit(0);
            case 's':
                size = uint64_t(atol(optarg));
                break;
            case 'v':
                logPolicy = "VERBOSE";
                break;
            case 256:
                logPolicy = optarg;
                break;
            case 'o':
                resultsFileName = optarg;
                break;
            case 'w':
                writesPerMs = uint64_t(atol(optarg));
                break;
            case 'r':
                readsPerMs = uint64_t(atol(optarg));
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
        std::cout << "Reads and writes continuously to LogCabin until timeout." << std::endl
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

                  << "  --out <file>        "
                  << "Output file for list of operations" << std::endl

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
                  << "Example: Client@NOTICE,Test.cc@SILENT,VERBOSE." << std::endl
                  << "  --writes-per-ms <num>   "
                  << "Number of writes per millisecond [default: 10]" << std::endl
                  << "  --reads-per-ms <num>    " << "Number of reads per millisecond [default: 20]"
                  << std::endl;
    }

    int &argc;
    char **&argv;
    std::string cluster;
    std::string logPolicy;
    uint64_t size;
    uint64_t timeoutNanos;
    std::string resultsFileName;
    uint64_t writesPerMs;
    uint64_t readsPerMs;
};

class ZipfGenerator
{
public:
    ZipfGenerator(int n, double s)
        : N(n)
        , exponent(s)
        , probabilities()
        , dist(0.0, 1.0)
        , rd()
        , rng(rd())
    {
        compute_probabilities();
    }

    int generate()
    {
        double r = dist(rng);
        double cumulative = 0.0;

        for (int i = 1; i <= N; ++i)
        {
            cumulative += probabilities[i - 1];
            if (r < cumulative)
            {
                return i;
            }
        }
        return N; // Fallback (shouldn't happen)
    }

private:
    int N;
    double exponent;
    std::vector<double> probabilities;
    std::uniform_real_distribution<double> dist;
    std::random_device rd;
    std::mt19937 rng;

    void compute_probabilities()
    {
        probabilities.resize(N);
        double sum = 0.0;

        // Compute denominator
        for (int i = 1; i <= N; ++i)
        {
            sum += 1.0 / std::pow(i, exponent);
        }

        // Compute probabilities
        for (int i = 1; i <= N; ++i)
        {
            probabilities[i - 1] = (1.0 / std::pow(i, exponent)) / sum;
        }
    }
};

/**
 * Start time, end time, latency, all in nanoseconds.
 */
typedef std::tuple<uint64_t, uint64_t, uint64_t> OperationResult;

/**
 * Return the time since the Unix epoch in nanoseconds.
 */
uint64_t timeNanos()
{
    struct timespec now;
#ifdef NDEBUG
    clock_gettime(CLOCK_REALTIME, &now);
#else
    int r = clock_gettime(CLOCK_REALTIME, &now);
    assert(r == 0);
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
        ZipfGenerator zipf(1000, 0.5);
        std::string value(options.size, 'v');

        uint64_t now = timeNanos();
        uint64_t nextReadTimeNanos = now;
        uint64_t nextWriteTimeNanos = now;
        uint64_t whenToStopNanos = now + options.timeoutNanos;
        std::mutex resultsMutex;
        std::map<OperationType, std::vector<OperationResult>> results;

        {
            Cluster cluster = Cluster(options.cluster);
            Tree tree = cluster.getTree();
            tree.setTimeout(100000000UL); // 100ms

            while (timeNanos() < whenToStopNanos)
            {
                if (now >= nextReadTimeNanos)
                {
                    int key = zipf.generate();
                    tree.asyncRead(std::to_string(key),
                                   [&](Result result, uint64_t startNanos, uint64_t stopNanos)
                                   {
                                       if (result.status == Status::OK)
                                       {
                                           std::lock_guard<std::mutex> lock(resultsMutex);
                                           results[OperationType::READ].push_back(OperationResult(
                                               startNanos, stopNanos, stopNanos - startNanos));
                                       }
                                   });
                    nextReadTimeNanos += uint64_t(1000000 / options.readsPerMs);
                }
                if (now >= nextWriteTimeNanos)
                {
                    int key = zipf.generate();
                    tree.asyncWrite(std::to_string(key), value,
                                    [&](Result result, uint64_t startNanos, uint64_t stopNanos)
                                    {
                                        if (result.status == Status::OK)
                                        {
                                            std::lock_guard<std::mutex> lock(resultsMutex);
                                            results[OperationType::WRITE].push_back(OperationResult(
                                                startNanos, stopNanos, stopNanos - startNanos));
                                        }
                                        else {
                                            VERBOSE("Write failed: %s", result.error.c_str());
                                        }
                                    });
                    nextWriteTimeNanos += uint64_t(1000000 / options.writesPerMs);
                }

                now = timeNanos();
                auto nextOperationTimeNanos = std::min(nextReadTimeNanos, nextWriteTimeNanos);
                if (nextOperationTimeNanos > now)
                {
                    __useconds_t sleep_us =
                        static_cast<__useconds_t>((nextOperationTimeNanos - now) / 1000);
                    usleep(sleep_us);
                }
            }
            
            usleep(1000000); 
        } // destroy Tree and Cluster so threads stop
        
        if (options.resultsFileName != "")
        {
            std::lock_guard<std::mutex> lock(resultsMutex); // just in case
            std::ofstream f(options.resultsFileName);
            f << "operationType,startNanos,stopNanos,latencyNanos" << std::endl;

            for (const auto &operationType : {OperationType::READ, OperationType::WRITE})
            {
                auto result = results[operationType];
                for (const auto &row : result)
                {
                    f << operationType << "," << std::get<0>(row) << "," << std::get<1>(row) << ","
                      << std::get<2>(row) << std::endl;
                }
            }
        }

        return 0;
    }
    catch (const LogCabin::Client::Exception &e)
    {
        std::cerr << "Exiting due to LogCabin::Client::Exception: " << e.what() << std::endl;
        exit(1);
    }
}
