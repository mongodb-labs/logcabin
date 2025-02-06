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
#include <cmath>
#include <cstring>
#include <ctime>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <random>
#include <thread>
#include <unistd.h>
#include <vector>

#include <LogCabin/Client.h>
#include <LogCabin/Debug.h>
#include <LogCabin/Util.h>

namespace
{

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

const int THREADS = 10;
const int WRITES_PER_MS = 100;
const int READS_PER_MS = 100;

enum OperationType
{
    READ,
    WRITE,
};

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
        , timeout(parseNonNegativeDuration("30s"))
        , resultsFileName("")
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
                                                  {0, 0, 0, 0}};
            int c = getopt_long(argc, argv, "c:hs:t:o:n:v", longOptions, NULL);

            // Detect the end of the options.
            if (c == -1)
                break;

            switch (c)
            {
            case 'c':
                cluster = optarg;
                break;
            case 'd':
                timeout = parseNonNegativeDuration(optarg);
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
                  << "Example: Client@NOTICE,Test.cc@SILENT,VERBOSE." << std::endl;
    }

    int &argc;
    char **&argv;
    std::string cluster;
    std::string logPolicy;
    uint64_t size;
    uint64_t timeout;
    std::string resultsFileName;
};

struct ThreadResult
{
    ThreadResult()
        : read_latencies()
        , write_latencies()
    {
    }

    // Pair of start time and latency, both in nanoseconds.
    typedef std::vector<std::pair<uint64_t, uint64_t>> latencies_t;
    latencies_t read_latencies, write_latencies;
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

void operationThreadMain(const OptionParser &options, Tree tree, const std::string &value,
                         OperationType operationType, std::atomic<bool> &exit, ThreadResult &result)
{
    ZipfGenerator zipf(100, 1.0);

    while (!exit)
    {
        int key = zipf.generate();
        uint64_t start = duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
        if (operationType == OperationType::READ)
        {
            std::string contents;
            auto result = tree.read(std::to_string(key), contents);
            if (result.status != Status::OK && result.status != Status::LOOKUP_ERROR)
            {
                std::cerr << "Read key '" << key << "': " << result.error;
                ::exit(1);
            }
        }
        else
        {
            tree.writeEx(std::to_string(key), value);
        }

        uint64_t end = duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
        auto latency = end - start;
        if (operationType == OperationType::READ)
        {
            result.read_latencies.push_back({start, latency});
        }
        else
        {
            result.write_latencies.push_back({start, latency});
        }
        int64_t sleep_us =
            1000 / (operationType == OperationType::READ ? READS_PER_MS : WRITES_PER_MS) -
            latency / 1000;
        if (sleep_us > 0)
        {
            usleep(sleep_us);
        }
    }
}

/**
 * Return the time since the Unix epoch in nanoseconds.
 */
uint64_t timeNanos()
{
    struct timespec now;
    int r = clock_gettime(CLOCK_REALTIME, &now);
    assert(r == 0);
    return uint64_t(now.tv_sec) * 1000 * 1000 * 1000 + uint64_t(now.tv_nsec);
}

/**
 * Main function for the timer thread, whose job is to wait until a particular
 * timeout elapses and then set 'exit' to true.
 * \param timeout
 *      Seconds to wait before setting exit to true.
 * \param[in,out] exit
 *      If this is set to true from another thread, the timer thread will exit
 *      soonish. Also, if the timeout elapses, the timer thread will set this
 *      to true and exit.
 */
void timerThreadMain(uint64_t timeout, std::atomic<bool> &exit)
{
    uint64_t start = timeNanos();
    while (!exit)
    {
        usleep(50 * 1000);
        if ((timeNanos() - start) > timeout)
        {
            exit = true;
        }
    }
}

} // anonymous namespace

int main(int argc, char **argv)
{
    try
    {
        OptionParser options(argc, argv);
        LogCabin::Client::Debug::setLogPolicy(
            LogCabin::Client::Debug::logPolicyFromString(options.logPolicy));
        Cluster cluster = Cluster(options.cluster);
        Tree tree = cluster.getTree();
        tree.setTimeout(10000000000); // 10 seconds in nanoseconds.

        std::string value(options.size, 'v');

        std::atomic<bool> exit(false);
        std::thread timer(timerThreadMain, options.timeout, std::ref(exit));
        std::vector<OperationType> operationTypes = {OperationType::READ, OperationType::WRITE};
        std::vector<ThreadResult> resultPerThread{THREADS * operationTypes.size()};
        std::vector<std::thread> threads;
        size_t j = 0;
        for (const auto &operationType : operationTypes)
        {
            for (uint64_t i = 0; i < THREADS; ++i)
            {
                threads.emplace_back(operationThreadMain, std::ref(options), tree, std::ref(value),
                                     operationType, std::ref(exit),
                                     std::ref(resultPerThread.at(j++)));
            }
        }
        timer.join(); // Await timeout.
        for (auto &t : threads)
        {
            t.join();
        }

        if (options.resultsFileName != "")
        {
            std::ofstream f(options.resultsFileName);
            f << "operationType,recordedAtNanos,latencyNanos" << std::endl;

            ThreadResult::latencies_t read_latencies, write_latencies;
            for (const auto &threadResult : resultPerThread)
            {
                read_latencies.insert(read_latencies.end(), threadResult.read_latencies.begin(),
                                      threadResult.read_latencies.end());
                write_latencies.insert(write_latencies.end(), threadResult.write_latencies.begin(),
                                       threadResult.write_latencies.end());
            }
            for (const auto &row : read_latencies)
            {
                f << "read," << row.first << "," << row.second << std::endl;
            }
            for (const auto &row : write_latencies)
            {
                f << "write," << row.first << "," << row.second << std::endl;
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
