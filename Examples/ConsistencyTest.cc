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
 * Test LogCabin's consistency, e.g. linearizability or read-your-writes.
 */

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <getopt.h>
#include <iostream>
#include <limits>
#include <string>
#include <sstream>
#include <thread>

#include <google/protobuf/stubs/common.h>
#include <LogCabin/Client.h>
#include <LogCabin/Debug.h>
#include "build/Protocol/ServerStats.pb.h"

namespace
{

    using LogCabin::Client::Cluster;
    using LogCabin::Client::Result;
    using LogCabin::Client::Status;
    using LogCabin::Client::Tree;

    /**
     * Parses argv for the main function.
     */
    class OptionParser
    {
    public:
        OptionParser(int &argc, char **&argv)
            : argc(argc), argv(argv), cluster("logcabin:5254")
        {
            while (true)
            {
                static struct option longOptions[] = {
                    {"cluster", required_argument, NULL, 'c'},
                    {"help", no_argument, NULL, 'h'},
                    {"verbose", no_argument, NULL, 'v'},
                    {0, 0, 0, 0}};
                int c = getopt_long(argc, argv, "c:hmv", longOptions, NULL);

                // Detect the end of the options.
                if (c == -1)
                    break;

                switch (c)
                {
                case 'c':
                    cluster = optarg;
                    break;
                case 'h':
                    usage();
                    exit(0);
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
            std::cout
                << "Checks LogCabin's consistency, e.g. linearizability or "
                << "read-your-writes."
                << std::endl
                << std::endl
                << "This program is subject to change (it is not part of "
                << "LogCabin's stable API)."
                << std::endl
                << std::endl

                << "Usage: " << argv[0] << " [options]"
                << std::endl
                << std::endl

                << "Options:"
                << std::endl

                << "  -c <addresses>, --cluster=<addresses>  "
                << "Network addresses of the LogCabin"
                << std::endl
                << "                                         "
                << "servers, comma-separated"
                << std::endl
                << "                                         "
                << "[default: logcabin:5254]"
                << std::endl

                << "  -h, --help                     "
                << "Print this usage information"
                << std::endl

                << "Comma-separated LEVEL or PATTERN@LEVEL rules."
                << std::endl
                << "                                 "
                << "Levels: SILENT ERROR WARNING NOTICE VERBOSE."
                << std::endl
                << "                                 "
                << "Patterns match filename prefixes or suffixes."
                << std::endl
                << "                                 "
                << "Example: Client@NOTICE,Test.cc@SILENT,VERBOSE."
                << std::endl;
        }

        int &argc;
        char **&argv;
        std::string cluster;
    };

} // anonymous namespace

void executeCommand(const std::string &) {}

std::vector<std::pair<std::string, std::string>> parseHostPortList(const std::string &input)
{
    std::vector<std::pair<std::string, std::string>> result;
    std::istringstream stream(input);
    std::string item;

    // Split the input by commas
    while (std::getline(stream, item, ','))
    {
        auto colonPos = item.find(':');
        if (colonPos != std::string::npos)
        {
            // Extract host and port from the pair
            std::string host = item.substr(0, colonPos);
            std::string port = item.substr(colonPos + 1);
            result.emplace_back(host, port);
        }
        else
        {
            // Handle case where no colon is found
            PANIC("Invalid format: %s", item.c_str());
        }
    }

    return result;
}

std::string joinHostPortList(std::vector<std::pair<std::string, std::string>> hosts)
{
    std::ostringstream oss;
    for (size_t i = 0; i < hosts.size(); ++i)
    {
        oss << hosts[i].first << ":" << hosts[i].second;
        if (i != hosts.size() - 1)
        {
            oss << ",";
        }
    }

    return oss.str();
}

void setupLatency(const std::string &cluster)
{
    NOTICE("Setting up artificial network latency...");

    auto hosts = parseHostPortList(cluster);
    for (const auto &hostPort : hosts)
    {
        const auto port = hostPort.second;
        executeCommand("sudo nft add rule inet logcabin_test input tcp dport " + port + " limit rate 100 bytes/second");
        executeCommand("sudo nft add rule inet logcabin_test output tcp sport " + port + " limit rate 100 bytes/second");
    }

    NOTICE("Artificial latency setup complete.");
}

void partitionServer(Cluster &cluster, const std::string &clusterStr, const std::string &port)
{
    NOTICE("Blocking process on port %s from communicating with peers", port.c_str());
    // Can't use nft for this, it would block client messages as well as intra-server messages.
    auto targetHostPort = std::string{"localhost:"} + port;
    cluster.debugMakePartition(targetHostPort,
                               /* timeout = 2s */ 2000000000000UL,
                               true);

    NOTICE("Process on port %s blocked from communicating with others.", port.c_str());
}

void cleanup()
{
    NOTICE("Cleaning up nftables rules...");
    executeCommand("sudo nft flush table inet logcabin_test");
    executeCommand("sudo nft delete table inet logcabin_test");
    NOTICE("Cleanup complete.");
}

std::string leaderPort(Cluster &cluster, const std::string &clusterStr)
{
    auto hosts = parseHostPortList(clusterStr);
    std::string leaderHostPort;
    for (const auto &hostPort : hosts)
    {
        auto hostPortStr = hostPort.first + ":" + hostPort.second;
        LogCabin::Protocol::ServerStats stats = cluster.getServerStatsEx(
            hostPortStr,
            /* timeout = 2s */ 2000000000000UL);

        if (stats.raft().state() == LogCabin::Protocol::ServerStats_Raft::LEADER)
        {
            return hostPort.second;
        }
    }

    return "";
}

int main(int argc, char **argv)
{
    try
    {

        atexit(google::protobuf::ShutdownProtobufLibrary);
        atexit(cleanup);
        OptionParser options(argc, argv);
        LogCabin::Client::Debug::setLogPolicy(
            LogCabin::Client::Debug::logPolicyFromString(
                "NOTICE"));

        setupLatency(options.cluster);

        // Don't let the Cluster cache the leader identity between the write
        // and read, we want it to be possible to accidentally write to a
        // leader in a newer term and read from a leader in an older term.
        std::string oldLeaderPort;
        {
            Cluster cluster1(options.cluster);
            oldLeaderPort = leaderPort(cluster1, options.cluster);
            NOTICE("Found leader on port %s, creating test file with 'foo'",
                oldLeaderPort.c_str());
            auto tree1 = cluster1.getTree();
            tree1.makeDirectoryEx("/ConsistencyTest");
            tree1.writeEx("/ConsistencyTest/test", "foo");
            partitionServer(cluster1, options.cluster, oldLeaderPort);
        }

        auto hosts = parseHostPortList(options.cluster);
        hosts.erase(
            std::remove_if(hosts.begin(), hosts.end(), [&](const std::pair<std::string, std::string> &elem)
                           { return elem.second == oldLeaderPort; }),
            hosts.end());

        auto cluster2str = joinHostPortList(hosts);
        NOTICE("Remaining hosts: %s", cluster2str.c_str());

        {
            Cluster cluster2(cluster2str);

            // Wait for new leader.
            while (true)
            {
                auto newLeaderPort = leaderPort(cluster2, cluster2str);
                if (!newLeaderPort.empty())
                {
                    NOTICE("Found NEW leader on port %s", newLeaderPort.c_str());
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            auto tree2 = cluster2.getTree();
            NOTICE("Writing bar");
            tree2.writeEx("/ConsistencyTest/test", "bar");
        }

        NOTICE("Reconnect to old leader");
        Cluster cluster3(std::string{"localhost:"} + oldLeaderPort);
        NOTICE("Verifying old leader is still leader");
        auto leaderPort3 = leaderPort(cluster3, std::string{"localhost:"} + oldLeaderPort);
        if (leaderPort3 != oldLeaderPort)
        {
            NOTICE("Old leader is no longer leader, now it's %s", leaderPort3.c_str());
            exit(2);
        }
        NOTICE("Reading from old leader");
        std::string contents = cluster3.getTree().readEx("/ConsistencyTest/test");
        NOTICE("Read %s", contents.c_str());
        if (contents != "bar")
        {
            NOTICE("Consistency violation, %s != bar", contents.c_str());
        }
    }
    catch (const LogCabin::Client::Exception &e)
    {
        WARNING("Exiting due to LogCabin::Client::Exception: %s", e.what());
        exit(1);
    }
}
