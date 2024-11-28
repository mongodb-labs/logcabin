#!/usr/bin/env python
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""
This runs consistency tests against a LogCabin cluster on localhost.

Usage:
  consistencytest.py [options]
  consistencytest.py (-h | --help)

Options:
  -h --help            Show this help message and exit
  --binary=<cmd>       Server binary to execute [default: build/LogCabin]
  --client=<cmd>       Client binary to execute
                       [default: build/Examples/ConsistencyTest]
  --reconf=<opts>      Additional options to pass through to the Reconfigure
                       binary. [default: '']
  --servers=<num>      Number of servers [default: 5]
  --timeout=<seconds>  Number of seconds to wait for client to complete before
                       exiting with an error [default: 10]
"""


from common import sh, Sandbox
from docopt import docopt
import random
import time

def main():
    arguments = docopt(__doc__)
    client_command = arguments['--client']
    server_command = arguments['--binary']
    num_servers = int(arguments['--servers'])
    reconf_opts = arguments['--reconf']
    if reconf_opts == "''":
        reconf_opts = ""
    timeout = int(arguments['--timeout'])

    server_ids_and_ports = [(i + 1, i + 5254) for i in range(num_servers)]
    cluster = "--cluster=%s" % ','.join([
        'localhost:%s' % port for server_id, port in server_ids_and_ports])
    alphabet = [chr(ord('a') + i) for i in range(26)]
    cluster_uuid = ''.join([random.choice(alphabet) for i in range(8)])
    with Sandbox() as sandbox:
        sh('rm -rf storage/')
        sh('rm -f debug/*')
        sh('mkdir -p debug')

        for server_id, port in server_ids_and_ports:
            with open('consistencytest-%d.conf' % server_id, 'w') as f:
                f.write('serverId = %d\n' % server_id)
                f.write('listenAddresses = localhost:%d\n' % port)
                f.write('clusterUUID = %s\n' % cluster_uuid)
                f.write('logPolicy = VERBOSE\n')
                f.write('snapshotMinLogSize = 1024\n')
                f.write('quorumCheckOnRead = false\n')
                f.write('delta = 5000\n')
                try:
                    f.write(open('consistencytest.conf').read())
                except:
                    pass

        print('Initializing first server\'s log')
        sandbox.rsh('localhost',
                    '%s --bootstrap --config consistencytest-%d.conf' %
                    (server_command, server_ids_and_ports[0][0]),
                   stderr=open('debug/bootstrap', 'w'))
        print()

        for server_id, port in server_ids_and_ports:
            command = ('%s --config consistencytest-%d.conf' %
                       (server_command, server_id))
            print('Starting %s on localhost:%d' % (command, port))
            sandbox.rsh('localhost', command, bg=True,
                        stderr=open('debug/%d' % server_id, 'w'))
            sandbox.checkFailures()

        print('Growing cluster')
        sh('build/Examples/Reconfigure %s %s set %s' %
           (cluster,
            reconf_opts,
            ' '.join(['localhost:%s' % port 
                      for server_id, port in server_ids_and_ports])))
        
        print('Starting %s %s on localhost' % (client_command, cluster))
        client = sandbox.rsh('localhost',
                             '%s %s' % (client_command, cluster),
                             bg=True)

        start = time.time()
        while client.proc.returncode is None:
            sandbox.checkFailures()
            time.sleep(.1)
            if time.time() - start > timeout:
                raise Exception('timeout exceeded')

if __name__ == '__main__':
    main()
