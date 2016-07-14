//-------------------------------------------------------------------------------
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
//      http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//-------------------------------------------------------------------------------
// --------------------------------------------------------------------------------
// IMPORTANT IMPORTANT IMPORTANT
//    ALWAYS update the version even for trivial changes
// IMPORTANT IMPORTANT IMPORTANT
// --------------------------------------------------------------------------------

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include <sys/socket.h>
#include <netinet/in.h>

void do_exit(int rc)
{
    exit(rc);
}

long long stat1 = 0;
void return_stats(int fd)
{
    // These are the fields we have to return 11 longs, whose semantics
    // are a function of the service.
    struct stats {
        long long perf_stat_1;
        long long perf_stat_2;
        long long perf_stat_3;
        long long perf_stat_4;
    } statbuf;

    fprintf(stdout, "Starting send of len %lu\n", sizeof(struct stats));
    if ( 1 ) {
        statbuf.perf_stat_1 = ++stat1;
        statbuf.perf_stat_2 = random() & 0x000000000000ffffL;
        statbuf.perf_stat_3 = random() & 0x000000000000ffffL;
        statbuf.perf_stat_4 = random() & 0x000000000000ffffL;
    } else {
        statbuf.perf_stat_1 = 19;
        statbuf.perf_stat_2 = 41011; 
        statbuf.perf_stat_3 = 31064;
        statbuf.perf_stat_4 = 12238;
    }

    write(fd, &statbuf, sizeof(struct stats));
    fprintf(stdout, "Sent %lld %lld %lld %lld\n", statbuf.perf_stat_1, statbuf.perf_stat_2, statbuf.perf_stat_3, statbuf.perf_stat_4);
}

/**
 * Start a listen socket.
 *  - on accept() form a packet with faked-out stats
 * ... forever, we use ctl-c or ducc cancel to stop this
 */
void service_responder(char *port_s)
{
    int listen_fd;

    if ( port_s == NULL ) {
        fprintf(stderr, "Missing SERVICE_PORT, cannot start.\n");
        do_exit(1);
    }


    char *en = 0;
    long lport = strtol(port_s, &en, 10);
    int port = 0;
    if ( *en ) {
        fprintf(stderr, "Port[%s] is not numeric.\n", port_s);
        do_exit(1);
    }
    port = lport;


    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if ( listen_fd < 0 ) {
        perror("Cannot create listen socket.");
    }

    struct sockaddr_in addr;
    struct sockaddr_in client_addr;
    
    memset(&addr, 0, sizeof(addr));
    memset(&client_addr, 0, sizeof(client_addr));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    bind(listen_fd, (struct sockaddr *) &addr, sizeof(addr));

    listen(listen_fd, 10);

    for ( ; ; ) {
        socklen_t len = sizeof(client_addr);
        fprintf(stdout, "Starting accept\n");
        int client_fd = accept(listen_fd, (struct sockaddr *) &client_addr, &len);
        fprintf(stdout, "Received connection on port %d\n", ntohs(client_addr.sin_port));

        return_stats(client_fd);
        close(client_fd);
    }

}

int main(int argc, char **argv, char **envp)
{
    if ( argc != 2 ) {
        fprintf(stdout, "Usage: service [portno]\n");
        do_exit(1);
    }
    srandom(time(NULL));
    service_responder(argv[1]);
    do_exit(0);
}
