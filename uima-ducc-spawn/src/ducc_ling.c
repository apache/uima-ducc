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
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/resource.h>
#include <stdarg.h>
#include <time.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>

#include "ducc_ling.h"         /* Created by the makefile to define the UID value */

#define VERSION "2.2.1"

/**
 * 2012-05-04 Support -w <workingdir>.  jrc.
 * 2012-05-04 0.6.0 Update version to match current DUCC beta. jrc.
 * 2012-05-13 0.6.1 Update for MAC getpwnam() bug jrc.
 * 2012-05-13 0.6.2 Update to change group as well as userid. jrc.
 * 2012-07-12 0.6.3 RLIMIT_CORE support. jrc
 * 2012-10-04 0.6.4 Renice. jrc
 * 2012-10-26 0.7.0 Redirect stdio to socket (and match DUCC version). jrc
 * 2013-01-04 0.7.1 Skipped so i can match with DUCC level. jrc
 * 2012-10-26 0.7.2 Print local port when redirecting, and match DUCC level. jrc
 * 2013-01-31 0.7.3 Print message '1001' as marker and flush stdout before exec.  ld
 * 2013-01-31 0.7.3a Print message '1002 CONSOLE REDIRECT' with fn.  jrc
 * 2013-03-08 0.8.0 more complete ulimit suport. jrc
 * 2013-03-08 0.8.1 set hard as well as soft ulimits, and make sure they are shown
 *                  in both user and agent logs. jrc
 * 2013-05-07 0.8.2 Implement append (-a) option. jrc
 * 2013-05-07 0.8.3 Implement version (-v) option. jrc
 * 2013-06-14 0.8.4 Don't create log heirarchy before switching ids!  jrc
 * 2013-06-20 0.8.5 Use mode 0777 making directories so umask can control permissions. jrc
 * 2013-06-20 0.8.6 Show full environment being passed to exec-ed process. jrc
 * 2013-07-16 0.8.7 Agh.  MAX_COMPONENTS is restricting depth of directories too much and crashing!.  jrc
 * 2013-07-25 0.8.8 Allow unlimited path elements. jrc
 * 2013-09-15 0.8.9 Common logging and -q option. jrc
 * 2013-10-03 0.8.10 DUCC_CONSOLE_LISTENER=suppress means direct stdin/stderr to /dev/null jrc
 * 2013-11-21 0.8.10 Update version to 1.0.0 for release jrc
 * 2014-02-14 1.0.1 Use initgroups to fully initalize usergrouops.  jrc
 * 2014-02-14 1.1.0 Support DUCC_UMASK to give user control over umask.  jrc
 * 2014-07-16 1.1.1 Bug in group switching; show IDS the process is to run with. jrc
 * 2014-07-16 1.1.2 Send group switching msgs to log_stdout so they get covered by -q option. jrc
 * 2015-04-30 2.0.0 Fix hole and update version for DUCC 2.0. jrc
 * 2015-11-19 2.1.0 Create 2 streams if console port ends with "?splitstreams".  Add timestamp to log. bll
 * 2017-03-08 2.2.1 Set umask before creating logs so permissions of log directories are set correctly. bll
 * 2017-04-27 2.2.1 DUCC should allow the "ducc" user to be other than exactly "ducc".  lrd bll
 * 2017-07-21 2.2.1 Do not allow ducc_ling to run as any system user, e.g. uid < 500. eae
 */

/**
 * Minimum target uid that ducc_ling will try to switch to
 */
#define MIN_UID 500


/**
 * Numbering - every message is numbered to facilitate filtering and identification in
 *             logs and messages.
 *
 * 4000 series - limits
 */

/**
 * BUFFLEN is largest size for our stack buffers.
 * STRLEN  is longest string we'll place into a stack buffer.
 */
#define BUFLEN (4096)
#define STRLEN (BUFLEN-1)

static int quiet = 0;

struct limit_set
{
    char * name;
    int resource;
};

struct limit_set limits[] = {
    { "DUCC_RLIMIT_CORE"   , RLIMIT_CORE},
    { "DUCC_RLIMIT_CPU"    , RLIMIT_CPU},
    { "DUCC_RLIMIT_DATA"   , RLIMIT_DATA},
    { "DUCC_RLIMIT_FSIZE"  , RLIMIT_FSIZE},
    { "DUCC_RLIMIT_MEMLOCK", RLIMIT_MEMLOCK},
    { "DUCC_RLIMIT_NOFILE" , RLIMIT_NOFILE},
    { "DUCC_RLIMIT_NPROC"  , RLIMIT_NPROC},
    { "DUCC_RLIMIT_RSS"    , RLIMIT_RSS},
    { "DUCC_RLIMIT_STACK"  , RLIMIT_STACK},
#ifndef __APPLE__
    { "DUCC_RLIMIT_AS"        , RLIMIT_AS},
    { "DUCC_RLIMIT_LOCKS"     , RLIMIT_LOCKS},
    { "DUCC_RLIMIT_SIGPENDING", RLIMIT_SIGPENDING},
    { "DUCC_RLIMIT_MSGQUEUE"  , RLIMIT_MSGQUEUE},
    { "DUCC_RLIMIT_NICE"      , RLIMIT_NICE},
    { "DUCC_RLIMIT_STACK"     , RLIMIT_STACK},
    { "DUCC_RLIMIT_RTPRIO"    , RLIMIT_RTPRIO},
#endif
};
u_long limits_len = sizeof(limits) / sizeof (struct limit_set);

void
log_stdout(char *format, ...)
{
    if ( quiet ) return;

	va_list pvar;
	va_start(pvar, format);
	vfprintf(stdout, format, pvar);
	va_end(pvar);
}

void
log_stderr(char *format, ...)
{
	va_list pvar;
	va_start(pvar, format);
	vfprintf(stderr, format, pvar);
	va_end(pvar);
}

void version()
{
    log_stdout("050 ducc_ling Version %s compiled %s at %s\n", VERSION, __DATE__, __TIME__);
}


void usage()
{
    log_stderr("999 Usage:\n");
    log_stderr("999   ducc_ling <-v> <-u user> [-q] [-a] [-w workingdir] [-f filepath] -- program_name [program args]\n");
    exit(1);
}


/**
 * Make a subdirectory.
 *
 * Number 2000
 */
int mksubdir(char *path)
{
    char buf[BUFLEN];
    struct stat statbuf;

    // if it exists and is a dir just return
    if ( stat(path, &statbuf) == 0 ) {
        //log_stdout("2210 Directory %s already exists.\n", path);
        if ( ! ( statbuf.st_mode & S_IFDIR) ) {
            log_stderr("2200 Log base %s is not a directory\n", path);
            return 0;
        }
        return 1;
    }

    log_stdout("2000 Creating directory %s\n", path);
    if ( mkdir(path, 0777) != 0 ) {

        if ( errno == EEXIST ) {
            // Terribly, terribly ugly.  Parts of the directory might be made already in the
            // CLI.  It is observed that if NFS is slow, or system dates are a bit off, when this
            // this code starts to run, the existance check above may fail, but the attempt to
            // make the directory will now fail with "already exists".  So we simply repeat the
            // stat to make sure it's a directory and not a regular file.
            if ( stat(path, &statbuf) == 0 ) {
                //log_stdout("2210 Directory %s already exists.\n", path);
                if ( ! ( statbuf.st_mode & S_IFDIR) ) {
                    log_stderr("2200 Log base %s is not a directory\n", path);
                    return 0;
                }
                return 1;
            }
        }

        snprintf(buf, STRLEN, "2100 Cannot create log path component %s", path);
        buf[STRLEN] = '\0';
        perror(buf);
        return 0;
    }
    return 1;
}

/**
 * Concatenate thing to buf inplace within buf without overstepping BUFFLEN.
 * We assume buf is a buffer of length BUFLEN and will forcibly termintate the
 * the string at the end of the buffer.
 *
 * Number 3000
 */
void concat(char *buf, const char *thing)
{
    //
    // Shouldn't happen unless we're sloppy or being hacked. Die hard and fast.
    //
    if ( (strlen(buf) + strlen(thing) + 1 ) > BUFLEN ) {
        log_stderr("3000 Buffer overflow: string length too long to concatenate %s and %s.  maxlen = %d\n",
               buf, thing, BUFLEN);
        exit(1);
    }
    strncat(buf, thing, STRLEN);
    buf[STRLEN] = '\0';
}


/**
 * Walk the directory structure.  Check access until we find a spot where the dir does
 * not exist.  At this point the dir must have rwx privs and all components be dirs. If not,
 * error exit.  If so, then begin creating directories to create the full path.
 *
 * The path created is base/subdir/jobid.
 *
 * base is the user-specified log location.
 * filestem is the agent-specified log name, minus the required pid
 */
char * mklogfile(const char *filepath)
{
    //
    // First step, the base must exist and be writable.
    //
    char buf[BUFLEN];
    char *next_tok = NULL;
    char *final_tok = NULL;

    int i,j = 0;
    char *tmp;
    char *fullpath = strdup(filepath);

    int len = strlen(fullpath);
    int nelems = 0;
    for ( i = 0; i < len; i++ ) {
        if ( fullpath[i] == '/') nelems++;
    }
    char *path_components[nelems+1];  // and one for luck since its free :)

    i = 0;
    log_stdout("2210 Directory %s\n", fullpath);
    for ( next_tok = strtok(fullpath, "/"); next_tok; next_tok = strtok(NULL, "/") ) {
        //printf("Component %d: %s\n", i, next_tok);
        path_components[i++] = next_tok;
    }

    buf[0] = '\0';                    // make it into a "" string
    if ( filepath[0] == '/' ) {       // strtok removes the '/'es so if it's absolute path, need to put one back in
        concat(buf, "/");
    }
    for ( j = 0; j < i-1; j++ ) {
        concat(buf, path_components[j]);
        if ( ! mksubdir(buf) ) {
            return NULL;
        }
        concat(buf, "/");
    }

    tmp = strdup(buf);
    snprintf(buf, STRLEN, "%s%s-%d.log", tmp, path_components[i-1], getpid());
    return strdup(buf);
}

void show_env(char **envp)
{
    int count = -1;
    while ( envp[++count] != NULL ) {
        log_stdout("Environ[%d] = %s\n", count, envp[count]);
    }
}

void query_limits()
{
    struct rlimit limstruct;
    int i;

    for ( i = 0; i < limits_len; i++ ) {
        getrlimit(limits[i].resource, &limstruct);
        char *name = limits[i].name+12;
        log_stdout("4050 Limits: %10s soft[%lld] hard[%lld]\n", name, limstruct.rlim_cur, limstruct.rlim_max);
    }
}

void set_one_limit(char *name, int resource, rlim_t val)
{
    struct rlimit limstruct;
    char * lim_name = &name[5];
    log_stdout("4010 Setting %s to %lld\n", lim_name, val);   // ( bypass DUCC_ in the name. i heart c. )

    getrlimit(resource, &limstruct);
    log_stdout("4020 Before: %s soft[%lld] hard[%lld]\n", lim_name, limstruct.rlim_cur, limstruct.rlim_max);

    // prep the message, which we may never actually use
    char buf[BUFLEN];
    sprintf(buf, "4030 %s limit was not set.", name);

    limstruct.rlim_cur = val;
    limstruct.rlim_max = val;
    int rc = setrlimit(resource, &limstruct);
    if ( rc != 0 ) {
        perror(buf);
        return;
    }

    getrlimit(resource, &limstruct);
    log_stdout("4040 After: %s soft[%lld] hard[%lld]\n", lim_name, limstruct.rlim_cur, limstruct.rlim_max);
}

void set_limits()
{
    int i;

    for ( i = 0; i < limits_len; i++ ) {
         char *climit = getenv(limits[i].name);
         if ( climit != NULL ) {
             char *en = 0;
             rlim_t lim = strtoll(climit, &en, 10);
             if (*en) {
                 log_stderr("4000 %s is not numeric; core limit note set: %s\n", limits[i].name, climit);
                 return;
             }
             set_one_limit(limits[i].name, limits[i].resource, lim);
         }
     }
}

#ifndef __APPLE__
void renice()
{
    char *nicestr = getenv("DUCC_NICE");
    int   niceval = 10;
    if ( nicestr != NULL ) {
        char *en = 0;
        niceval = strtol(nicestr, &en, 10);
        if (*en) {
            log_stderr("4070 NICE: %s is not numeric; nice not set.\n", nicestr);
            return;
        }
    }
    log_stdout("4050 Nice: Using %d\n", niceval);
    int rc = nice(niceval);
    if ( rc < 0 ) {
       perror("4060 Can't set nice.");
    }
}
#else
void renice()
{
    // mac seems to have no 'nice' syscall but we don't care since its only for test and devel anyway
}
#endif

void set_umask()
{
    char *umaskstr = getenv("DUCC_UMASK");
    mode_t umaskval = 0;
    mode_t oldval = 0;
    if ( umaskstr != NULL ) {
        char *en = 0;
        umaskval = strtol(umaskstr, &en, 8);   // note octal is what we support
        if (*en) {
            log_stderr("4080 UMASK: %s is not numeric/octal; umask not set.\n", umaskstr);
            return;
        }
        oldval = umask(umaskval);
        log_stdout("4090 Umask set to O%o.  Old value: O%o\n", umaskval, oldval);
    }
}

void redirect_to_file(char *filepath)
{
    char buf[BUFLEN];

    char *logfile = filepath;
    if ( strncmp(filepath, "/dev/null", strlen("/dev/null")) ) {
        logfile = mklogfile(filepath);
    }

    if ( logfile == NULL ) exit(1);                 // mklogdir creates sufficient erro rmessages

    //snprintf(buf, STRLEN, "%s/%s-%d.log", logdir, log, getpid());
    //buf[STRLEN] = '\0';

    log_stdout("1200 Redirecting stdout and stderr to %s as uid %d euid %d\n", logfile, getuid(), geteuid());

    fflush(stdout);
    fflush(stderr);

    // do we want apend or trunc?
    int fd = open(logfile, O_CREAT + O_WRONLY + O_TRUNC, 0644);
    // dup stdout and stderr into the log file
    if ( fd >= 0 ) {
        int rc1 = dup2(fd, 1);
        int rc2 = dup2(fd, 2);
    } else {
        snprintf(buf, STRLEN, "1300 cannot open file %s", logfile);
        buf[STRLEN] = '\0';
        perror(buf);
        exit(1);
    }

}

/**
 * There is a console listener out in the world somewhere (like an eclipse
 * session with DuccJobSubmit listening to the console).  Connect stdio to that
 * instead of a file.
 * Syntax:  ipaddr:port<?splitstreams>
 */
void redirect_to_socket(char *sockloc)
{
    int sock, sock2;
    int port;
    char *hostname;
    char *portname;
    char *p;
    char buf[BUFLEN];
    int numstreams = 1;

    // Split the host & port apart
    p = strchr(sockloc, ':');
    if ( p == NULL ) {
        log_stderr("1700 invalid socket, missing port: %s\n", sockloc);
        exit(1);
    }
    *p = '\0';
    portname = p+1;
    hostname = sockloc;
    // Check for a query string - "splitstreams" => maintain separate stdout/stderr streams
    p = strchr(portname, '?');
    if (p != NULL) {
    	*p++ = '\0';
    	if (strcmp(p, "splitstreams") == 0) {
    		numstreams = 2;
    	}
    }
    log_stdout("1701 host[%s] port[%s] streams[%d]\n", hostname, portname, numstreams);

    char *en = 0;
    long lport = strtol(portname, &en, 10);
    if ( *en ) {
        log_stderr("1702 Port[%s] is not numeric.\n", portname);
        exit(1);
    }
    port = lport;

    //
    // Note that we require an ip so we can avoid all resolution issues.  The assumption
    // is that socket redirection is requested from DuccJobSubmit which will only
    // use the IP.
    //
    struct in_addr ip;
    //struct hostent *hp;
    if (!inet_aton(hostname, &ip)) {
        log_stderr("1703 Can't parse IP address %s\n", hostname);
        exit(1);
    }
    log_stdout("1704 addr: %x\n", ip.s_addr);

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)&ip.s_addr, (char *)&serv_addr.sin_addr.s_addr, sizeof(serv_addr.sin_addr.s_addr));
    serv_addr.sin_port = htons(port);

    log_stdout("1705 About to connect\n");
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        perror("1707 Error connecting to console socket.");
        exit(1);
    }
    log_stdout("1706 Connected\n");

    struct sockaddr sockname;
    socklen_t namelen = sizeof(sockname);
    if ( getsockname(sock, &sockname,  &namelen ) == 0 ) {
        struct sockaddr_in *sin = (struct sockaddr_in *) &sockname;
        log_stdout("1708 Local port is %d\n", sin -> sin_port);
    } else {
        perror("1709 Cannot get local socket name, ignoring error.");
    }

    // Optionally create a 2nd socket for stderr
	if ( numstreams == 1 ) {
		sock2 = sock;
	} else {
		log_stdout("1705 About to connect for stderr\n");
		sock2 = socket(AF_INET, SOCK_STREAM, 0);
		if (sock2 == -1) {
			log_stdout("1707 Error creating stderr socket %s\n",
					strerror(errno));
		}
		if (connect(sock2, (struct sockaddr *) &serv_addr, sizeof(serv_addr))
				< 0) {
			perror("1707 Error connecting to stderr socket.");
			exit(1);
		}
		log_stdout("1706 Connected stderr\n");

		if (getsockname(sock2, &sockname, &namelen) == 0) {
			struct sockaddr_in *sin2 = (struct sockaddr_in *) &sockname;
			log_stdout("1708 Local stderr port is %d\n", sin2 -> sin_port);
		} else {
			perror("1709 Cannot get local stderr socket name, ignoring error.");
		}
	}

    //
    // Finally, we seem to have viable sockets, redirect get on with life.
    //
    fflush(stdout);
    fflush(stderr);

    int rc0 = dup2(sock, 0);
    int rc1 = dup2(sock, 1);
    int rc2 = dup2(sock2, 2);

    // Identify the separate streams to the console listener
    if (numstreams == 2) {
    	log_stdout("1500 Stream: STDOUT\n");
    	log_stderr("1500 Stream: STDERR\n");
    }
}

int do_append(char *filepath, int argc, char **argv)
{
    FILE *fp = fopen(filepath, "a");
    if ( fp == NULL ) {
        perror("1800 Cannot open file for append.");
        return 1;
    }
    int i = 0;
    for (i = 0; i < argc; i++) {
        fprintf(fp, "%s ", argv[i]);
    }
    fprintf(fp, "\n");
    fclose(fp);
    return(0);
}

void show_ids(char *userid)
{
    int size = getgroups(0, NULL);
    gid_t groups[size];
    getgroups(size, groups);

    log_stdout("1103 Groups:");
    int i = 0;
    for ( i = 0; i < size; i++ ) {
        struct group* gr = getgrgid(groups[i]);
        log_stdout(" %d(%s)", groups[i], gr -> gr_name);
    }
    log_stdout("\n");

    gid_t my_group = getgid();
    gid_t my_effective_group = getegid();
    uid_t my_id = getuid();
    uid_t my_effective_id = geteuid();
    log_stdout("1104 Running with user and group: id %d gid %d eid %d egid %d\n", my_id, my_group, my_effective_id, my_effective_group);

}

/**
 * Proposed calling conventtion:
 *    ducc_ling <duccling args> -- executable_name <executable args>
 * Where
 *    executable is whatever, usually the path to the jova binary
 *    <executable args> are whatever you want to start java with, probably
 *       the JVM parms followed by the app parms
 *    <duccling args> are args for ducc_ling to process.  Perhaps something like:
 *       -u <userid>      - userid to switch to
 *       -f <filepath>    - if provided, ducc_ling will attempt to use this as
 *                          the log path.  The string <pid>.log is appended, where
 *                          <pid> is the process id.  Intermediate directories are
 *                          created as needed.
 *       -w <workingdir>  - if provided, ducc_ling will attempt to cd to the
 *                          specified dir as workingdir before execing to
 *                          the indicated process.
 *       -a               - "append" option - if specified, the args are appended
 *                          to the **exact** file path specified in -f.  This
 *                          provides an efficient way for DUCC to update some logs
 *                          in use space.
 *       -q               - inhibit all informational messages (but not error messages)
 *       -v                 print version and exit
 *
 *     If -f is missing, no redirection is performed and no files are created.
 */
int main(int argc, char **argv, char **envp)
{
    int i;
    int opt;
    char *userid = NULL;
    char *filepath = NULL;
    char *workingdir = NULL;
    char *logfile = NULL;
    struct passwd *pwd= NULL;
    int switch_ids = 0;
    int redirect = 0;
    char buf[BUFLEN];
    int append = 0;

    // don't allow root to exec a process
    if ( getuid() == 0 ) {
        log_stderr("400 Can't run ducc_ling as root\n");
    	exit(1);
    }

    while ( (opt = getopt(argc, argv, "af:w:u:vqh?") ) != -1) {
        switch (opt) {
        case 'a':
            append = 1;
            break;
        case 'u':
            userid = optarg;
            break;
        case 'f':
            filepath = optarg;
            break;
        case 'w':
            workingdir = optarg;
            break;
        case 'v':
            version();
            exit(0);
            break;
        case 'q':
            quiet = 1;
            break;
        case 'h':
        case '?':
            usage();
        default:
            log_stderr("100 Unrecognized argument %s\n", optarg);
            usage();
        }
    }

    argc -= optind;
    argv += optind;

    version();            // this gets echoed into the Agent's log

    if ( userid == NULL ) {
        log_stderr("200 missing userid\n");
        exit(1);
    }

    if ( getenv("DUCC_CONSOLE_LISTENER") != NULL ) {
        log_stdout("302 Redirecting console into socket %s.\n", getenv("DUCC_CONSOLE_LISTENER"));
        redirect = 1;
    } else if ( filepath != NULL ) {
        log_stdout("301 Redirecting console into file %s.\n", filepath);
        redirect = 1;
    }

    // do this here before redirection stdout / stderr
    log_stdout("0 %d\n", getpid());                                         // code 0 means we passed tests and are about to dup I/O

    //	fetch installed "ducc" user passwd structure
    pwd = getpwnam(UID);

    if ( pwd == NULL ) {
        pwd = getpwuid(getuid());
#ifdef __APPLE__
        // Seems theres a bug in getpwuid and nobody seems to have a good answer.  On mac we don't
        // care anyway so we ignore it (because mac is supported for test only).
        if ( pwd == NULL ) {
            log_stdout("600 No \"%s\" user found and I can't find my own name.  Running as id %d", UID, getuid());
        } else {
            log_stdout("600 No \"%s\" user found, running instead as %s.\n", UID, pwd->pw_name);
        }
#else
        log_stdout("600 No \"%s\" user found, running instead as %s.\n", UID, pwd->pw_name);
#endif
    } else if ( pwd->pw_uid != getuid() ) {
	    log_stdout("700 Caller is not %s (%d), not trying to switch ids ... \n", UID, pwd->pw_uid);
        pwd = getpwuid(getuid());
        log_stdout("800 Running instead as %s.\n", pwd->pw_name);
    } else {
        // Invoked by the owning "ducc" id so will try to switch - but may not succeed
        switch_ids = 1;
    }

    //
    //	fetch target user's passwd structure and try switch identities.
    //  if not setuid the switch will fail but carry on anyway.
    //
    if ( switch_ids ) {

        pwd = getpwnam(userid);
        if ( pwd == NULL ) {
            log_stderr("820 User \"%s\" does not exist.\n", userid);
            exit(1);
        }

        // don't allow a change to a "system" uid 
        if ( pwd->pw_uid < MIN_UID ) {
		  log_stderr("900 setuid < %d not allowed. Exiting.\n", MIN_UID);
            exit(1);
        }

        if ( initgroups(userid, pwd->pw_gid) != 0 ) {
            snprintf(buf, STRLEN,  "1100 Unable to initialize groups for %s.", userid);
            buf[STRLEN] = '\0';
            perror(buf);
        } else {
            log_stdout("830 User groups are initialized for %s.\n", userid);
        }

        if ( setgid(pwd->pw_gid) != 0 ) {
            snprintf(buf, STRLEN,  "1101 Unable to switch group for %s.",userid);
            buf[STRLEN] = '\0';
            perror(buf);
        } else {
            log_stdout("840 Switched to group %d.\n", pwd-> pw_gid);
        }

        if ( setuid(pwd->pw_uid) != 0 ) {
            snprintf(buf, STRLEN,  "1102 Unable to switch to user id %s.",userid);
            buf[STRLEN] = '\0';
            perror(buf);
        } else {
            log_stdout("850 Switched to user %d.\n", pwd-> pw_uid);
        }
    }

    uid_t my_effective_id = geteuid();
    if ( my_effective_id == 0 ) {
        log_stdout("851 ID switch fails.  Check ducc_ling ownership and privileges.\n");
        exit(1);
    }

    show_ids(userid);

    set_umask();   // Set umask befor creating logfiles
    if ( redirect && ( filepath != NULL) ) {
        logfile = mklogfile(filepath);
    } else {
        log_stdout("300 Bypassing redirect of log.\n");
    }

    if ( append ) {
        return do_append(filepath, argc, argv);
    }

    set_limits();         // AFTER the switch, set soft and limits if needed

    query_limits();       // Once, for the agent
    renice();

    //
    // Set up logging dir.  We have switched by this time so we can't do anything the user couldn't do.
    //
    if ( redirect ) {
        char *console_port = getenv("DUCC_CONSOLE_LISTENER");
        if ( console_port == NULL ) {
            redirect_to_file(filepath);
        } else if ( !strncmp(console_port, "suppress", strlen("suppress") ) ) {
            log_stdout("303 Redirect stdout and stderr to /dev/null.\n");
            redirect_to_file("/dev/null");
        } else {
            fflush(stdout);
            redirect_to_socket(console_port);
            if ( filepath != NULL ) {
                // on console redirection, spit out the name of the log file it would have been
                log_stdout("1002 CONSOLE_REDIRECT %s\n", logfile);
            }
        }
        // Start with the current date-time
    	time_t now = time(0);
    	log_stdout(ctime(&now));

        version();             // this gets echoed as first message into the redirected log
        query_limits();       // Once, for the user
    }

    //
    // chdir to working dir if specified
    //
    if ( workingdir != NULL ) {
        int rc = chdir(workingdir);
        if ( rc == -1 ) {
            snprintf(buf, STRLEN,  "1110 Unable to switch to working directory %s.", workingdir);
            buf[STRLEN] = '\0';
            perror(buf);
            exit(1);
        }
        log_stdout("1120 Changed to working directory %s\n", workingdir);
    }

    //
    // Translate DUCC_LD_LIBRARY_PATH into LD_LIBRARY_PATH, if it exists.
    //

    int env_index = -1;
    char ** pathstr = NULL;
    while ( envp[++env_index] != NULL ) {
        char *srchstring = "DUCC_LD_LIBRARY_PATH=";
        int len = strlen(srchstring);
        if ( strncmp(envp[env_index], srchstring, len) == 0 ) {
            // log_stdout("3000 Found DUCC_LD_LIBRARY_PATH and it is %s\n", envp[env_index]);
            pathstr = &envp[env_index];
            break;
        }
    }
    if ( pathstr == NULL ) {
	    //log_stdout("3001 Did not find DUCC_LD_LIBRARY_PATH, not setting LD_LIBRARY_PATH.\n");
    } else {
        //
        // We modify the variable in place.
        //
        char *val = getenv("DUCC_LD_LIBRARY_PATH");
        //log_stdout("3002 Changing DUCC_LD_LIBRARY_PATH to LD_LIBRARY_PATH\n");
        sprintf(*pathstr, "LD_LIBRARY_PATH=%s", val);
    }
    show_env(envp);

    //
    // Now just transmogrify into the requested command
    //
    log_stdout("1000 Command to exec: %s\n", argv[0]);
    for ( i = 1; i < argc; i++ ) {
        log_stdout("    arg[%d]: %s\n", i, argv[i]);
    }

    log_stdout("1001 Command launching...\n");
    fflush(stdout);
    fflush(stderr);
    execve(argv[0], argv, envp);                     // just run the passed-in command

    //
    // if we get here it's because exec failed - it never returns if it succeeds.
    //
    snprintf(buf, STRLEN, "1400 cannot exec %s", argv[0]);
    buf[STRLEN] = '\0';
    perror(buf);
    exit(1);
}
