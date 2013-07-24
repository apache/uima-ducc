#!/usr/bin/env python

# THis provides a way to list the manifest jars vertically since it seems like mvn corrupts the
# manifest if they're listed like this in the pom directly.
#
# Run this script and c/p the stdout into the DUCC_CP property in the pom.
#
def main():

    cp = [ \
        "uima-ducc-common.jar",
        "uima-ducc-transport.jar",
        "../resources/",
        "slf4j/jcl-over-slf4j.jar",
        "slf4j/slf4j-api.jar",
        "slf4j/slf4j-log4j12.jar",
        "xstream/xstream.jar",
        "http-client/commons-codec-${commons.codec.version}.jar",
        "http-client/commons-httpclient-${commons.httpclient.version}.jar",
        "google-gson/gson-${google.gson.version}.jar",
        "apache-log4j/log4j-${log4j.version}.jar",
        "apache-commons-cli/commons-cli-${commons.cli.version}.jar",
        "uima/uima-core-${org.apache.uima.version}.jar",
        "uima/uimaj-as-core-${org.apache.uima.as.version}.jar",
        ]

    print ' '.join(cp)

main()
