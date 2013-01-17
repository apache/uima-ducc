#!/usr/bin/env python

# THis provides a way to list the manifest jars vertically since it seems like mvn corrupts the
# manifest if they're listed like this in the pom directly.
#
# Run this script and c/p the stdout into the DUCC_CP property in the pom.
#
def main():

    cp = [ \
        "uima-ducc-cli.jar",
        "uima-ducc-common.jar",
        "uima-ducc-transport.jar",
        "../resources/",
        "http-client/commons-codec-${commons.codec.version}.jar",
        "http-client/commons-httpclient-${commons.httpclient.version}.jar",
        "google-gson/gson-${google.gson.version}.jar ",
        "apache-camel/xstream-${xstream.version}.jar",
        "apache-camel/mina-core-${mina.core.version}.jar",
        "apache-camel/camel-context-${org.apache.camel.version}.jar",
        "apache-camel/camel-core-${org.apache.camel.version}.jar",
        "apache-camel/camel-http-${org.apache.camel.version}.jar",
        "apache-camel/camel-http4-${org.apache.camel.version}.jar",
        "apache-camel/camel-jetty-${org.apache.camel.version}.jar",
        "apache-camel/camel-jms-${org.apache.camel.version}.jar",
        "apache-camel/camel-mina-${org.apache.camel.version}.jar",
        "apache-camel/camel-servlet-${org.apache.camel.version}.jar",
        "apache-camel/camel-spring-${org.apache.camel.version}.jar",
        "apache-camel/camel-stream-${org.apache.camel.version}.jar",
        "apache-camel/camel-test-${org.apache.camel.version}.jar",
        "apache-camel/camel-test-spring-${org.apache.camel.version}.jar",
        "apache-camel/camel-xmlbeans-${org.apache.camel.version}.jar",
        "apache-camel/camel-xstream-${org.apache.camel.version}.jar",
        "apache-log4j/log4j-${log4j.version}.jar",
        "apache-commons-cli/commons-cli-1.2.jar",
        "apache-activemq/activemq-all-${org.apache.activemq.version}.jar",
        "springframework/slf4j-log4j12-${slf4j.version}.jar",
        "springframework/slf4j-api-${slf4j.version}.jar",
        "springframework/jcl-over-slf4j-${slf4j.version}.jar",
        "springframework/com.springsource.net.sf.cglib-2.2.0.jar",
        "springframework/aopalliance-${aopalliance.version}.jar",
        "springframework/spring-aop-${org.springframework.version}.jar",
        "springframework/spring-asm-${org.springframework.version}.jar",
        "springframework/spring-beans-${org.springframework.version}.jar",
        "springframework/spring-context-${org.springframework.version}.jar",
        "springframework/spring-context-support-${org.springframework.version}.jar",
        "springframework/spring-core-${org.springframework.version}.jar",
        "springframework/spring-expression-${org.springframework.version}.jar",
        "springframework/spring-jms-${org.springframework.version}.jar",
        "springframework/spring-tx-${org.springframework.version}.jar",
        "uima/uima-core-2.4.1-20121211.001650-31.jar",
        "uima/uimaj-as-core-2.4.1-20130110.191457-4.jar",
        ]

    print ' '.join(cp)

main()
