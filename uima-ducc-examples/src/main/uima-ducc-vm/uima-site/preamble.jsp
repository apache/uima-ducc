<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<h2>What is DUCC?</h2>

<p>
DUCC stands for Distributed UIMA Cluster Computing. 
DUCC is a cluster management system providing tooling, management, 
and scheduling facilities to automate the scale-out of applications 
written to the UIMA framework. 

<p>
<i style="color:#009900;">
Core UIMA
</i>
provides a generalized framework for applications that 
process unstructured information such as human language, 
but does not provide a scale-out mechanism. 
<i style="color:#009900;">
UIMA-AS
</i>
provides a scale-out mechanism to distribute UIMA pipelines 
over a cluster of computing resources, but does not provide job or 
cluster management of the resources. 
<i style="color:#009900;">
UIMA-DUCC 
</i>
defines a formal job 
model that closely maps to a standard UIMA pipeline. Around this 
job model DUCC provides cluster management services to automate the 
scale-out of UIMA pipelines over computing clusters.

<h2>What is ducc-mon?</h2>

<p>
<i style="color:#009900;">
ducc-mon
</i>
stands for Distributed UIMA Cluster Computing Monitor. 
It comprises a web server that is meant to display information about 
the running DUCC including: users' Jobs, users' Reservations, users' 
Services and the DUCC System itself.

<h2>What is the DUCC demo?</h2>

<p>
The 
<i style="color:#009900;">
demo
</i>
comprises a DUCC installation within the apache.org domain. 
A computing cluster has been configured and started, and a continually 
running driver simulates users? work submissions.

<p>
Use a browser to visit 
<a href='http://uima-ducc-vm.apache.org:42133'>http://uima-ducc-vm.apache.org:42133</a>
which yields
the associated web server home page. The visitor is free to navigate and 
explore an actual DUCC working system as a view-only user.

<p>
The purpose of the web server (known as ducc-mon) is to provide information
about the running system. It will help to answer questions such as:
