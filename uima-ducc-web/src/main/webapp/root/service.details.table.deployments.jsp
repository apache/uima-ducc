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
<table>
<caption><b>Deployments List</b><br><i><small>click column heading to sort</small></i></caption>
<tr>
<td>
	<table class="sortable">
	<thead>
	<tr class="ducc-head">
	<th title="The system assigned id for this process" class="sorttable_numeric">Id</th>
	<th title="The log file name associated with this process">Log</th>
	<th title="The log file size, in MB">Size</th>
	<th title="The host for this process">Host<br>Name</th>
	<th title="The OS assigned PID for this process"class="sorttable_numeric">PID</th>
	<th title="Process scheduling state">State<br>Scheduler</th>
	<th title="Process scheduling reason (for scheduling state)">Reason<br>Scheduler<br><small>or extraordinary status</small></th>
	<th title="Process agent state">State<br>Agent</th>
	<th title="Process agent reason (for agent state)">Reason<br>Agent</th>
	<th title="Process initialization time, ddd:hh:mm:ss">Time<br>Init</th>
    <th title="Process run time (not including process initialization time), ddd:hh:mm:ss">Time<br>Run</th>
	<th title="Process total time spent performing garbage collections, hh:mm:ss">Time<br>GC</th>
	<th title="Process count of major faults which required loading a memory page from disk">PgIn</th>
	<th title="Process GB swapped out to disk, current if state=running or maximum if state=completed">Swap</th>
	<th title="%CPU time, as percentage of process lifetime">%CPU</th>
	<th title="Resident Storage Size in GB, current if state=running or maximum if state=completed">RSS</th>
	<!--
	<th title="Average seconds per work item">Time<br>Avg</th>
	<th title="Maximum seconds for any work item">Time<br>Max</th>
	<th title="Minimum seconds for any work item">Time<br>Min</th>
	<th title="The number of work items that completed successfully">Done</th>
	<th title="The number of work items that failed to complete successfully">Error</th>
	<th title="The number of work items that were retried, excluding preemptions">Retry</th>
	<th title="The number of work items that were preempted">Pre-<br>empt</th>
	-->
	<th title="The JConsole URL for this process">JConsole<br>URL</th>
	</tr>
	</thead>
	<tbody id="deployments_list_area">
	</tbody>
	</table> 				
</table>
   