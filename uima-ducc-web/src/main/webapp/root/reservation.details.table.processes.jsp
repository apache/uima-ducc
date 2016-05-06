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
<caption><b>Processes List</b><br><i><small>click column heading to sort</small></i></caption>
<tr>
<td>
	<table class="sortable">
	<thead>
	<tr class="ducc-head">
	<th class="sorttable_numeric" title="The system assigned id for this process">Id</th>
	<th class="none"              title="The log file name associated with this process">Log</th>
	<th class="sorttable_numeric" title="The log file size, in MB">Log<br>Size</th>
	<th class="none"              title="The host for this process">Host<br>Name</th>
	<th class="sorttable_numeric" title="The OS assigned PID for this process">PID</th>
	<th class="none"              title="Process scheduling state">State<br>Scheduler</th>
	<th class="none"              title="Process scheduling reason (for scheduling state)">Reason<br>Scheduler<br><small>or extraordinary status</small></th>
	<th class="none"              title="Process agent state">State<br>Agent</th>
	<th class="none"              title="Process agent reason (for agent state)">Reason<br>Agent</th>
	<th class="sorttable_numeric" title="Process exit code or signal">Exit</th>
	<th class="none"              title="Process run time (not including process initialization time), hh:mm:ss, as reported by the agent managing the process. The agent reports run start and end times. The time reported here is the difference between them, unless the process is still running then the end time is estimated as now.">Time<br>Run</th>
	<th class="sorttable_numeric" title="Process count of major faults which required loading a memory page from disk">PgIn</th>
	<th class="sorttable_numeric" title="Process GB swapped out to disk, current if state=running or maximum if state=completed">Swap</th>
	<th class="sorttable_numeric" title="%CPU time, as percentage of process lifetime">%CPU</th>
	<th class="sorttable_numeric" title="Resident Storage Size in GB, current if state=running or maximum if state=completed">RSS</th>
	</tr>
	</thead>
	<tbody id="processes_list_area">
	</tbody>
	</table> 				
</table>
   