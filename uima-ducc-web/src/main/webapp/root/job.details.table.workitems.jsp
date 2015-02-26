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
<div id=loading_workitems_area>
</div>
<table>
  <caption><b>Work Items List</b><br><i><small>click column heading to sort</small></i></caption>
  <tr>
  <td>
    <table class="sortable">
    <thead>
    <tr class="ducc-head">
    <th title="The system assigned work item sequence number" class="sorttable_numeric">SeqNo</th>
    <th title="The user assigned work item identity">Id</th>
    <th title="The work item status, normally operating or ended">Status</th>
    <th title="The approx. number of seconds elapsed between work item (a) queued for processing and (b) dequeued for start of processing" class="sorttable_numeric">Queue<br>Time</th>
    <th title="The approx. number of seconds elapsed between work item (a) dequeued for start of processing and (b) end of processing" class="sorttable_numeric">Process<br>Time</th>
    <th title="The approx. number of seconds invested in current epoch" class="sorttable_numeric">Investment<br>Time</th>
    <th title="The node for the work item, by address">Node<br>(IP)</th>
    <th title="The node for the work item, by name">Node<br>(Name)</th>
    <th title="The PID for the work item" class="sorttable_numeric">PID</th>
    </tr>
    </thead>
    <tbody id="workitems_data_area">
    </tbody>
    </table>
</table>