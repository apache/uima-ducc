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
<div>
<table>
<tr>
<td>
<span id="refreshbutton">
<input type="image" onclick="ducc_refresh('system-broker');" title="Refresh" alt="Refresh" src="opensources/images/refresh.png">
</span>
<span id="loading" style="display:none;">
<img title="loading" src="opensources/images/indicator.gif" style="border:1px solid #000000" alt="Loading...">
</span>
<td>
<table>
<tr>
<td align="left">
<form name="duccform" style="margin-bottom:0;">
<fieldset>
<legend>Refresh</legend>
<input type="radio" name="refresh" value="manual"            onclick="ducc_put_cookie('ducc:refreshmode','manual'   )" /> Manual
<br>
<input type="radio" name="refresh" value="automatic" checked onclick="ducc_put_cookie('ducc:refreshmode','automatic')" /> Automatic
</fieldset>
</form>
</table>
</tr>
</table>
</div> 