/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
* 
*      http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
<script>
window.onload=init;
function init() {
	hide('q2');
	hide('q3');
	show('q1');
}
function hide(id) {
	//alert('hide:'+id);
	element = document.getElementById(id)
	element.style.display='none';
}
function show(id) {
	//alert('show:'+id);
	element = document.getElementById(id)
	element.style.display='';
}
function goLeft(location) {
	if(location == 'q1') {
		hide('q1');
		hide('q2');
		show('q3');
	}
	else if(location == 'q2') {
		hide('q2');
		hide('q3');
		show('q1');
	}
	else if(location == 'q3') {
		hide('q3');
		hide('q1');
		show('q2');
	}
}
function goRight(location) {
	if(location == 'q1') {
		hide('q3');
		hide('q1');
		show('q2');
	}
	else if(location == 'q2') {
		hide('q1');
		hide('q2');
		show('q3');
	}
	else if(location == 'q3') {
		hide('q3');
		hide('q2');
		show('q1');
	}
}
</script>