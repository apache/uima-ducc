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
/*!
 * nodeviz.js
 */


function ducc_viz_node_sorter(what) 
{
    //console.log("sort by " + what);
    //console.log(document);

    var s = document.getElementById("ducc-viz-sort-size");
    var n = document.getElementById("ducc-viz-sort-name");

    if ( (n == null) || (s == null ) ) {
        //console.log("Returning because the nodes aren't loaded yet.");
        return;             // Waiting for stuff to finish loading still
    } 
    //console.log("Starting sort.");

    var comparator;

    if ( what == 'size' ) {
        ducc_put_cookie('viz-sort-order', 'size');

        s.style.color = 'red';
        n.style.color = 'black';
        comparator = function(a, b) {
            var mem_a = parseInt(a.getAttribute("mem"), 10);
            var mem_b = parseInt(b.getAttribute("mem"), 10);
            var ret = (mem_b - mem_a);

            if ( ret == 0 ) {
                var id_a = a.getAttribute("id");
                var id_b = b.getAttribute("id");            
                ret = id_a.localeCompare(id_b);
            }
            return ret;
        }

    } else if ( what == 'name' ) {
        ducc_put_cookie('viz-sort-order', 'name');

        s.style.color = 'black';
        n.style.color = 'red';
        comparator = function(a, b) {
            var id_a = a.getAttribute("id");
            var id_b = b.getAttribute("id");            
            var ret = id_a.localeCompare(id_b);

            if ( ret == 0 ) {
                var mem_a = parseInt(a.getAttribute("mem"), 10);
                var mem_b = parseInt(b.getAttribute("mem"), 10);
                ret = (mem_b - mem_a);
            }
            return ret;
        }
    } else {
        console.log("Illegal sort order: " + what);
    }

    var nodediv = document.getElementById("nodelist");
    // console.log("Nodediv: " + nodediv);

    var node_list = nodediv.childNodes;
    //console.log("Nodes: " + node_list + " length " + node_list.length);

    var listToArray = function(obj) {
        return [].map.call(obj, function(element) {
                return element;
            })
    };

    var node_array = listToArray(node_list).sort(comparator);
    // console.log("node_array len is " + node_array.length);

    for ( i = 0, len = node_array.length; i < len; i++ ) {
        // console.log("Node is " + node_array[i].getAttribute("id"));
        nodediv.appendChild(node_array[i]);
    }
}

function ducc_viz_onreload()
{
    //console.log("Node visualization starts"); 

    var vizsort = ducc_get_cookie('viz-sort-order');
    //console.log('viz-sort-order: ' + vizsort);
    if ( vizsort == null ) {
        ducc_viz_node_sorter('size');
    } else {
        ducc_viz_node_sorter(vizsort);
    }
}
