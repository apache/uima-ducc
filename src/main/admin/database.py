#!/usr/bin/python

# -----------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# -----------------------------------------------------------------------

# This is common code to deal wiht the orientdb configuration xml

global domloaded
try:
    from xml.dom.minidom import parse
    domloaded = True
except ImportError:
    domloaded = False 


def db_dom(dbconfig):
    return parse(dbconfig)


#    <users>
#        <user resources="*" password="asdfasdf" name="root"/>
#    </users>

def get_db_password(ducc_home, dbconfig):
    dom = db_dom(ducc_home + '/' + dbconfig)
    
    lst = dom.getElementsByTagName('users')   # returns a Nodelist object
    for u in lst:
        childs = u.childNodes
        for c in childs:
            if (c.nodeName == 'user'):
                atts = c.attributes
                name = atts.getNamedItem('name').nodeValue
                pw   = atts.getNamedItem('password').nodeValue
                if ( name == 'root' ):
                    return pw
    return None

def update_head(dbconfig, head):
    # update the listeners with the correct head, return the dom
    dom = db_dom(dbconfig)

    lst = dom.getElementsByTagName('listener')
    for l in lst:
        atts = l.attributes
        l.setAttribute('ip-address', head)
        print 'Listener for protocol', atts.getNamedItem('protocol').value, 'set to', head
    return dom

def write_config(dbconfig, dom):
    config = open(dbconfig, 'w')
    config.write(dom.toxml())
    config.close()
        
def configure(dbconfig, head, pw):
    print 'Configure', dbconfig, 'at', head, 'pw', pw

    # parse the config, update the head node in the 'listeners' section
    dom = update_head(dbconfig, head)

    # set the root pw
    lst = dom.getElementsByTagName('users')   # returns a Nodelist object
    for u in lst:
        childs = u.childNodes
        for c in childs:
            if (c.nodeName == 'user'):
                atts = c.attributes
                name = atts.getNamedItem('name').nodeValue
                if ( name == 'root' ):
                    c.setAttribute('password', pw)

    # and rewrite the config
    write_config(dbconfig, dom)

