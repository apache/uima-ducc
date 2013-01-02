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
package org.apache.uima.ducc.common;

/**
 * This is a simple association of two objects
 */

public class Pair<F, S>
{
    private F f;
    private S s;

    public Pair()
    {
        f = null;
        s = null;
    }

    public Pair(F first, S second)
    {
        this.f = first;
        this.s = second;
    }

    public F first()
    {
        return f;
    }

    public S second()
    {
        return s;
    }

    public String toString()
    {
        return f.toString() + " " + s.toString();
    }

}
