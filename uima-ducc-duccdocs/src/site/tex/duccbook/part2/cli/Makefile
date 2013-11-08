#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing,
#   software distributed under the License is distributed on an
#   #  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#   KIND, either express or implied.  See the License for the
#   specific language governing permissions and limitations
#   under the License.

all:
	pdflatex "\def\infile{ducc-cancel.tex}\input{so.tex}"; mv so.pdf ducc-cancel.pdf
	pdflatex "\def\infile{ducc-submit.tex}\input{so.tex}"; mv so.pdf ducc-submit.pdf

latex:
	latex "\def\infile{ducc-submit.tex}\input{so.tex}"; 

dejunk:
	@-rm -rf *.dvi *.log *.4ct *.4tc *.aux *.lg *.log *.tmp *.xref *.idv

clean:
	@-rm -rf *.dvi *.log *.4ct *.4tc *.aux *.css *.html *.lg *.log *.tmp *.xref *.idv *.pdf
