
all:
	pdflatex "\def\infile{ducc-cancel.tex}\input{so.tex}"; mv so.pdf ducc-cancel.pdf
	pdflatex "\def\infile{ducc-submit.tex}\input{so.tex}"; mv so.pdf ducc-submit.pdf

latex:
	latex "\def\infile{ducc-submit.tex}\input{so.tex}"; 

dejunk:
	@-rm -rf *.dvi *.log *.4ct *.4tc *.aux *.lg *.log *.tmp *.xref *.idv

clean:
	@-rm -rf *.dvi *.log *.4ct *.4tc *.aux *.css *.html *.lg *.log *.tmp *.xref *.idv *.pdf
