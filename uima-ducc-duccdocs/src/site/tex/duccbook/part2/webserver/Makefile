
all:
	htlatex job-skel.tex
	htlatex job-details-skel.tex
	htlatex reservations-skel.tex
	$(MAKE) dejunk

latex:
	latex "\def\infile{ducc-submit.tex}\input{so.tex}"; 

dejunk:
	@-rm -rf *.dvi *.log *.4ct *.4tc *.aux *.lg *.log *.tmp *.xref *.idv

clean:
	@-rm -rf *.dvi *.log *.4ct *.4tc *.aux *.css *.html *.lg *.log *.tmp *.xref *.idv *.pdf
