

all: ALWAYS
	pdflatex duccbook.tex
	htlatex duccbook.tex
	$(MAKE) dejunk

	$(MAKE) -C part2/webserver

d: ALWAYS
	pdflatex skel.tex
	pdflatex skel.tex
	open skel.pdf

dejunk:
	@-rm -rf *.dvi *.log *.4ct *.4tc *.aux *.lg *.log *.tmp *.xref *.idv *.lof *.toc 


clean: ALWAYS
	rm -rf *.css *.html *.pdf *.dvi *~ *.png *.out
	$(MAKE) dejunk
	$(MAKE) -C part2/webserver clean

ALWAYS:
