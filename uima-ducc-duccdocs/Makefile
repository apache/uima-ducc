
#
# This makefile is convenient for building and cleaning from 'make' because it's *very* fast
# Use maven for 'official' build.
#

all:  ALWAYS
	$(MAKE) -C src/site/tex/duccbook all

clean: ALWAYS
	-@rm *~
	-@$(MAKE) -C src/site/tex/duccbook clean

ALWAYS:
