.PHONY: all watch view clean
all: main.pdf
main.pdf: main.tex refs.bib
	latexmk -pdf main.tex
watch:
	latexmk -pdf -pvc main.tex
view: main.pdf
	open -a Skim main.pdf
clean:
	latexmk -C
