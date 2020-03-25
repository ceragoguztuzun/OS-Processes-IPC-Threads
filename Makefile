all: ex mvp mvt 

ex: ex.c
	gcc -Wall  -o ex ex.c -lm

mvp: mvp.c
	gcc -Wall  -o mvp mvp.c -lm

mvt: mvt.c
	gcc -Wall  -o mvt mvt.c -lpthread -lm

clean: 
	rm -fr *~  mv mvp mvt