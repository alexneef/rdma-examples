all :  pub sub

pub : publisher.o RDMAHelper.o
	g++ -Wall -g publisher.cpp RDMAHelper.cpp -o pub -libverbs -lrdmacm -lpthread

sub : subscriber.o RDMAHelper.o
	g++ -Wall -g subscriber.cpp RDMAHelper.cpp -o sub -libverbs -lrdmacm -lpthread
	
package : 
	tar -cf PubSubExample.tar instrument.h publisher.cpp RDMAHelpers.h makefile RDMAHelper.cpp subscriber.cpp
	
clean : 
	rm *.o sub pub

