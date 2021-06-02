#include <iostream>
#include <pthread.h>
#include <unistd.h>

#include "instrument.h"
#include "RDMAHelpers.h"

using namespace std;

//Shared Memory Regions
instrument_t			instrument;
MemoryRegionInfo_t		mri_Instrument;


void PrintUsage()
{
	printf("usage: sub [ -l ip ]\n");
	printf("\t[-l ip] - use the local interface associated with this ip\n");
}

int main(int argc,char *argv[], char *envp[])
{
	//Create the Instrument
	instrument.Symbol[0] = 'M';
    instrument.Symbol[1] = 'L';
    instrument.Symbol[2] = 'N';
    instrument.Symbol[3] = 'X';
    instrument.Symbol[4] = '\0';
    instrument.Value = 1.0;

	int op;
	while ((op = getopt(argc, argv, "l:")) != -1)
	{
		switch (op)
		{
		case 'l':
			s_srcAddr = optarg;
			break;
		default:
			PrintUsage();
			return -1;
		}
	}

	fprintf(stdout, "********  ********  ********  ********\n");
	fprintf(stdout,"MARKET DATA SUBSCRIBER\n");
	fprintf(stderr, "Local IPoIB Address:      %s\n", s_srcAddr);
	fprintf(stdout, "********  ********  ********  ********\n\n");

	if(RDMACreateChannel() != 0)
	{
		fprintf(stderr, "Exiting - Failed Create the RDMA cHannel.\n");
		return 0;
	}

	if(RDMAServerInit()!= 0)
	{
		fprintf(stderr, "Exiting - Failed to Initialize the Server Side CM Connection.\n");
		return 0;
	}

	if(RDMAServerConnect() != 0)
	{
		fprintf(stderr, "Exiting - Failed to establish connection with the client\n");
		return 0;
	}

	//Register the Memory Region
	ibv_mr* mr_mrinfo = create_MEMORY_REGION(&mri_Instrument, sizeof(MemoryRegionInfo_t));
	ibv_mr* mr_instrument = create_MEMORY_REGION(&instrument, sizeof(instrument_t));

	//Set the address Space Information to the address and rkey from instrument.
	mri_Instrument.addr = mr_instrument->addr;
	mri_Instrument.rkey = mr_instrument->rkey;

	//Send the Publisher the Address and RKey for the Memory Region.
	fprintf(stderr, "Sending the Publisher my Address and RKey for the Instruments Memory Region\n");
	fprintf(stderr, "Server Addr: %u\n", mri_Instrument.addr);
	fprintf(stderr, "Server RKey: %u\n", mri_Instrument.rkey);
	fprintf(stderr, "Waiting for Client to post Receive Buffer ...\n");
	sleep(2);

	//Create a Send WQE - Containing the Address
	ibv_send_wr* sWQE = create_SEND_WQE(&mri_Instrument, sizeof(MemoryRegionInfo_t), mr_mrinfo);

	//Post the Receive WQE
	post_SEND_WQE(sWQE);

	//Wait For Completion
	PollCQ();

	/*
	 * Monitor my memory ever second and print the number.
	 * The Publisher has a channel and the key to write to my memory. It will change without me doing anything.
	 */
	fprintf(stdout, "********  ********  ********  ********\n");
	fprintf(stdout,"Displaying Instrument number Every Second\n");
	while(true)
	{
		printf("\rUPDATE (%s,%f)", instrument.Symbol, instrument.Value);
        fflush(stdout);
        sleep (1);
	}

	CleanUpQPContext();
	CleanUpCMContext();

	return 0;
}
