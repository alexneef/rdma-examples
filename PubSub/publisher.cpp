#include <stdlib.h>
#include <stdio.h>

#include <iostream>
#include <pthread.h>
#include <unistd.h>

#include "instrument.h"
#include "RDMAHelpers.h"

using namespace std;

//Shared Memory Regions
instrument_t			instrument;
MemoryRegionInfo_t		mri_serverInstrument;

int publish()
{
	fprintf(stdout, "MKT UPDATE (%s,%f)\n", instrument.Symbol, instrument.Value);
	return 0;
}

void PrintUsage()
{
	printf("usage: pub [ -l ip ] remote-node\n");
	printf("\t[-l ip] - use the local interface associated with this ip\n");
}

void *MonitorCQ(void* data)
{
	struct ibv_wc wc;
	int ret = 0;

	fprintf(stderr, "Starting Completion Queue Monitor\n");
	do {
		ret = ibv_poll_cq(g_cq, 10, &wc);
		//if(ret > 0)
		//	fprintf(stderr, "REmoved %u CQE's from CQ\n", ret);
	} while(true);
}

int main(int argc,char *argv[], char *envp[])
{
	//Create the Instrument
	instrument.Symbol[0] = 'N';
    instrument.Symbol[1] = 'V';
    instrument.Symbol[2] = 'D';
    instrument.Symbol[3] = 'A';
    instrument.Symbol[4] = '\0';
    instrument.Value = 0;

	int op;
	while ((op = getopt(argc, argv, "l:")) != -1)
	{
		switch (op)
		{
		case 'l':
			s_srcAddr = optarg;
			break;
		default:
			printf("usage: %s [ -l ip ] remote-node remote-node-port\n", argv[0]);
			printf("\t[-l ip] - use the local interface associated with this ip\n");
			return -1;
		}
	}

	if(argc <= optind)
		{
			PrintUsage();
			return -1;
		}
	else
		{
			s_dstAddr = argv[optind++];
			n_dstPort = atoi(argv[optind]);
		}

	fprintf(stdout, "********  ********  ********  ********\n");
	fprintf(stdout,"MARKET DATA PUBLISHER\n");
    fprintf(stdout,"Uses an RC Channel to do RDMA_WRITE's to a MARKET DATA SUBSCRIBER\n");
	fprintf(stderr, "Local IPoIB Address:      %s\n", s_srcAddr);
	fprintf(stderr, "Subscriber IPoIB Address: %s port(%u)\n", s_dstAddr, n_dstPort);
	fprintf(stdout, "********  ********  ********  ********\n\n");

	if(RDMACreateChannel() != 0)
	{
		fprintf(stderr, "Exiting - Failed Create the RDMA Channel.\n");
		return 0;
	}

	if(RDMAClientInit()!= 0)
	{
		fprintf(stderr, "Exiting - Failed to initialize the Client Side CM Connection.\n");
		return 0;
	}

	if(RDMACreateQP() != 0)
	{
		fprintf(stderr, "Exiting - Failed to Create Queue Pair\n");
		return 0;
	}

	if(RDMAClientConnect() != 0)
	{
		fprintf(stderr, "Exiting - Failed to establish connection to client\n");
		return 0;
	}

	/*
	 * The Subscriber will send us the Address and RKey for it's Instrument Memory Region, we will use this to post updates
	 * Demonstrates Message Passing (SEND) Operation
	 */
	//Register the Memory Regions
	ibv_mr* mr_asi = create_MEMORY_REGION(&mri_serverInstrument, sizeof(MemoryRegionInfo_t));
	ibv_mr* mr_instrument = create_MEMORY_REGION(&instrument, sizeof(instrument_t));

	//Create a Receive WQE
	ibv_recv_wr* rWQE = create_RECEIVE_WQE(&mri_serverInstrument, sizeof(MemoryRegionInfo_t), mr_asi);

	//Post the Receive WQE
	post_RECEIVE_WQE(rWQE);

	//Wait For a Completion - this indicates the subscriber has posted the address and rkey for the instrument.
	PollCQ();

	//Print the Information
	fprintf(stderr, "Server Addr: %u\n", mri_serverInstrument.addr);
	fprintf(stderr, "Server RKey: %u\n", mri_serverInstrument.rkey);

	/*
	 *
	 */
	//Create a Send WQE
	ibv_send_wr* writeWQE = create_WRITE_WQE(&instrument, sizeof(instrument_t), mr_instrument, mri_serverInstrument.addr, mri_serverInstrument.rkey);

	/*
	 * Start Monitoring the Completion Queue (CQ) for CQEs
	 */
	int data = 5;
	pthread_t 				CQMonitorThread;
	pthread_create(&CQMonitorThread, NULL, MonitorCQ, (void *) &data);

	/*
	 * Send 10M Updates as fast as we can from the Publisher to the Subscriber
	 * Demonstrated RDMA (RDMA WRITE) Operation
	 */
	for(int i=0; i < 10000000; i++)
	{
		//update the instrument
		instrument.Value++;

		//Post the Receive WQE
		post_SEND_WQE(writeWQE);
		usleep(50);
	}

	CleanUpQPContext();
	CleanUpCMContext();

	return 0;
}
