#include <string>
#include <rdma/rdma_cma.h>
#include <arpa/inet.h>
#include <getopt.h>

#include "rdmautil.h"
#include "message.h"

#define RESOLVE_TIMEOUT 2000

#define NUM_OPERATIONS 10

struct SubContext
{
    struct sockaddr_in6       local_in;
    struct sockaddr           *local_addr;
    struct rdma_event_channel *CMEventChannel;
};
static struct SubContext g_SubContext;

typedef struct ConnectionContext
{
    char*                           subAddrString;
    int                             subPort;
    char*                           pubAddrString;
    int                             pubPort;

    struct sockaddr_in			    pubAddr;
    struct sockaddr_in			    subAddr;
    rdma_event_channel *            CMEventChannel;

    struct rdma_cm_id             *pcmid;
    struct ibv_pd                 *pd;
    struct ibv_cq                 *cq;
    struct ibv_mr                 *mr;

    struct ibv_recv_wr            *bad_wqe;
} ConnectionContext_t ;

int subscribe(ConnectionContext_t* c)
{
    rdma_cm_event_type et;

    c->CMEventChannel = NULL;

    //Just use the globaly defined one
    c->CMEventChannel = g_SubContext.CMEventChannel;

    //Create the Communication Manager id, conceptualy similar to a socket
    if(0 != rdma_create_id(c->CMEventChannel,&c->pcmid, NULL, RDMA_PS_TCP))
    {
        fprintf(stderr, "Failed to Create CM ID");
        return -1;
    }

    //Resolve our Local IPoIB Address
    if(0 != get_addr(c->subAddrString, c->subPort, &c->subAddr))
    {
        fprintf(stderr, "Failed to Resolve Local Address\n");
        return -1;
    }

    //Resolve REmote IPoIB Addresss
    if(0 != get_addr(c->pubAddrString, c->pubPort, &c->pubAddr))
    {
        fprintf(stderr, "Failed to Resolve Destination Address\n");
        return -1;
    }

    //Print these as a sanity check
    char str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(c->subAddr.sin_addr), str, INET_ADDRSTRLEN);
    fprintf(stdout, "Subscribing to puiblisher at address: %s at port %u\n", c->pubAddrString, c->pubPort);
    c->subAddr.sin_port = htons(c->pubPort);

    //Convert the IPoIB Addresses to Inifniband Native Addresses Called GIDS
    fprintf(stdout, "Resolving IP addresses to GIDS ...\n");
    if(0 != rdma_resolve_addr(c->pcmid, (struct sockaddr*)&c->subAddr, (struct sockaddr*)&c->pubAddr, 2000))
    {
        fprintf(stderr, "CM couldn't resolve IP addresses to GIDS\n");
        return -1;
    }

    fprintf(stdout, "Waiting for CM to resolve IP Addresses ...\n");
    do
    {
        if(0 != GetCMEvent(g_SubContext.CMEventChannel, &et))
        {
            fprintf(stderr, "ERROR Processing CM Events\n");
        }
    } while(et != RDMA_CM_EVENT_ADDR_RESOLVED);

    //Now that we know we can connect to the other node start creating the RDMA Queues we need

    struct ibv_qp_init_attr qp_init_attr;
    //Create a Protection Domain
    c->pd = ibv_alloc_pd(c->pcmid->verbs);
    if(!c->pd)
    {
        fprintf(stderr, "ERROR - RDMACreateQP: Couldn't create protection domain\n");
        return -1;
    }

    // Create a completion Queue
    c->cq = ibv_create_cq(c->pcmid->verbs, 1000, NULL, NULL, 0);
    if(!c->cq)
    {
        fprintf(stderr, "ERROR - RDMACreateQP: Couldn't create completion queue\n");
        return -1;
    }

    // Initialize the QP Attributes
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));

    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.send_cq = c->cq;
    qp_init_attr.recv_cq = c->cq;
    qp_init_attr.cap.max_send_wr = 2;
    qp_init_attr.cap.max_recv_wr = 2;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    //Create the QP on on side
    if(0 != rdma_create_qp(c->pcmid,c->pd, &qp_init_attr))
    {
        fprintf(stderr, "ERROR - RDMACreateQP: Couldn't Create Queue Pair Error(%s)\n", strerror(errno));
        return -1;
    }

    //rdma resolve route
    if(0 != rdma_resolve_route(c->pcmid, RESOLVE_TIMEOUT))
    {
        fprintf(stderr, "ERROR - RDMAClientConnect: Couldn't resolve the Route\n");
        return -1;
    }

    fprintf(stdout, "Waiting for Resolve Route CM Event ...\n");
    do
    {
        if(0 != GetCMEvent(g_SubContext.CMEventChannel, &et))
        {
            fprintf(stderr, "ERROR Processing CM Events\n");
        }
    } while(et != RDMA_CM_EVENT_ROUTE_RESOLVED);

    fprintf(stdout, "Waiting for Connection Established Event ...\n");

    struct rdma_conn_param ConnectionParams;
    memset(&ConnectionParams, 0, sizeof(ConnectionParams));

    if(0 != rdma_connect(c->pcmid, &ConnectionParams))
    {
        fprintf(stderr, "Client Couldn't Establish Connection error(\n");
        return -1;
    }

    fprintf(stderr, "QPN: %d\n", ConnectionParams);

    do
    {
        if(0 != GetCMEvent(g_SubContext.CMEventChannel, &et))
        {
            fprintf(stderr, "ERROR Processing CM Events\n");
        }
    } while(et != RDMA_CM_EVENT_ESTABLISHED);


    return 0;
}

ibv_recv_wr* Create_Receive_WQE(void* buffer, size_t bufferlen, ibv_mr* bufferMemoryRegion)
{
    struct ibv_recv_wr *wqe;
   	struct ibv_sge *sge;

	wqe = (ibv_recv_wr *)malloc(sizeof(ibv_recv_wr));
	sge = (ibv_sge *)malloc(sizeof(ibv_sge));

	memset(wqe, 0, sizeof(ibv_recv_wr));
 	memset(sge, 0, sizeof(ibv_sge));

	wqe->wr_id = 2;
	wqe->next = NULL;
	wqe->sg_list = sge;
	wqe->num_sge = 1;

	sge->addr = (uintptr_t)buffer;
	sge->length = bufferlen;
	sge->lkey = bufferMemoryRegion->lkey;

	return wqe;
}

void PrintUsage()
{
    printf("usage: sub [ -l ip ] [-p local port] publisher-ip publisher-port\n");
    printf("\t[-l ip] - use the local interface associated with this ip\n");
    printf("\t[-p ip] - use this local port \n");
}

int main(int argc,char *argv[], char *envp[])
{
    ConnectionContext_t* conn;
    conn = new ConnectionContext();

    int op;
    while ((op = getopt(argc, argv, "l:p:")) != -1)
    {
        switch (op)
        {
            case 'l':
                conn->subAddrString = optarg;
                break;
            case 'p':
                conn->subPort = atoi(optarg);
                break;
            default:
                PrintUsage();
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
        conn->pubAddrString = argv[optind++];
        conn->pubPort = atoi(argv[optind]);
    }

    //Create on global event channel that we will assign and use for this subscriber.
    g_SubContext.CMEventChannel = rdma_create_event_channel();
    if(!g_SubContext.CMEventChannel)
    {
        fprintf(stderr, "Failed to Open CM Event Channel");
        return -1;
    }



    //Resolve the subscribers via ipoib then create a conntect for them. Finally Connect the QPs.
    // Assumes Subscriber is waiting
    if (0 != subscribe(conn))
    {
        fprintf(stderr, "Failed to Connect to Publisher\n");
        return -1;
    }

    /*
     * Monitor my memory ever second and print the number.
     * The Publisher has a channel and the key to write to my memory. It will change without me doing anything.
     */
    fprintf(stdout, "********  ********  ********  ********\n");
    fprintf(stdout,"Waiting to Receive Messages on Queue Pair (%u)\n", conn->pcmid->qp->qp_num);

    //Register the Memory Regions
    message_t m;
    strcpy (m.message,"Initial number (Nothing Published)");
    m.number = 0;
    conn->mr = createMemoryRegion(conn->pd, &m, sizeof(message_t));

    //Create a Send WQE
    ibv_recv_wr* recvWQE = Create_Receive_WQE(&m, sizeof(message_t), conn->mr);

    /*
  * Post a WQ Element to the Receive Queue for the next message that will come in
  */
    int iter = 0;
    do {
        //Put it on the Receive Queue
        if(0 != ibv_post_recv(conn->pcmid->qp, recvWQE, &conn->bad_wqe))
        {
            fprintf(stderr, "ERROR post_RECEIVE_WQE - Couldn't Post Receive WQE\n");
            free(conn->bad_wqe);
            return -1;
        }

        /*
         * Wait for a new message from the publisher. We know we got a message becasue a completion will be generated.
         */
        //ARM the Completion Channel
        ibv_req_notify_cq(conn->cq, 1);

        fprintf(stderr, "Waiting for a Completion to Be Generated... \n");

        fprintf(stderr, "Waiting for CQE\n");
        int ret;
        struct ibv_wc wc;
        do {
            ret = ibv_poll_cq(conn->cq, 1, &wc);
        } while (ret == 0);
        fprintf(stderr, "DEBUG: Received %d CQE Elements\n", ret);
        fprintf(stderr, "DEBUG: WRID(%i)\tStatus(%i)\n", wc.wr_id, wc.status);

        /*
         * We can now process the message
         */
        printf("Recevied New Message Sequence Number(%f) \n%s\n", m.number, m.message);
        fflush(stdout);
        iter++;
    } while(iter <= NUM_OPERATIONS);

    return 0;
}