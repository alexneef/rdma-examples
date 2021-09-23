#include <string>
#include <rdma/rdma_cma.h>
#include <arpa/inet.h>
#include <getopt.h>

#include "rdmautil.h"
#include "message.h"

#define RESOLVE_TIMEOUT 2000

#define NUM_OPERATIONS 10

#define LOCAL_IP_ADD        "10.10.10.215"
#define LOCAL_PORT          20001

#define PUBLISHER_IP_ADD    "10.10.10.220"
#define PUBLISHER_PORT         20080

typedef struct ConnectionContext
{
    struct rdma_event_channel       *CMEventChannel;

    struct sockaddr_in			    pubAddr;
    struct sockaddr_in			    subAddr;

    struct rdma_cm_id             *pcmid;
    struct ibv_pd                 *pd;
    struct ibv_cq                 *cq;
    struct ibv_mr                 *mr;

    struct ibv_recv_wr            *bad_wqe;
} ConnectionContext_t ;

int subscribe(ConnectionContext_t* ctx)
{
    rdma_cm_event_type et;

    //Create an event channel that we will assign and use for this subscriber.
    ctx->CMEventChannel = rdma_create_event_channel();
    if(!ctx->CMEventChannel)
    {
        fprintf(stderr, "Failed to Open CM Event Channel");
        return -1;
    }

    //Create the Communication Manager id, conceptualy similar to a socket
    if(0 != rdma_create_id(ctx->CMEventChannel, &ctx->pcmid, NULL, RDMA_PS_TCP))
    {
        fprintf(stderr, "Failed to Create CM ID");
        return -1;
    }

    //Resolve our Local IPoIB Address
    if(0 != get_addr(LOCAL_IP_ADD, LOCAL_PORT, &ctx->subAddr))
    {
        fprintf(stderr, "Failed to Resolve Local Address\n");
        return -1;
    }

    //Resolve Remote IPoIB Addresss
    if(0 != get_addr(PUBLISHER_IP_ADD, PUBLISHER_PORT, &ctx->pubAddr))
    {
        fprintf(stderr, "Failed to Resolve Destination Address\n");
        return -1;
    }

    fprintf(stderr, "Creating new Subscriber at ");
    print_ipv4(&ctx->subAddr);

    fprintf(stderr, "Subscribing to publisher at ");
    print_ipv4(&ctx->pubAddr);


    //Convert the IPoIB Addresses to Inifniband Native Addresses Called GIDS
    fprintf(stdout, "Resolving IP addresses to GIDS ...\n");
    if(0 != rdma_resolve_addr(ctx->pcmid, (struct sockaddr*)&ctx->subAddr, (struct sockaddr*)&ctx->pubAddr, 2000))
    {
        fprintf(stderr, "CM couldn't resolve IP addresses to GIDS\n");
        return -1;
    }

    fprintf(stdout, "Waiting for CM to resolve IP Addresses ...\n");
    do
    {
        if(0 != GetCMEvent(ctx->CMEventChannel, &et))
        {
            fprintf(stderr, "ERROR Processing CM Events\n");
        }
    } while(et != RDMA_CM_EVENT_ADDR_RESOLVED);

    //Now that we know we can connect to the other node start creating the RDMA Queues we need

    struct ibv_qp_init_attr qp_init_attr;
    //Create a Protection Domain
    ctx->pd = ibv_alloc_pd(ctx->pcmid->verbs);
    if(!ctx->pd)
    {
        fprintf(stderr, "ERROR - RDMACreateQP: Couldn't create protection domain\n");
        return -1;
    }

    // Create a completion Queue
    ctx->cq = ibv_create_cq(ctx->pcmid->verbs, 1000, NULL, NULL, 0);
    if(!ctx->cq)
    {
        fprintf(stderr, "ERROR - RDMACreateQP: Couldn't create completion queue\n");
        return -1;
    }

    // Initialize the QP Attributes
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));

    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.send_cq = ctx->cq;
    qp_init_attr.recv_cq = ctx->cq;
    qp_init_attr.cap.max_send_wr = 2;
    qp_init_attr.cap.max_recv_wr = 2;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    //Create the QP on on side
    if(0 != rdma_create_qp(ctx->pcmid, ctx->pd, &qp_init_attr))
    {
        fprintf(stderr, "ERROR - RDMACreateQP: Couldn't Create Queue Pair Error(%s)\n", strerror(errno));
        return -1;
    }

    //rdma resolve route
    if(0 != rdma_resolve_route(ctx->pcmid, RESOLVE_TIMEOUT))
    {
        fprintf(stderr, "ERROR - RDMAClientConnect: Couldn't resolve the Route\n");
        return -1;
    }

    fprintf(stdout, "Waiting for Resolve Route CM Event ...\n");
    do
    {
        if(0 != GetCMEvent(ctx->CMEventChannel, &et))
        {
            fprintf(stderr, "ERROR Processing CM Events\n");
        }
    } while(et != RDMA_CM_EVENT_ROUTE_RESOLVED);

    fprintf(stdout, "Waiting for Connection Established Event ...\n");

    struct rdma_conn_param ConnectionParams;
    memset(&ConnectionParams, 0, sizeof(ConnectionParams));

    if(0 != rdma_connect(ctx->pcmid, &ConnectionParams))
    {
        fprintf(stderr, "Client Couldn't Establish Connection error(\n");
        return -1;
    }

    fprintf(stderr, "QPN: %d\n", ConnectionParams.qp_num);

    do
    {
        if(0 != GetCMEvent(ctx->CMEventChannel, &et))
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

int main(int argc,char *argv[], char *envp[])
{
    fprintf(stdout, "********  ********  ********  ********\n");
    fprintf(stdout,"Subscriber - Processor \n");
    fprintf(stdout,"Uses an UC Channel receive messages from the publisher\n");
    fprintf(stdout,"\n");
    fprintf(stdout, "********  ********  ********  ********\n\n");

    ConnectionContext_t* ctx;
    ctx = new ConnectionContext();

    //Resolve the subscribers via ipoib then create a conntect for them. Finally Connect the QPs.
    // Assumes Publisher is waiting
    if (0 != subscribe(ctx))
    {
        fprintf(stderr, "Failed to Connect to Publisher\n");
        return -1;
    }

    /*
     * Monitor my memory ever second and print the number.
     * The Publisher has a channel and the key to write to my memory. It will change without me doing anything.
     */
    fprintf(stdout, "********  ********  ********  ********\n");
    fprintf(stdout, "Waiting to Receive Messages on Queue Pair (%u)\n", ctx->pcmid->qp->qp_num);

    //Register the Memory Regions
    message_t m;
    strcpy (m.message,"Initial number (Nothing Published)");
    m.number = 0;
    ctx->mr = createMemoryRegion(ctx->pd, &m, sizeof(message_t));

    //Create a Send WQE
    ibv_recv_wr* recvWQE = Create_Receive_WQE(&m, sizeof(message_t), ctx->mr);

    /*
  * Post a WQ Element to the Receive Queue for the next message that will come in
  */
    int iter = 0;
    do {
        //Put it on the Receive Queue
        if(0 != ibv_post_recv(ctx->pcmid->qp, recvWQE, &ctx->bad_wqe))
        {
            fprintf(stderr, "ERROR post_RECEIVE_WQE - Couldn't Post Receive WQE\n");
            free(ctx->bad_wqe);
            return -1;
        }

        /*
         * Wait for a new message from the publisher. We know we got a message becasue a completion will be generated.
         */
        //ARM the Completion Channel
        ibv_req_notify_cq(ctx->cq, 1);

        fprintf(stderr, "Waiting for a Completion to Be Generated... \n");

        fprintf(stderr, "Waiting for CQE\n");
        int ret;
        struct ibv_wc wc;
        do {
            ret = ibv_poll_cq(ctx->cq, 1, &wc);
        } while (ret == 0);
        fprintf(stderr, "DEBUG: Received %d CQE Elements\n", ret);
        fprintf(stderr, "DEBUG: WRID(%li)\tStatus(%i)\n", wc.wr_id, wc.status);

        /*
         * We can now process the message
         */
        printf("Recevied New Message Sequence Number(%f) \n%s\n", m.number, m.message);
        fflush(stdout);
        iter++;
    } while(iter <= NUM_OPERATIONS);

    return 0;
}