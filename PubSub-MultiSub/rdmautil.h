#include <infiniband/verbs.h>
#include </usr/include/infiniband/verbs.h>
#include </usr/include/rdma/rdma_cma.h>

#ifndef RDMA_HELP_HPP
#define RDMA_HELP_HPP

bool PollCQ(struct ibv_cq *cq, char *errorMsg);

ibv_mr *createMemoryRegion(struct ibv_pd* pd, void* buffer, size_t bufferlen);

void PrintCMEvent(struct rdma_cm_event *event);

int GetCMEvent(rdma_event_channel *EC, rdma_cm_event_type* EventType);

int get_addr(const char *dst, int port, struct sockaddr_in *addr);

void PrintConnectionInfo(rdma_conn_param cParam);

#endif