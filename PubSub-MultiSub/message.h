//
// Created by alex on 5/25/2021.
//

#ifndef PUBSUB_MESSAGE_H
#define PUBSUB_MESSAGE_H

#define MAX_MSG_SIZE 280 //size of a tweet

typedef struct message
{
    char                            message[MAX_MSG_SIZE];
    double 	                        number;
} message_t;

#endif //PUBSUB_MESSAGE_H
