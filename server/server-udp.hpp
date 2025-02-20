#pragma once
#include <iostream>
#include <functional>
#include <cstring>
#include <thread>
#include <atomic>
#include <condition_variable>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include "datapacket.hpp"
#include "safedequeue.hpp"
#include "safevector.hpp"
#include "safemap.hpp"
#include "utils.hpp"
#include "create_socket_exception.hpp"
#include "recv_data_exception.hpp"
#include "send_data_exception.hpp"
#include "broken_pipe_exception.hpp"

#define PORT 12345
#define BUFFER_SIZE 1024

/**
 * 
 * 
 * 
 */

using namespace std;

class ServerUDP{
public:
    ServerUDP();

    void add_to_message_queue(const string& data);

    ~ServerUDP();

private:
    int sockfd;
    struct sockaddr_in server_addr, client_addr;
    char buffer[BUFFER_SIZE];
    socklen_t addr_len = sizeof(client_addr);

    bool client_address_available;
    bool stop_condition;
    int sequence;
    int ordered_window_size = 10;
    int limit = 210000000*ordered_window_size;
    atomic<int> packet_failure;
    int ms_send_interval;
    int ms_timeout_interval;
    TSMap<int,string> sent_messages;
    TSMap<int,string> messages_to_print;
    TSDeQueue<int> recv_ack_queue;
    TSDeQueue<string> messages_to_send;
    TSVector<thread> workers;
    TSVector<thread> t_workers;
    TSVector<tuple<string,function<void()>>> tasks;
    TSVector<tuple<string,function<void()>>> t_tasks;
    mutex task_queue_mutex;
    MessageType TYPE_MSG = MSG;
    MessageType TYPE_ACK = ACK;
    datapacket dp;

    condition_variable cv_received_message;
    mutex mtx_received_message;

    condition_variable cv_acknoledge_handling;
    mutex mtx_acknoledge_handling;

    condition_variable cv_threadWiper;
    mutex mtx_threadWiper;

    mutex t_worker_mutex;

    int timer(int s);
    void connection_status_monitor();
    void initialize();
    void main_loop();
    void deinitialize();
    void message_handler_loop();
    void fetch_and_send_loop();
    void acknoledge_handling_loop();
    void received_message_loop();
    void task_launcher(TSVector<tuple<string,function<void()>>> & t);
    void threadWiper();
};


int main() {

    ServerUDP r;
    r.add_to_message_queue("DOG");
    r.add_to_message_queue("CAT");
    r.add_to_message_queue("ELEPHANT");
    r.add_to_message_queue("HORSE");
    r.add_to_message_queue("CAMEL");
    r.add_to_message_queue("TURTLE");
    r.add_to_message_queue("DOG");
    r.add_to_message_queue("CAT");
    r.add_to_message_queue("ELEPHANT");
    r.add_to_message_queue("HORSE");
    r.add_to_message_queue("CAMEL");
    r.add_to_message_queue("TURTLE");
    r.add_to_message_queue("DOG");
    r.add_to_message_queue("CAT");
    r.add_to_message_queue("ELEPHANT");
    r.add_to_message_queue("HORSE");
    r.add_to_message_queue("CAMEL");
    r.add_to_message_queue("TURTLE");
    r.add_to_message_queue("DOG");
    r.add_to_message_queue("CAT");
    r.add_to_message_queue("ELEPHANT");
    r.add_to_message_queue("HORSE");
    r.add_to_message_queue("CAMEL");
    r.add_to_message_queue("TURTLE");


    return 0; 
}
