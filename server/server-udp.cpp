#include "server-udp.hpp"

ServerUDP::ServerUDP():sequence(0),packet_failure(0),ms_send_interval(2),ms_timeout_interval(ms_send_interval*3),stop_condition(false),client_address_available(false){
    initialize();
    main_loop();
}

ServerUDP::~ServerUDP(){
    deinitialize();
}

/**
 *  The following method add a message to send to a message outgoing queue
 * 
 */

void ServerUDP::add_to_message_queue(const string& data){
    messages_to_send.push(data);
}

/**
 *  The following method initialize a datagram socket
 *
 */

void ServerUDP::initialize(){
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        throw create_socket_exception();
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(sockfd, (const struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        cerr << "Error socket binding." << endl;
        close(sockfd);
    }
}

/**
 *  The following method create some task involved in the communication and lauch it associating it to a worker
 * 
 */

void ServerUDP::main_loop(){
    
    function<void()> task_message_handler_loop = [this]() {
        this->message_handler_loop();
    };
    function<void()> task_fetch_and_send_loop = [this](){
        this->fetch_and_send_loop();
    };
    function<void()> task_acknoledge_handling_loop = [this]() {
        this->acknoledge_handling_loop();
    };
    function<void()> task_received_message_loop = [this]() {
        this->received_message_loop();
    };
    function<void()> task_connection_status_monitor = [this]() {
        this->connection_status_monitor();
    };
    function<void()> task_threadWiper = [this]() {
        this->threadWiper();
    };

    tasks.push_back(tuple(string("udp-Smessage"),move(task_message_handler_loop)));
    tasks.push_back(tuple(string("udp-Sfetch"),move(task_fetch_and_send_loop)));
    tasks.push_back(tuple(string("udp-Sacknoledge"),move(task_acknoledge_handling_loop)));
    tasks.push_back(tuple(string("udp-Sreceived"),move(task_received_message_loop)));
    tasks.push_back(tuple(string("udp-SconnectionMonitor"),move(task_connection_status_monitor)));
    tasks.push_back(tuple(string("udp-SthreadWiper"),move(task_threadWiper)));

    while(tasks.size()>0){
        workers.push_back(thread([this](TSVector<tuple<string,function<void()>>> & tasks){this->task_launcher(tasks);},ref(tasks)));
    }
}

/**
 *  The following method check, join and delete all the ended timer threads 
 * 
 */

void ServerUDP::threadWiper(){
    while(true){

        {
            unique_lock<mutex> threadWiper_lock(mtx_threadWiper);
            cv_threadWiper.wait(threadWiper_lock, [this](){ 
                return !t_workers.empty();
            });
        }

        {
            lock_guard<mutex> lock(t_worker_mutex);
            for (auto it = t_workers.begin(); it != t_workers.end(); ) {
                if (it->joinable()) {
                    it->join();
                    it = t_workers.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }
}

/**
 *  The following method ensure the proper termination of all the tasks launched
 * 
 */

void ServerUDP::deinitialize(){
    for(thread& w : workers){
        if(w.joinable()){
            w.join();
        }
    }
    close(sockfd);
}

/**
 *  The following function runs as a task launched by the main loop.
 *  This task gets incoming messages and process it withrespect their type.
 */

void ServerUDP::message_handler_loop(){
    while(!stop_condition){
        memset(buffer, 0, BUFFER_SIZE);
        int n = recvfrom(sockfd, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&client_addr, &addr_len);
        // cout << "Messagge received from -> " << inet_ntoa(client_addr.sin_addr) << " : " << ntohs(client_addr.sin_port) << endl;
        
        if (n < 0) {
            close(sockfd);
            throw recv_data_exception();
        }else if (n >= BUFFER_SIZE) {
            cerr << "message overflows the buffer." << endl;
            continue; 
        }else{
            client_address_available = true;
        }

        buffer[n] = '\0';
        string s(buffer);
        vector<string> pack = dp.unpack(s);

        switch (stoi(pack[0])) {
            case MSG:
                // cout<<"Message type MSG."<<endl;
                messages_to_print.insert(stoi(pack[1]),pack[2]);
                sendto(sockfd, dp.pack(TYPE_ACK,stoi(pack[1]),"ACK_MESSAGE").c_str(), strlen(dp.pack(TYPE_ACK,stoi(pack[1]),"ACK_MESSAGE").c_str()), 0, (const struct sockaddr *)&client_addr, addr_len);
                cv_received_message.notify_all();
                break;
            case ACK:
                // cout<<"Message type ACK." << endl;
                recv_ack_queue.push(stoi(pack[1]));
                cv_acknoledge_handling.notify_all();
                break;
            default:
                cout<<"Message type unknown."<<endl;
        }
    }
}

/**
 * The following function runs as a task launched by the main loop. 
 * It packs and send messages and initialize a timer fot each data sent. 
 * 
 */

void ServerUDP::fetch_and_send_loop(){
    while (!client_address_available){
        this_thread::sleep_for(chrono::milliseconds(ms_timeout_interval*2));
    }
    string pack;
    string data;
    while(!stop_condition){
        this_thread::sleep_for(chrono::milliseconds(ms_send_interval));
        
        if(!messages_to_send.empty()){
            data = messages_to_send.front();
            cout << "Sending: "<< data << " with   sequence number:" << sequence << endl;
            pack = dp.pack(TYPE_MSG,sequence,data);
            messages_to_send.pop();
        }else{
            data = "ALIVE";
            cout << "Sending: "<< data << " with   sequence number:" << sequence << endl;
            pack = dp.pack(TYPE_MSG,sequence,data);
        }
        
        int n = sendto(sockfd, pack.c_str(), strlen(pack.c_str()), 0, (const struct sockaddr *)&client_addr, addr_len);
        if (n < 0) {
            close(sockfd);
            throw send_data_exception();
        }

        sent_messages.insert(sequence,data);

        t_tasks.push_back(tuple(string("udp-timer"),move([this](){
            bool stop = false;
            int retry = 0;
            while(!stop){
                int sec = timer(sequence);
                
                if(sent_messages.find(sec)){
                    retry++;
                    cout << "Timeout for " << sec << " resend .. attempt num:("<<retry<<")." << endl;
                    string pack;
                    try {
                        pack = dp.pack(TYPE_MSG,sec,sent_messages.get(sec));
                    } catch (const std::out_of_range& e) {
                        cout << sec << " properly received."; 
                        continue;
                    }
                    sendto(sockfd, pack.c_str(), strlen(pack.c_str()), 0, (const struct sockaddr *)&client_addr, addr_len);
                }else{
                    stop = true;
                }
                
                if(retry > 10){
                    cout << "ERROR: Packet "<<sec<<" lost."<< endl;
                    packet_failure.fetch_add(1, std::memory_order_seq_cst);
                    stop = true;
                }

            }
        })));

        {
            lock_guard<mutex> lock(t_worker_mutex);
            t_workers.push_back(thread([this](TSVector<tuple<string,function<void()>>> & t_tasks){this->task_launcher(t_tasks);},ref(t_tasks)));
        }

  
        sequence = static_cast<int>((sequence+1) % limit);
    }
}

/**
 *  The following function runs as a task launched by the main loop.
 *  The following function handle the received ack
 * 
 */

void ServerUDP::acknoledge_handling_loop(){
    while(!stop_condition){

        {
            unique_lock<mutex> acknoledge_handling_loop_lock(mtx_acknoledge_handling);
            cv_acknoledge_handling.wait(acknoledge_handling_loop_lock, [this](){ 
                return !recv_ack_queue.empty();
            });
        }

        while(!recv_ack_queue.empty()){
            if(sent_messages.find(recv_ack_queue.front())){
                sent_messages.erase(recv_ack_queue.front());
                // cout<< "ACK " << recv_ack_queue.front() << " received, message sent successfully."<<endl;
                recv_ack_queue.pop();
            }else{
                cout << "ACK duplicated, data already received: " << recv_ack_queue.front() << endl;
                recv_ack_queue.pop();
            }
        }
    }
}

/**
 *  The following function runs as a task launched by the main loop.
 *  The following function checks periodically messages failure counter and interrupt the threads if too much packet are lost
 * 
 */

void ServerUDP::connection_status_monitor(){
    while(!stop_condition){
        if(packet_failure.load(std::memory_order_seq_cst) > 0){
            stop_condition = true;
            cv_received_message.notify_all();
            cv_acknoledge_handling.notify_all();
            throw broken_pipe_exception(packet_failure.load(std::memory_order_seq_cst));
        }else{
            cout << "Connection alive!" << endl;
        }
        this_thread::sleep_for(chrono::seconds(10)); 
    }
}

/**
 *  The following function runs as a task launched by the main loop.
 *  The following function handle the received messages
 * 
 */

void ServerUDP::received_message_loop(){
    int message_processed = 0;
    vector<string> ordered_window(ordered_window_size);
    while (!stop_condition) {

        {
            unique_lock<mutex> received_message_loop_lock(mtx_received_message);
            cv_received_message.wait(received_message_loop_lock, [this](){ 
                return !messages_to_print.empty();
            });
        }

        while(messages_to_print.find(message_processed)){
            ordered_window[static_cast<int>(message_processed % ordered_window_size)] = messages_to_print.get(message_processed);
            messages_to_print.erase(message_processed);
                     
            if(static_cast<int>(message_processed % ordered_window_size) == (ordered_window_size-1)){
                cout << "Received messages (ordered) ---------- >>";
                for_each(ordered_window.begin(),ordered_window.end(),[](string i){cout<< i << " ";});
                cout<<endl;
            }
            message_processed = static_cast<int>((message_processed+1) % limit); 
        }
    }
}

/**
 *  The following code is assigned to each task. It fetches an available tasks and executes it.
 * 
 */

void ServerUDP::task_launcher(TSVector<tuple<string,function<void()>>> & t){
    
    while(!t.empty()){
        function<void()> f;
        {
            lock_guard<mutex> lock(task_queue_mutex);
            
            if(t.empty()){
                return;
            }
            tuple<string,function<void()>> entry = t.back();
            pthread_setname_np(pthread_self(), get<0>(entry).c_str());
            f = get<1>(entry);
            t.pop_back();
            //cout << "A New thread is loaded with a new task." << endl;
        }
        f();
    }
    cv_threadWiper.notify_all();
}

/**
 *  The following function counts the timeout for each packet sent.
 * 
 */

int ServerUDP::timer(int s) {
    this_thread::sleep_for(chrono::milliseconds(ms_timeout_interval));
    return s;
}