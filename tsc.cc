#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "client.h"

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::ID;

void sig_ignore(int sig) {
  std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}


class Client : public IClient
{
public:
  Client(const std::string& coord_ip,
         const std::string& coord_port,
         const std::string& user_id,
	 const std::string& uname)
    :coordinatorIP(coord_ip), coordinatorPort(coord_port),
     userId(user_id), username(uname) {}


protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void processTimeline();

private:
  // Coordinator connection info
  std::string coordinatorIP;
  std::string coordinatorPort;
  std::string userId;

  // Server connection info (filled in after calling coordinator)
  std::string hostname;
  std::string username;
  std::string port;

  // Stub for coordinator service
  std::unique_ptr<CoordService::Stub> coordinator_stub_;

  // Stub for SNS service (server)
  std::unique_ptr<SNSService::Stub> stub_;

  // Helper function to get server info from coordinator
  bool getServerFromCoordinator();

  IReply Login();
  IReply List();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void   Timeline(const std::string &username);
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo()
{
  // ------------------------------------------------------------
  // In this function, you are supposed to create a stub so that
  // you call service methods in the processCommand/porcessTimeline
  // functions. That is, the stub should be accessible when you want
  // to call any service methods in those functions.
  // Please refer to gRpc tutorial how to create a stub.
  // ------------------------------------------------------------

  // Step 1: Contact coordinator to get server assignment
  LOG(INFO) << "Contacting coordinator to get server assignment...";
  if (!getServerFromCoordinator()) {
      std::cout << "Failed to get server assignment from coordinator" << std::endl;
      return -1;
  }

  // Step 2: Create server address string using assigned hostname and port
  std::string server_address = hostname + ":" + port;
  LOG(INFO) << "Assigned server address: " << server_address;

  // Step 3: Create a channel (connection) to the assigned server
  stub_ = SNSService::NewStub(
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials())
  );

  // Step 4: Try to login the user
  IReply login_reply = Login();

  // Step 5: Check if login was successful
  if (login_reply.comm_status == SUCCESS) {
      std::cout << "Connected to server at " << server_address << std::endl;
      return 1;  // Success
  } else {
      std::cout << "Failed to connect/login to server" << std::endl;
      return -1; // Failed
  }
}

IReply Client::processCommand(std::string& input)
{
  // ------------------------------------------------------------
  // GUIDE 1:
  // In this function, you are supposed to parse the given input
  // command and create your own message so that you call an 
  // appropriate service method. The input command will be one
  // of the followings:
  //
  // FOLLOW <username>
  // UNFOLLOW <username>
  // LIST
  // TIMELINE
  // ------------------------------------------------------------
  
  // ------------------------------------------------------------
  // GUIDE 2:
  // Then, you should create a variable of IReply structure
  // provided by the client.h and initialize it according to
  // the result. Finally you can finish this function by returning
  // the IReply.
  // ------------------------------------------------------------
  
  
  // ------------------------------------------------------------
  // HINT: How to set the IReply?
  // Suppose you have "FOLLOW" service method for FOLLOW command,
  // IReply can be set as follow:
  // 
  //     // some codes for creating/initializing parameters for
  //     // service method
  //     IReply ire;
  //     grpc::Status status = stub_->FOLLOW(&context, /* some parameters */);
  //     ire.grpc_status = status;
  //     if (status.ok()) {
  //         ire.comm_status = SUCCESS;
  //     } else {
  //         ire.comm_status = FAILURE_NOT_EXISTS;
  //     }
  //      
  //      return ire;
  // 
  // IMPORTANT: 
  // For the command "LIST", you should set both "all_users" and 
  // "following_users" member variable of IReply.
  // ------------------------------------------------------------

    IReply ire;

    // Parse the input command
    std::string cmd;
    std::string arg;

    // Find the first space to split command and argument
    std::size_t space_pos = input.find(' ');
    if (space_pos != std::string::npos) {
        // Command with argument (FOLLOW <username>, UNFOLLOW <username>)
        cmd = input.substr(0, space_pos);
        arg = input.substr(space_pos + 1);
    } else {
        // Command without argument (LIST, TIMELINE)
        cmd = input;
        arg = "";
    }

    // Route to appropriate function based on command
    if (cmd == "FOLLOW") {
        ire = Follow(arg);
    } else if (cmd == "UNFOLLOW") {
        ire = UnFollow(arg);
    } else if (cmd == "LIST") {
        ire = List();
    } else if (cmd == "TIMELINE") {
        ire = List(); // Just return something for now, Timeline is handled differently
        ire.comm_status = SUCCESS; // Timeline doesn't return here, it's handled in run()
    } else {
        // Invalid command
        ire.comm_status = FAILURE_INVALID;
        std::cout << "Invalid command: " << cmd << std::endl;
    }

    return ire;
}


void Client::processTimeline()
{
    Timeline(username);
}

// Helper function to get server assignment from coordinator
bool Client::getServerFromCoordinator() {
    // Step 1: Create coordinator stub if not already created
    if (!coordinator_stub_) {
        std::string coordinator_address = coordinatorIP + ":" + coordinatorPort;
        coordinator_stub_ = CoordService::NewStub(
            grpc::CreateChannel(coordinator_address, grpc::InsecureChannelCredentials())
        );
        LOG(INFO) << "Created coordinator stub for " << coordinator_address;
    }

    // Step 2: Prepare the GetServer request with userId
    ID id_request;
    id_request.set_id(std::stoi(userId));

    // Step 3: Create containers for response and context
    ServerInfo server_info;
    ClientContext context;

    // Step 4: Call GetServer RPC
    Status status = coordinator_stub_->GetServer(&context, id_request, &server_info);

    // Step 5: Process the response
    if (status.ok()) {
        // Extract server details from response
        hostname = server_info.hostname();
        port = server_info.port();

        LOG(INFO) << "Coordinator assigned server - Hostname: " << hostname
                  << ", Port: " << port;
        return true;
    } else {
        LOG(ERROR) << "Failed to get server from coordinator: "
                   << status.error_message();
        return false;
    }
}

// List Command
IReply Client::List() {

    IReply ire;

    // Step 1: Create the request
    Request request;
    request.set_username(username);

    // Step 2: Create reply container and context
    ListReply list_reply;
    ClientContext context;

    // Step 3: Call the server's List method
    Status status = stub_->List(&context, request, &list_reply);

    // Step 4: Set the gRPC status
    ire.grpc_status = status;

    // Step 5: Process the response
    if (status.ok()) {
        ire.comm_status = SUCCESS;

        // Copy all users from list_reply to ire
        for (const std::string& u : list_reply.all_users()) {
            ire.all_users.push_back(u);
        }

        // Copy all followers from list_reply to ire
        for (const std::string& f : list_reply.followers()) {
            ire.followers.push_back(f);
        }

        // Build formatted strings for output
        std::string all_users_str = "All users: ";
        for (size_t i = 0; i < ire.all_users.size(); ++i) {
            all_users_str += ire.all_users[i];
            if (i < ire.all_users.size() - 1) all_users_str += ", ";
        }

        std::string followers_str = "Followers: ";
        for (size_t i = 0; i < ire.followers.size(); ++i) {
            followers_str += ire.followers[i];
            if (i < ire.followers.size() - 1) followers_str += ", ";
        }

        // Output using glog instead of std::cout
        LOG(INFO) << all_users_str;
        LOG(INFO) << followers_str;

    } else {
        ire.comm_status = FAILURE_UNKNOWN;
        LOG(INFO) << "Command failed: ";
    }

    return ire;
}

// Follow Command
IReply Client::Follow(const std::string& username2) {

    IReply ire;

    // Step 1: Check if trying to follow yourself
    if (username == username2) {
        ire.comm_status = FAILURE_ALREADY_EXISTS;
        ire.grpc_status = Status::OK;  // No network error
        LOG(INFO) << "User is trying to follow themselves";
        return ire;
    }

    // Step 2: Create the request
    Request request;
    request.set_username(username);           // Current user
    request.add_arguments(username2);         // User to follow

    // Step 3: Create reply container and context
    Reply reply;
    ClientContext context;

    // Step 4: Call the server's Follow method
    Status status = stub_->Follow(&context, request, &reply);

    // Step 4: Set the gRPC status
    ire.grpc_status = status;

    // Step 5: Process the response based on server message
    if (status.ok()) {
        if (reply.msg() == "SUCCESS") {
            ire.comm_status = SUCCESS;
            LOG(INFO) << "Follow successful";
        } else if (reply.msg() == "FAILURE_ALREADY_EXISTS") {
            ire.comm_status = FAILURE_ALREADY_EXISTS;
            LOG(INFO) << "You have already joined";
        } else if (reply.msg() == "FAILURE_NOT_EXISTS") {
            ire.comm_status = FAILURE_NOT_EXISTS;
            LOG(INFO) << "Command failed with invalid username";
        } else if (reply.msg() == "FAILURE_INVALID_USERNAME") {
            ire.comm_status = FAILURE_INVALID_USERNAME;
            LOG(INFO) << "Command failed with invalid username";
        } else {
            ire.comm_status = FAILURE_UNKNOWN;
            LOG(INFO) << "Follow command failed: Unknown error";
        }
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
        LOG(ERROR) << "Follow command failed: " << status.error_message();
    }

    return ire;
}

// UNFollow Command
IReply Client::UnFollow(const std::string& username2) {

    IReply ire;

    // Check if trying to unfollow yourself
    if (username == username2) {
        ire.comm_status = FAILURE_INVALID_USERNAME;
        ire.grpc_status = Status::OK;  // No network error
        LOG(INFO) << "UnFollow result: Cannot unfollow yourself";
        return ire;
    }

    // Create the request
    Request request;
    request.set_username(username);           // Current user
    request.add_arguments(username2);         // User to unfollow

    // Create reply container and context
    Reply reply;
    ClientContext context;

    // Call the server's UnFollow method
    Status status = stub_->UnFollow(&context, request, &reply);

    // Set the gRPC status
    ire.grpc_status = status;

    if (status.ok()) {
        if (reply.msg() == "SUCCESS") {
            ire.comm_status = SUCCESS;
        } else if (reply.msg() == "FAILURE_NOT_A_FOLLOWER") {
            ire.comm_status = FAILURE_NOT_A_FOLLOWER;
        } else if (reply.msg() == "FAILURE_NOT_EXISTS") {
            ire.comm_status = FAILURE_NOT_EXISTS;
        } else if (reply.msg() == "FAILURE_INVALID_USERNAME") {
            ire.comm_status = FAILURE_INVALID_USERNAME;
        } else {
            ire.comm_status = FAILURE_UNKNOWN;
        }
        LOG(INFO) << "UnFollow result: " << reply.msg();
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
        LOG(ERROR) << "UnFollow command failed: " << status.error_message();
    }

    return ire;
}

// Login Command
IReply Client::Login() {

    IReply ire;

    // Step 1: Create the request message
    Request request;
    request.set_username(username);

    // Step 2: Create reply container and context
    Reply reply;
    ClientContext context;

    // Step 3: Call the server's Login method
    Status status = stub_->Login(&context, request, &reply);

    // Step 4: Set the gRPC status
    ire.grpc_status = status;

    // Step 5: Use the reply message to set comm_status
    if (status.ok()) {
        if (reply.msg() == "SUCCESS") {
            ire.comm_status = SUCCESS;
        } else if (reply.msg() == "FAILURE_ALREADY_EXISTS") {
            ire.comm_status = FAILURE_ALREADY_EXISTS;
        }
        std::cout << "Login result: " << reply.msg() << std::endl;
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
        std::cout << "Network error: " << status.error_message() << std::endl;
    }

    return ire;
}

// Timeline Command
void Client::Timeline(const std::string& username) {

    // ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions
    // in client.cc file for both getting and displaying messages
    // in timeline mode.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
    // ------------------------------------------------------------

    // Step 1: Create bidirectional stream
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<Message, Message>> stream = stub_->Timeline(&context);

    if (!stream) {
        LOG(ERROR) << "Failed to create timeline stream";
        return;
    }

    // Step 2: Send empty identification message first to get historical timeline
    Message identification_msg = MakeMessage(username, "");  // Empty message for identification
    if (!stream->Write(identification_msg)) {
        LOG(ERROR) << "Failed to send identification message";
        return;
    }

    LOG(INFO) << "Entering Timeline mode. Type messages to post (Ctrl+C to exit)";

    // Step 3: Create background thread for receiving messages
    std::thread receiver([&]() {
        Message server_message;
        while (stream->Read(&server_message)) {
            // Display incoming messages from server (other users' posts)
            std::time_t msg_time = server_message.timestamp().seconds();
            displayPostMessage(server_message.username(),
                             server_message.msg(),
                             msg_time);
        }
    });

    // Step 4: Main thread handles user input and sending posts
    while (true) {
        std::string post_message = getPostMessage();

        // Check for exit condition (empty message or stream issues)
        if (post_message.empty()) {
            break;
        }

        // Create message with timestamp
        Message msg = MakeMessage(username, post_message);

        // Send message to server
        if (!stream->Write(msg)) {
            LOG(ERROR) << "Failed to send message to server";
            break;  // Stream closed or error
        }
    }

    // Step 4: Cleanup
    stream->WritesDone();  // Signal no more writes
    receiver.join();       // Wait for receiver thread to finish

    Status status = stream->Finish();
    if (!status.ok()) {
        LOG(INFO) << "Command failed";
    } else {
        LOG(INFO) << "Timeline session ended";
    }
}



//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string coordinatorIP = "localhost";
  std::string coordinatorPort = "9090";
  std::string userId = "1";

  int opt = 0;
  while ((opt = getopt(argc, argv, "h:k:u:")) != -1){
    switch(opt) {
    case 'h':
      coordinatorIP = optarg;break;
    case 'k':
      coordinatorPort = optarg;break;
    case 'u':
      userId = optarg;break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }

  // Initialize Google Logging
  std::string log_file_name = std::string("client-") + userId;
  google::InitGoogleLogging(log_file_name.c_str());
  FLAGS_logtostderr = true;  // Output logs to terminal instead of files
  std::cout << "Logging Initialized. Client starting..." << std::endl;

  // Derive username from userId (e.g., userId "1" becomes "user1")
  std::string username = userId;

  Client myc(coordinatorIP, coordinatorPort, userId, username);
  
  myc.run();
  
  return 0;
}
