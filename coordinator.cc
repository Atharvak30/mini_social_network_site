#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

#define log(severity, msg) LOG(severity) << msg; \
google::FlushLogFiles(google::severity);

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;
using csce438::SynchService;

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isMaster;  // Is this server currently acting as Master?
    int consecutive_missed_heartbeats;  // Track consecutive misses
    bool isActive();

};

//potentially thread safe
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};

// Vector to store synchronizers separately from tsd servers
std::vector<zNode*> synchronizers;


//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}


class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        // Extract server information from the request
        int serverID = serverinfo->serverid();
        std::string hostname = serverinfo->hostname();
        std::string port = serverinfo->port();
        std::string type = serverinfo->type();

        // Extract clusterID from gRPC context metadata
        int clusterID = -1;
        auto metadata = context->client_metadata();
        auto iter = metadata.find("clusterid");

        if(iter != metadata.end()){
            std::string clusterid_str(iter->second.data(), iter->second.size());
            clusterID = std::stoi(clusterid_str);
        } else {
            log(ERROR, "Heartbeat received without clusterID metadata");
            confirmation->set_status(false);
            return Status::OK;
        }

        // Validate clusterID (should be 1, 2, or 3)
        if(clusterID < 1 || clusterID > 3){
            log(ERROR, "Invalid clusterID: " << clusterID);
            confirmation->set_status(false);
            return Status::OK;
        }

        // Lock the mutex for thread safety
        v_mutex.lock();

        // Route based on type: "synchronizer" vs "server"
        if (type == "synchronizer") {
            // SYNCHRONIZER REGISTRATION/HEARTBEAT
            int syncIndex = findServer(synchronizers, serverID);

            if (syncIndex == -1) {
                // SCENARIO 1: REGISTRATION (First time from this synchronizer)
                zNode* newSync = new zNode();
                newSync->serverID = serverID;
                newSync->hostname = hostname;
                newSync->port = port;
                newSync->type = type;
                newSync->last_heartbeat = getTimeNow();
                newSync->missed_heartbeat = false;
                newSync->consecutive_missed_heartbeats = 0;

                // Determine if this synchronizer should be Master or Slave
                // Check if there's already an active Master synchronizer for this cluster
                bool hasMaster = false;
                for (auto* sync : synchronizers) {
                    // Check if there's already a Master synchronizer for this clusterID
                    if (sync->isMaster && sync->isActive() &&
                        sync->consecutive_missed_heartbeats < 2 &&
                        ((sync->serverID - 1) % 3 + 1) == clusterID) {  // Calculate which cluster this sync belongs to
                        hasMaster = true;
                        break;
                    }
                }

                // If no active Master exists for this cluster, this synchronizer becomes Master
                newSync->isMaster = !hasMaster;

                // Add to synchronizers vector
                synchronizers.push_back(newSync);

                std::string role = newSync->isMaster ? "Master" : "Slave";

                v_mutex.unlock();

                log(INFO, "Synchronizer registered - ClusterID: " << clusterID
                          << ", SyncID: " << serverID
                          << ", Role: " << role
                          << ", Hostname: " << hostname
                          << ", Port: " << port);

                // Return confirmation with role
                confirmation->set_status(true);
                confirmation->set_ismaster(newSync->isMaster);

            } else {
                // SCENARIO 2: REGULAR HEARTBEAT (Keep-alive from existing synchronizer)
                zNode* existingSync = synchronizers[syncIndex];
                existingSync->last_heartbeat = getTimeNow();
                existingSync->missed_heartbeat = false;
                existingSync->consecutive_missed_heartbeats = 0;  // Reset consecutive misses

                v_mutex.unlock();

                log(INFO, "Heartbeat received - Synchronizer ClusterID: " << clusterID
                          << ", SyncID: " << serverID
                          << ", Role: " << (existingSync->isMaster ? "Master" : "Slave"));

                // Return confirmation with current role
                confirmation->set_status(true);
                confirmation->set_ismaster(existingSync->isMaster);
            }

        } else {
            // TSD SERVER REGISTRATION/HEARTBEAT
            // Check if server already exists in the cluster
            int serverIndex = findServer(clusters[clusterID - 1], serverID);

            if(serverIndex == -1){
                // SCENARIO 1: REGISTRATION HEARTBEAT (First time from this server)
                zNode* newServer = new zNode();
                newServer->serverID = serverID;
                newServer->hostname = hostname;
                newServer->port = port;
                newServer->type = type;
                newServer->last_heartbeat = getTimeNow();
                newServer->missed_heartbeat = false;
                newServer->consecutive_missed_heartbeats = 0;

                // Determine if this server should be Master or Slave
                // Check if there's already an active Master in this cluster
                bool hasMaster = false;
                for (auto* server : clusters[clusterID - 1]) {
                    if (server->isMaster && server->isActive() && server->consecutive_missed_heartbeats < 2) {
                        hasMaster = true;
                        break;
                    }
                }

                // If no active Master exists, this server becomes Master
                newServer->isMaster = !hasMaster;

                // Add to the appropriate cluster
                clusters[clusterID - 1].push_back(newServer);

                std::string role = newServer->isMaster ? "Master" : "Slave";

                v_mutex.unlock();

                log(INFO, "Server registered - ClusterID: " << clusterID
                          << ", ServerID: " << serverID
                          << ", Role: " << role
                          << ", Hostname: " << hostname
                          << ", Port: " << port
                          << ", Type: " << type);

                // Return confirmation with role
                confirmation->set_status(true);
                confirmation->set_ismaster(newServer->isMaster);

            } else {
                // SCENARIO 2: REGULAR HEARTBEAT (Keep-alive from existing server)
                zNode* existingServer = clusters[clusterID - 1][serverIndex];
                existingServer->last_heartbeat = getTimeNow();
                existingServer->missed_heartbeat = false;
                existingServer->consecutive_missed_heartbeats = 0;  // Reset consecutive misses

                v_mutex.unlock();

                log(INFO, "Heartbeat received - ClusterID: " << clusterID
                          << ", ServerID: " << serverID
                          << ", Role: " << (existingServer->isMaster ? "Master" : "Slave"));

                // Return confirmation with current role
                confirmation->set_status(true);
                confirmation->set_ismaster(existingServer->isMaster);
            }
        }

        return Status::OK;
    }

    //function returns the MASTER server information for requested client id
    //this function assumes there are always 3 clusters and has math
    //hardcoded to represent this.
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        //Extract client_id from the request
        int client_id = id->id();

        // Calculate cluster assignment using the formula
        // clusterID = ((client_id - 1) % 3) + 1
        int clusterID = ((client_id - 1) % 3) + 1;

        log(INFO, "Client " << client_id << " requesting server assignment. Calculated ClusterID: " << clusterID);

        v_mutex.lock();

        // Check if cluster has any servers
        if(clusters[clusterID - 1].empty()){
            v_mutex.unlock();
            log(ERROR, "No servers available in Cluster " << clusterID);
            return Status(grpc::StatusCode::UNAVAILABLE, "No servers available in cluster");
        }

        // Find the Master server in the cluster
        zNode* masterServer = nullptr;
        for (auto* server : clusters[clusterID - 1]) {
            if (server->isMaster && server->isActive()) {
                masterServer = server;
                break;
            }
        }

        // Check if Master was found
        if (masterServer == nullptr) {
            v_mutex.unlock();
            log(ERROR, "No active Master server in Cluster " << clusterID);
            return Status(grpc::StatusCode::UNAVAILABLE, "No active Master server in cluster");
        }

        // Fill ServerInfo with the Master server details
        serverinfo->set_serverid(masterServer->serverID);
        serverinfo->set_hostname(masterServer->hostname);
        serverinfo->set_port(masterServer->port);
        serverinfo->set_type(masterServer->type);
        serverinfo->set_clusterid(clusterID);

        v_mutex.unlock();

        log(INFO, "Client " << client_id << " assigned to Master Server " << masterServer->serverID
                  << " in Cluster " << clusterID
                  << " (Hostname: " << masterServer->hostname
                  << ", Port: " << masterServer->port << ")");

        return Status::OK;
    }

    // function returns the SLAVE server information for a given cluster
    // Master servers use this to find their Slave for mirroring
    Status GetSlave(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        int clusterID = id->id();

        log(INFO, "GetSlave request for ClusterID: " << clusterID);

        v_mutex.lock();

        // Check if cluster has any servers
        if(clusters[clusterID - 1].empty()){
            v_mutex.unlock();
            log(ERROR, "No servers available in Cluster " << clusterID);
            return Status(grpc::StatusCode::UNAVAILABLE, "No servers available in cluster");
        }

        // Find the Slave server in the cluster (server that is NOT Master)
        zNode* slaveServer = nullptr;
        for (auto* server : clusters[clusterID - 1]) {
            if (!server->isMaster && server->isActive()) {
                slaveServer = server;
                break;
            }
        }

        // Check if Slave was found
        if (slaveServer == nullptr) {
            v_mutex.unlock();
            log(WARNING, "No active Slave server in Cluster " << clusterID);
            return Status(grpc::StatusCode::NOT_FOUND, "No active Slave server in cluster");
        }

        // Fill ServerInfo with the Slave server details
        serverinfo->set_serverid(slaveServer->serverID);
        serverinfo->set_hostname(slaveServer->hostname);
        serverinfo->set_port(slaveServer->port);
        serverinfo->set_type(slaveServer->type);
        serverinfo->set_clusterid(clusterID);

        v_mutex.unlock();

        log(INFO, "Returning Slave Server " << slaveServer->serverID
                  << " for Cluster " << clusterID
                  << " (Hostname: " << slaveServer->hostname
                  << ", Port: " << slaveServer->port << ")");

        return Status::OK;
    }

    // Manual synchronizer promotion: demote current Master and promote specified Slave
    Status PromoteSynchronizer(ServerContext* context, const ID* id, Confirmation* confirmation) override {
        int syncID = id->id();

        log(INFO, "PromoteSynchronizer request for SyncID: " << syncID);

        // Validate syncID range (should be 1-6 for 3 clusters with 2 synchronizers each)
        if (syncID < 1 || syncID > 6) {
            log(ERROR, "Invalid SyncID: " << syncID << " (must be 1-6)");
            confirmation->set_status(false);
            return Status::OK;
        }

        v_mutex.lock();

        // Find the synchronizer to promote
        int promoteIndex = findServer(synchronizers, syncID);
        if (promoteIndex == -1) {
            v_mutex.unlock();
            log(ERROR, "SyncID " << syncID << " not found in synchronizers list");
            confirmation->set_status(false);
            return Status::OK;
        }

        zNode* promoteSync = synchronizers[promoteIndex];

        // Calculate which cluster this synchronizer belongs to
        // Formula: clusterID = ((syncID - 1) % 3) + 1
        // syncID 1,4 → cluster 1; syncID 2,5 → cluster 2; syncID 3,6 → cluster 3
        int targetCluster = ((syncID - 1) % 3) + 1;

        log(INFO, "SyncID " << syncID << " belongs to Cluster " << targetCluster);

        // Check if this synchronizer is already Master
        if (promoteSync->isMaster) {
            v_mutex.unlock();
            log(WARNING, "SyncID " << syncID << " is already Master for Cluster " << targetCluster);
            confirmation->set_status(true);
            confirmation->set_ismaster(true);
            return Status::OK;
        }

        // Find and demote the current Master synchronizer for this cluster
        zNode* currentMaster = nullptr;
        for (auto* sync : synchronizers) {
            // Calculate cluster for each synchronizer
            int syncCluster = ((sync->serverID - 1) % 3) + 1;

            if (syncCluster == targetCluster && sync->isMaster) {
                currentMaster = sync;
                break;
            }
        }

        // Demote current Master if exists
        if (currentMaster != nullptr) {
            currentMaster->isMaster = false;
            log(INFO, "Demoted SyncID " << currentMaster->serverID
                      << " from Master to Slave for Cluster " << targetCluster);
        }

        // Promote the specified synchronizer to Master
        promoteSync->isMaster = true;

        v_mutex.unlock();

        log(INFO, "MANUAL PROMOTION: SyncID " << syncID
                  << " promoted to Master for Cluster " << targetCluster);

        confirmation->set_status(true);
        confirmation->set_ismaster(true);

        return Status::OK;
    }

    // Get list of all active synchronizers (both Master and Slave)
    Status GetSynchronizers(ServerContext* context, const ID* id, ServerList* serverlist) override {
        log(INFO, "GetSynchronizers request received");

        v_mutex.lock();

        // Return all active synchronizers (both Master and Slave)
        for (auto* sync : synchronizers) {
            if (sync->isActive()) {
                serverlist->add_serverid(sync->serverID);
                serverlist->add_hostname(sync->hostname);
                serverlist->add_port(sync->port);
                serverlist->add_type(sync->isMaster ? "master" : "slave");
            }
        }

        v_mutex.unlock();

        log(INFO, "Returning " << serverlist->serverid_size() << " active synchronizers");

        return Status::OK;
    }

};

void RunServer(std::string port_no){
    //start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    //localhost = 127.0.0.1
    std::string server_address("127.0.0.1:"+port_no);
    CoordServiceImpl service;
    //grpc::EnableDefaultHealthCheckService(true);
    //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    // Initialize glog
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;  // Output logs to terminal instead of files

    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }

    log(INFO, "Starting Coordinator on port " << port);
    RunServer(port);
    return 0;
}



void checkHeartbeat(){
    while(true){
        v_mutex.lock();

        // Iterate through each cluster
        for (int clusterIdx = 0; clusterIdx < clusters.size(); clusterIdx++) {
            auto& cluster = clusters[clusterIdx];
            bool masterRemoved = false;

            // Check each server in the cluster
            for (auto it = cluster.begin(); it != cluster.end(); ) {
                zNode* server = *it;

                // Check if heartbeat is missed (> 10 seconds)
                if (difftime(getTimeNow(), server->last_heartbeat) > 10) {
                    server->consecutive_missed_heartbeats++;

                    log(WARNING, "Missed heartbeat from ClusterID: " << (clusterIdx + 1)
                                << ", ServerID: " << server->serverID
                                << ", Consecutive misses: " << server->consecutive_missed_heartbeats);

                    // After 2 consecutive missed heartbeats, remove the server
                    if (server->consecutive_missed_heartbeats >= 2) {
                        bool wasMaster = server->isMaster;
                        int removedServerID = server->serverID;

                        log(ERROR, "Removing server - ClusterID: " << (clusterIdx + 1)
                                  << ", ServerID: " << removedServerID
                                  << ", Role: " << (wasMaster ? "Master" : "Slave")
                                  << " (2+ consecutive missed heartbeats)");

                        // Remove the server from the cluster
                        delete server;
                        it = cluster.erase(it);

                        // If the removed server was Master, we need to promote a Slave
                        if (wasMaster) {
                            masterRemoved = true;
                        }

                        continue;  // Don't increment iterator since we erased
                    } else {
                        server->missed_heartbeat = true;
                    }
                }

                ++it;  // Move to next server
            }

            // If Master was removed, promote the first available Slave to Master
            if (masterRemoved && !cluster.empty()) {
                // Find first server that's not already Master
                for (auto* server : cluster) {
                    if (!server->isMaster) {
                        server->isMaster = true;
                        log(INFO, "PROMOTION: ServerID " << server->serverID
                                  << " promoted to Master in ClusterID " << (clusterIdx + 1));

                        // Also promote the corresponding Slave synchronizer to Master
                        // Formula: Slave syncID = Master syncID + 3 = (clusterIdx + 1) + 3
                        int slaveSyncID = (clusterIdx + 1) + 3;

                        // Find and promote the Slave synchronizer
                        for (auto* sync : synchronizers) {
                            if (sync->serverID == slaveSyncID) {
                                // Find and demote current Master synchronizer
                                for (auto* masterSync : synchronizers) {
                                    int syncCluster = ((masterSync->serverID - 1) % 3) + 1;
                                    if (syncCluster == (clusterIdx + 1) && masterSync->isMaster) {
                                        masterSync->isMaster = false;
                                        log(INFO, "DEMOTION: SyncID " << masterSync->serverID
                                                  << " demoted from Master to Slave for Cluster " << (clusterIdx + 1));
                                        break;
                                    }
                                }

                                // Promote Slave synchronizer
                                sync->isMaster = true;
                                log(INFO, "AUTOMATIC SYNCHRONIZER PROMOTION: SyncID " << slaveSyncID
                                          << " promoted to Master for Cluster " << (clusterIdx + 1)
                                          << " (due to tsd server failover)");
                                break;
                            }
                        }

                        break;
                    }
                }
            }
        }

        v_mutex.unlock();

        sleep(3);  // Check every 3 seconds
    }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

// Helper function to find a server in a cluster by serverID
// Returns the index of the server in the vector, or -1 if not found
int findServer(std::vector<zNode*> cluster, int serverID){
    for(size_t i = 0; i < cluster.size(); i++){
        if(cluster[i]->serverID == serverID){
            return i;
        }
    }
    return -1;
}

