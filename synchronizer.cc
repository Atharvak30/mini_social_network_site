// NOTE: This starter code contains a primitive implementation using the default RabbitMQ protocol.
// You are recommended to look into how to make the communication more efficient,
// for example, modifying the type of exchange that publishes to one or more queues, or
// throttling how often a process consumes messages from a queue so other consumers are not starved for messages
// All the functions in this implementation are just suggestions and you can make reasonable changes as long as
// you continue to use the communication methods that the assignment requires between different processes

#include <bits/fs_fwd.h>
#include <ctime>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <semaphore.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <algorithm>
#include <glob.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "sns.grpc.pb.h"
#include "sns.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <jsoncpp/json/json.h>

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

namespace fs = std::filesystem;

using csce438::AllUsers;
using csce438::Confirmation;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using csce438::ServerList;
using csce438::SynchronizerListReply;
using csce438::SynchService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
// tl = timeline, fl = follow list
using csce438::TLFL;

int synchID = 1;
int clusterID = 1;
bool isMaster = false;
int total_number_of_registered_synchronizers = 6; // update this by asking coordinator
std::string coordAddr;
std::string clusterSubdirectory;
std::vector<std::string> otherHosts;
std::unordered_map<std::string, int> timelineLengths;

std::vector<std::string> get_lines_from_file(std::string);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);
std::vector<std::string> getFollowersOfUser(int);
bool file_contains_user(std::string filename, std::string user);

void Heartbeat();
void heartbeatThread();

std::unique_ptr<csce438::CoordService::Stub> coordinator_stub_;
ServerInfo myServerInfo;

class SynchronizerRabbitMQ
{
private:
    amqp_connection_state_t conn;
    amqp_channel_t channel;
    std::string hostname;
    int port;
    int synchID;

    void setupRabbitMQ()
    {
        conn = amqp_new_connection();
        amqp_socket_t *socket = amqp_tcp_socket_new(conn);
        amqp_socket_open(socket, hostname.c_str(), port);
        amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
        amqp_channel_open(conn, channel);
        //amqpl_confirm_select(conn, channel); //RABBIT CHANGE
    }

    void declareQueue(const std::string &queueName)
    {
        amqp_queue_declare(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, 0, 0, amqp_empty_table);
    }

    void publishMessage(const std::string &queueName, const std::string &message)
    {
        //old rabbit
        amqp_basic_publish(conn, channel, amqp_empty_bytes, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, NULL, amqp_cstring_bytes(message.c_str()));

    }

    std::string consumeMessage(const std::string &queueName, int timeout_ms = 5000)
    {
        amqp_basic_consume(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);

        struct timeval timeout;
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;

        amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);

        if (res.reply_type != AMQP_RESPONSE_NORMAL)
        {
            return "";
        }

        std::string message(static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len);
        amqp_destroy_envelope(&envelope);
        return message;
    }

public:
    // SynchronizerRabbitMQ(const std::string &host, int p, int id) : hostname(host), port(p), channel(1), synchID(id)
    SynchronizerRabbitMQ(const std::string &host, int p, int id) : hostname("rabbitmq"), port(p), channel(1), synchID(id)
    {
        setupRabbitMQ();
        declareQueue("synch" + std::to_string(synchID) + "_users_queue");
        declareQueue("synch" + std::to_string(synchID) + "_clients_relations_queue");
        declareQueue("synch" + std::to_string(synchID) + "_timeline_queue");
        // TODO: add or modify what kind of queues exist in your clusters based on your needs
    }

    void publishUserList()
    {
        // Only Master synchronizers publish to RabbitMQ
        if (!isMaster) {
            return;
        }

        std::vector<std::string> users = get_all_users_func(synchID);
        std::sort(users.begin(), users.end());
        Json::Value userList;
        for (const auto &user : users)
        {
            userList["users"].append(user);
        }
        Json::FastWriter writer;
        std::string message = writer.write(userList);

        // Get synchronizer list from coordinator and publish messages there
        ClientContext context;
        ID request;
        request.set_id(0);  // ID parameter not used
        ServerList serverList;

        Status status = coordinator_stub_->GetSynchronizers(&context, request, &serverList);

        if (!status.ok()) {
            log(ERROR, "Failed to get synchronizer list from coordinator: " + status.error_message());
            return;
        }

        // Calculate our paired synchronizer ID (Master <-> Slave in same cluster)
        // If we're Master (1,2,3), our Slave is synchID+3. If Slave (4,5,6), Master is synchID-3
        int pairedSyncID = (synchID <= 3) ? (synchID + 3) : (synchID - 3);

        // Publish to all synchronizer queues except our own and our paired sync
        int publishCount = 0;
        for (int i = 0; i < serverList.serverid_size(); i++) {
            int targetSyncID = serverList.serverid(i);

            // Skip our own queue and our paired synchronizer's queue
            if (targetSyncID == synchID || targetSyncID == pairedSyncID) {
                continue;
            }

            std::string queueName = "synch" + std::to_string(targetSyncID) + "_users_queue";
            publishMessage(queueName, message);
            //log(INFO, "Published to queue: " + queueName);
            publishCount++;
        }
        //log(INFO, "Published user list to " + std::to_string(publishCount) + " synchronizers");

    }

    void consumeUserLists()
    {
        std::vector<std::string> allUsers;

        // Consume from OUR OWN queue (where other synchronizers published TO us)
        std::string myQueueName = "synch" + std::to_string(synchID) + "_users_queue";

        int messageCount = 0;
        // Consume all available messages from our queue until empty
        while (true) {
            std::string message = consumeMessage(myQueueName, 1000); // 1 second timeout

            if (message.empty()) {
                break; // No more messages in queue
            }

            // Parse the JSON message
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root)) {
                for (const auto &user : root["users"]) {
                    allUsers.push_back(user.asString());
                }
                messageCount++;
            } else {
                log(ERROR, "Failed to parse JSON message from queue: " + myQueueName);
            }
        }

        updateAllUsersFile(allUsers);
    }

    void publishClientRelations()
    {
        if (!isMaster) return;

        Json::Value relations(Json::objectValue);

        // Scan ALL *_followers.txt files in the directory (not just local users)
        // This includes users from other clusters who are followed by users in this cluster
        std::string directory = "cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/";

        // Use glob to find all _followers.txt files
        glob_t glob_result;
        std::string pattern = directory + "*_followers.txt";
        glob(pattern.c_str(), GLOB_TILDE, NULL, &glob_result);

        for (size_t i = 0; i < glob_result.gl_pathc; ++i) {
            std::string filepath = glob_result.gl_pathv[i];

            // Extract username from filename (remove path and "_followers.txt")
            size_t lastSlash = filepath.find_last_of("/");
            std::string filename = (lastSlash != std::string::npos) ? filepath.substr(lastSlash + 1) : filepath;
            std::string username = filename.substr(0, filename.find("_followers.txt"));

            // Read followers from this file
            std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + username + "_followers";
            sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

            if (fileSem == SEM_FAILED) {
                log(ERROR, "Failed to open semaphore for " + filepath);
                continue;
            }

            sem_wait(fileSem);

            Json::Value followerList(Json::arrayValue);
            std::ifstream file(filepath);
            if (file.is_open()) {
                std::string line;
                while (std::getline(file, line)) {
                    if (!line.empty()) {
                        followerList.append(line);
                    }
                }
                file.close();
            }

            sem_post(fileSem);
            sem_close(fileSem);

            // Include ALL users (even with empty follower lists)
            relations[username] = followerList;
        }

        globfree(&glob_result);

        // Write JSON snapshot
        Json::StreamWriterBuilder builder;
        builder["indentation"] = ""; // compact JSON
        std::string message = Json::writeString(builder, relations);
        log(INFO, "Publishing client relations snapshot JSON: " + message);

        // Send to other synchronizers
        ClientContext context;
        ID request;
        request.set_id(0);
        ServerList serverList;

        Status status = coordinator_stub_->GetSynchronizers(&context, request, &serverList);
        if (!status.ok()) {
            log(ERROR, "Failed to get synchronizer list: " + status.error_message());
            return;
        }

        int pairedSyncID = (synchID <= 3) ? (synchID + 3) : (synchID - 3);

        for (int i = 0; i < serverList.serverid_size(); i++) {
            int target = serverList.serverid(i);

            if (target == synchID || target == pairedSyncID) continue;

            std::string queueName =
                "synch" + std::to_string(target) + "_clients_relations_queue";

            log(INFO, "ABOUT TO PUBLISH CLIENT RELATIONS to queue: " + queueName + " | Message: " + message.substr(0, 200));
            publishMessage(queueName, message);
            log(INFO, "Published snapshot to " + queueName);
        }
    }

    // void publishClientRelations()
    // {
    //     // Only Master synchronizers publish to RabbitMQ
    //     if (!isMaster) {
    //         return;
    //     }

    //     Json::Value relations;

    //     // Scan all *_followers.txt files in the cluster directory
    //     // This includes users from other clusters who are followed by users in this cluster
    //     std::string directory = "cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/";

    //     // Use glob to find all _followers.txt files
    //     glob_t glob_result;
    //     std::string pattern = directory + "*_followers.txt";
    //     glob(pattern.c_str(), GLOB_TILDE, NULL, &glob_result);

    //     for (size_t i = 0; i < glob_result.gl_pathc; ++i) {
    //         std::string filepath = glob_result.gl_pathv[i];

    //         // Extract username from filename (remove path and "_followers.txt")
    //         size_t lastSlash = filepath.find_last_of("/");
    //         std::string filename = (lastSlash != std::string::npos) ? filepath.substr(lastSlash + 1) : filepath;
    //         std::string username = filename.substr(0, filename.find("_followers.txt"));

    //         // Read followers from this file
    //         std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + username + "_followers";
    //         sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

    //         if (fileSem == SEM_FAILED) {
    //             log(ERROR, "Failed to open semaphore for " + filepath);
    //             continue;
    //         }

    //         sem_wait(fileSem);

    //         Json::Value followerList(Json::arrayValue);
    //         std::ifstream file(filepath);
    //         if (file.is_open()) {
    //             std::string line;
    //             while (std::getline(file, line)) {
    //                 if (!line.empty()) {
    //                     // Just read the username directly (no timestamp parsing needed)
    //                     followerList.append(line);
    //                 }
    //             }
    //             file.close();
    //         }

    //         sem_post(fileSem);
    //         sem_close(fileSem);

    //         if (!followerList.empty()) {
    //             relations[username] = followerList;
    //         }
    //     }

    //     globfree(&glob_result);

    //     Json::FastWriter writer;
    //     std::string message = writer.write(relations);

    //     // Get synchronizer list from coordinator and publish to other clusters
    //     ClientContext context;
    //     ID request;
    //     request.set_id(0);  // ID parameter not used
    //     ServerList serverList;

    //     Status status = coordinator_stub_->GetSynchronizers(&context, request, &serverList);

    //     if (!status.ok()) {
    //         log(ERROR, "Failed to get synchronizer list from coordinator: " + status.error_message());
    //         return;
    //     }

    //     // Calculate our paired synchronizer ID (Master <-> Slave in same cluster)
    //     int pairedSyncID = (synchID <= 3) ? (synchID + 3) : (synchID - 3);

    //     // Publish to all synchronizer queues except our own and our paired sync
    //     int publishCount = 0;
    //     for (int i = 0; i < serverList.serverid_size(); i++) {
    //         int targetSyncID = serverList.serverid(i);

    //         // Skip our own queue and our paired synchronizer's queue
    //         if (targetSyncID == synchID || targetSyncID == pairedSyncID) {
    //             continue;
    //         }

    //         std::string queueName = "synch" + std::to_string(targetSyncID) + "_clients_relations_queue";
    //         publishMessage(queueName, message);
    //         log(INFO, "Published client relations to queue: " + queueName + " message" + message);
    //         publishCount++;
    //     }
    // }

    void consumeClientRelations()
    {
        std::vector<std::string> allUsers = get_all_users_func(synchID);

        std::string queueName =
            "synch" + std::to_string(synchID) + "_clients_relations_queue";

        while (true)
        {
            log(INFO, "Entering consume loop " )
            std::string message = consumeMessage(queueName, 1000);
            if (message.empty()) break;

            Json::Value root;
            Json::Reader reader;

            if (!reader.parse(message, root)) {
                log(ERROR, "Failed to parse JSON message from " + queueName);
                continue;
            }
            log(INFO, "Processing consume message " + message )

            for (const auto &client : allUsers)
            {
                if (!root.isMember(client)) {
                    // Snapshot doesn't include this user → do NOT overwrite
                    continue;
                }

                std::set<std::string> newFollowers;
                for (const auto &entry : root[client]) {
                    newFollowers.insert(entry.asString());
                }

                std::string followerFile =
                    "./cluster_" + std::to_string(clusterID) + "/" +
                    clusterSubdirectory + "/" + client + "_followers.txt";

                std::string semName = "/" + std::to_string(clusterID) + "_" +
                                    clusterSubdirectory + "_" + client + "_followers";

                sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);
                if (fileSem == SEM_FAILED) {
                    log(ERROR, "Failed to open semaphore for " + followerFile);
                    continue;
                }

                sem_wait(fileSem);

                // Rewrite entire file with new snapshot
                std::ofstream out(followerFile, std::ios::trunc);
                if (out.is_open()) {
                    for (const auto &f : newFollowers)
                        out << f << "\n";
                    out.close();
                }

                sem_post(fileSem);
                sem_close(fileSem);

                log(INFO, "Synced snapshot into " + client + "_followers.txt");
            }
        }
    }

    // void consumeClientRelations()
    // {
    //     std::vector<std::string> allUsers = get_all_users_func(synchID);

    //     // Consume from OUR OWN queue (where other synchronizers published TO us)
    //     std::string myQueueName = "synch" + std::to_string(synchID) + "_clients_relations_queue";

    //     int messageCount = 0;
    //     // Consume all available messages from our queue until empty
    //     while (true) {
    //         std::string message = consumeMessage(myQueueName, 1000); // 1 second timeout

    //         if (message.empty()) {
    //             break; // No more messages in queue
    //         }

    //         // Parse the JSON message
    //         Json::Value root;
    //         Json::Reader reader;
    //         if (reader.parse(message, root)) {
    //             // Update follower files for clients in our cluster
    //             for (const auto &client : allUsers) {
    //                 std::string followerFile = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + client + "_followers.txt";
    //                 std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + client + "_followers";
    //                 sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

    //                 if (fileSem == SEM_FAILED) {
    //                     log(ERROR, "Failed to open semaphore for " + followerFile);
    //                     continue;
    //                 }

    //                 // Acquire semaphore lock
    //                 sem_wait(fileSem);

    //                 // Read existing followers into a set
    //                 std::set<std::string> existingFollowers;
    //                 bool fileExisted = false;
    //                 std::ifstream readFile(followerFile);
    //                 if (readFile.is_open()) {
    //                     fileExisted = true;
    //                     std::string line;
    //                     while (std::getline(readFile, line)) {
    //                         if (!line.empty()) {
    //                             // Just read the username directly (no timestamp parsing)
    //                             existingFollowers.insert(line);
    //                         }
    //                     }
    //                     readFile.close();
    //                 }

    //                 // Build set of followers from consumed message (the complete authoritative list)
    //                 std::set<std::string> consumedFollowers;
    //                 if (root.isMember(client)) {
    //                     for (const auto &follower : root[client]) {
    //                         consumedFollowers.insert(follower.asString());
    //                     }
    //                 }

    //                 // Determine what changed
    //                 std::set<std::string> followersToAdd;
    //                 std::set<std::string> followersToRemove;

    //                 // Find new followers to add (in consumed but not in existing)
    //                 for (const auto &follower : consumedFollowers) {
    //                     if (existingFollowers.find(follower) == existingFollowers.end()) {
    //                         followersToAdd.insert(follower);
    //                     }
    //                 }

    //                 // Find followers to remove (in existing but not in consumed)
    //                 for (const auto &follower : existingFollowers) {
    //                     if (consumedFollowers.find(follower) == consumedFollowers.end()) {
    //                         followersToRemove.insert(follower);
    //                     }
    //                 }

    //                 // Only rewrite file if there are changes OR if file is new with content
    //                 if (!followersToAdd.empty() || !followersToRemove.empty()) {
    //                     // Rewrite the entire file with the correct follower list
    //                     std::ofstream followerStream(followerFile, std::ios::trunc);
    //                     if (followerStream.is_open()) {
    //                         for (const auto &follower : consumedFollowers) {
    //                             followerStream << follower << std::endl;
    //                         }
    //                         followerStream.close();

    //                         // Build follower list string for logging
    //                         std::string followerList = "";
    //                         int count = 0;
    //                         for (const auto &follower : consumedFollowers) {
    //                             if (count > 0) followerList += ", ";
    //                             followerList += follower;
    //                             count++;
    //                         }

    //                         // Log whether this is a new file or an update
    //                         if (!fileExisted || existingFollowers.empty()) {
    //                             log(INFO, "Created " + client + "_followers.txt: [" + followerList + "]");
    //                         } else {
    //                             log(INFO, "Updated " + client + "_followers.txt: [" + followerList + "]");
    //                         }
    //                     }
    //                 }

    //                 // Release semaphore lock
    //                 sem_post(fileSem);
    //                 sem_close(fileSem);
    //             }
    //             messageCount++;
    //         } else {
    //             log(ERROR, "Failed to parse JSON message from queue: " + myQueueName);
    //         }
    //     }
    // }

    // for every client in your cluster, update all their followers' timeline files
    // by publishing your user's timeline file (or just the new updates in them)
    //  periodically to the message queue of the synchronizer responsible for that client
    void publishTimelines()
    {
        log(INFO, ">>> publishTimelines() CALLED, synchID=" + std::to_string(synchID) + ", isMaster=" + std::to_string(isMaster));

        // Only Master synchronizers publish to RabbitMQ
        if (!isMaster) {
            log(INFO, ">>> publishTimelines() EARLY RETURN: Not Master");
            return;
        }

        std::vector<std::string> users = get_all_users_func(synchID);
        log(INFO, ">>> publishTimelines() Got " + std::to_string(users.size()) + " users");

        // Get list of all synchronizers from coordinator
        ClientContext context;
        ID request;
        request.set_id(0);
        ServerList serverList;
        Status status = coordinator_stub_->GetSynchronizers(&context, request, &serverList);

        if (!status.ok()) {
            log(ERROR, "Failed to get synchronizer list: " + status.error_message());
            return;
        }

        // Calculate paired synchronizer ID (skip our own queue and paired sync queue)
        int pairedSyncID = (synchID <= 3) ? (synchID + 3) : (synchID - 3);

        for (const auto &client : users)
        {
            int clientId = std::stoi(client);
            int client_cluster = ((clientId - 1) % 3) + 1;
            log(INFO, ">>> Checking user " + client + ", cluster=" + std::to_string(client_cluster) + ", myCluster=" + std::to_string(clusterID));
            // only do this for clients in your own cluster
            if (client_cluster != clusterID)
            {
                log(INFO, ">>> SKIP user " + client + ": wrong cluster");
                continue;
            }

            // Get ENTIRE timeline (no incremental tracking)
            std::vector<std::string> timeline = get_tl_or_fl(synchID, clientId, true);
            log(INFO, ">>> User " + client + " has timeline size: " + std::to_string(timeline.size()));

            // Skip if timeline is empty
            if (timeline.empty()) {
                log(INFO, ">>> SKIP user " + client + ": empty timeline");
                continue;
            }

            // SIMPLIFIED APPROACH: Publish to ALL synchronizers (not filtering by followers)
            // Create JSON message with poster and COMPLETE timeline snapshot
            Json::Value message;
            message["poster"] = client;

            Json::Value timelineArray(Json::arrayValue);
            for (const auto &line : timeline) {
                timelineArray.append(line);
            }
            message["timeline_updates"] = timelineArray;

            // Empty followers array (we're broadcasting to everyone)
            Json::Value followersArray(Json::arrayValue);
            message["followers"] = followersArray;

            Json::FastWriter writer;
            std::string jsonMessage = writer.write(message);

            // Publish to ALL synchronizers except our own and our paired sync
            int publishCount = 0;
            for (int i = 0; i < serverList.serverid_size(); i++) {
                int targetSyncID = serverList.serverid(i);

                // Skip our own queue and our paired synchronizer's queue
                if (targetSyncID == synchID || targetSyncID == pairedSyncID) {
                    continue;
                }

                std::string queueName = "synch" + std::to_string(targetSyncID) + "_timeline_queue";
                log(INFO, "ABOUT TO PUBLISH TIMELINE to queue: " + queueName + " | Message: " + jsonMessage.substr(0, 200));
                publishMessage(queueName, jsonMessage);
                log(INFO, "Published timeline from user " + client + " to queue: " + queueName);
                publishCount++;
            }

            log(INFO, "Published user " + client + " timeline to " + std::to_string(publishCount) + " synchronizer queues");

            // OLD FOLLOWER-BASED APPROACH (COMMENTED OUT)
            // // Get followers of this user
            // std::vector<std::string> followers = getFollowersOfUser(clientId);
            // log(INFO, ">>> User " + client + " has " + std::to_string(followers.size()) + " followers");
            //
            // // Group followers by their cluster
            // std::map<int, std::vector<std::string>> followersByCluster;
            //
            // for (const auto &follower : followers)
            // {
            //     int followerId = std::stoi(follower);
            //     int followerCluster = ((followerId - 1) % 3) + 1;
            //
            //     // Skip followers in our own cluster (they already have the timeline file)
            //     if (followerCluster == clusterID) {
            //         continue;
            //     }
            //
            //     followersByCluster[followerCluster].push_back(follower);
            // }
            //
            // log(INFO, ">>>>>>> User " + client + " has followers in " + std::to_string(followersByCluster.size()) + " other clusters");
            //
            // // Publish to both Master and Slave synchronizers for each target cluster
            // for (const auto &entry : followersByCluster) {
            //     int targetCluster = entry.first;
            //     const std::vector<std::string> &targetFollowers = entry.second;
            //
            //     // Create JSON message with poster and COMPLETE timeline snapshot
            //     Json::Value message;
            //     message["poster"] = client;
            //
            //     Json::Value timelineArray(Json::arrayValue);
            //     for (const auto &line : timeline) {
            //         timelineArray.append(line);
            //     }
            //     message["timeline_updates"] = timelineArray;
            //
            //     Json::Value followersArray(Json::arrayValue);
            //     for (const auto &follower : targetFollowers) {
            //         followersArray.append(follower);
            //     }
            //     message["followers"] = followersArray;
            //
            //     Json::FastWriter writer;
            //     std::string jsonMessage = writer.write(message);
            //
            //     // Publish to BOTH Master and Slave synchronizers for this cluster
            //     // Master synch ID = cluster ID (1, 2, 3)
            //     int masterSyncID = targetCluster;
            //     // Slave synch ID = cluster ID + 3 (4, 5, 6)
            //     int slaveSyncID = targetCluster + 3;
            //
            //     // Publish to Master synchronizer
            //     std::string masterQueueName = "synch" + std::to_string(masterSyncID) + "_timeline_queue";
            //     log(INFO, "ABOUT TO PUBLISH TIMELINE to queue: " + masterQueueName + " | Message: " + jsonMessage.substr(0, 200));
            //     publishMessage(masterQueueName, jsonMessage);
            //     log(INFO, "Published timeline updates from user " + client + " to Master queue: " + masterQueueName + " for " + std::to_string(targetFollowers.size()) + " followers");
            //
            //     // Publish to Slave synchronizer
            //     std::string slaveQueueName = "synch" + std::to_string(slaveSyncID) + "_timeline_queue";
            //     log(INFO, "ABOUT TO PUBLISH TIMELINE to queue: " + slaveQueueName + " | Message: " + jsonMessage.substr(0, 200));
            //     publishMessage(slaveQueueName, jsonMessage);
            //     log(INFO, "Published timeline updates from user " + client + " to Slave queue: " + slaveQueueName + " for " + std::to_string(targetFollowers.size()) + " followers");
            // }
        }
    }

    // For each client in your cluster, consume messages from your timeline queue and modify your client's timeline files based on what the users they follow posted to their timeline
    void consumeTimelines()
    {
        std::string queueName = "synch" + std::to_string(synchID) + "_timeline_queue";

        // Consume ALL available messages from queue (like consumeClientRelations does)
        while (true) {
            std::string message = consumeMessage(queueName, 1000); // 1 second timeout

        
            if (message.empty()) {
                break; // No more messages in queue
            }
            log(INFO, "TIMELINE MESSAGE ----| Raw message: " + message);


            // Parse JSON message
            // Expected format: {"poster": "1", "timeline_updates": ["T 123", "U 1", "W Hello", ""], "followers": ["5", "8"]}
            Json::Value root;
            Json::CharReaderBuilder readerBuilder;
            std::string errs;
            std::istringstream messageStream(message);

            if (!Json::parseFromStream(readerBuilder, messageStream, &root, &errs)) {
                log(ERROR, "Failed to parse timeline message: " + errs + " | Raw message: " + message.substr(0, 100));
                continue; // Skip this message and process next one
            }

            // Extract poster ID
            if (!root.isMember("poster") || !root.isMember("timeline_updates")) {
                log(ERROR, "Timeline message missing required fields member not in root| Raw message: " + message.substr(0, 100));
                continue; // Skip this message and process next one
            }

            std::string poster = root["poster"].asString();

            // Extract timeline updates (array of strings)
            std::vector<std::string> timelineUpdates;
            if (root["timeline_updates"].isArray()) {
                for (const auto& line : root["timeline_updates"]) {
                    timelineUpdates.push_back(line.asString());
                }
            }

            if (timelineUpdates.empty()) {
                log(INFO, "No timeline updates to consume for poster: " + poster);
                continue; // Skip this message and process next one
            }

            // Extract followers (for logging purposes)
            std::vector<std::string> followers;
            if (root.isMember("followers") && root["followers"].isArray()) {
                for (const auto& follower : root["followers"]) {
                    followers.push_back(follower.asString());
                }
            }

            // Determine local timeline file path
            // Format: cluster_{clusterID}/{subdirectory}/{poster}.txt
            // subdirectory = 1 for Master, 2 for Slave
            std::string subdirectory = isMaster ? "1" : "2";
            std::string timelineFile = "./cluster_" + std::to_string(clusterID) + "/" + subdirectory + "/" + poster + ".txt";

            // Create directory if it doesn't exist
            std::string mkdir_command = "mkdir -p ./cluster_" + std::to_string(clusterID) + "/" + subdirectory;
            system(mkdir_command.c_str());

            // Use semaphore for file synchronization with TSD
            std::string semName = "/" + std::to_string(clusterID) + "_" + subdirectory + "_" + poster + "_timeline";
            sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

            if (fileSem == SEM_FAILED) {
                log(ERROR, "Failed to open semaphore " + semName + " for timeline file: " + timelineFile);
                continue; // Skip this message and process next one
            }

            // Acquire semaphore lock
            sem_wait(fileSem);

            // OVERWRITE timeline file with complete snapshot from poster
            std::ofstream file(timelineFile, std::ios::trunc);
            if (file.is_open()) {
                for (const auto& line : timelineUpdates) {
                    file << line << std::endl;
                }
                file.close();

                // Build follower list string for logging
                std::string followerList = "";
                for (size_t i = 0; i < followers.size(); i++) {
                    followerList += followers[i];
                    if (i < followers.size() - 1) followerList += ", ";
                }

                log(INFO, "Overwrote timeline file for user " + poster + " with " + std::to_string(timelineUpdates.size()) +
                          " lines (consumed from " + queueName + ")" +
                          " - Followers in this cluster: [" + followerList + "]");
            } else {
                log(ERROR, "Failed to open timeline file for writing: " + timelineFile);
            }

            // Release semaphore lock
            sem_post(fileSem);
            sem_close(fileSem);
        }
    }

private:
    void updateAllUsersFile(const std::vector<std::string> &users)
    {

        std::string usersFile = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/all_users.txt";
        std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_all_users";
        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);

        std::ofstream userStream(usersFile, std::ios::app | std::ios::out | std::ios::in);
        for (std::string user : users)
        {
            if (!file_contains_user(usersFile, user))
            {
                userStream << user << std::endl;
            }
        }
        sem_close(fileSem);
    }
};

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ);

class SynchServiceImpl final : public SynchService::Service
{
    // You do not need to modify this in any way
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID)
{
    // localhost = 127.0.0.1
    std::string server_address("127.0.0.1:" + port_no);
    log(INFO, "Starting synchronizer server at " + server_address);
    SynchServiceImpl service;
    // grpc::EnableDefaultHealthCheckService(true);
    // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Initialize RabbitMQ connection
    // SynchronizerRabbitMQ rabbitMQ("localhost", 5672, synchID);
    SynchronizerRabbitMQ rabbitMQ("rabbitmq", 5672, synchID);

    std::thread t1(run_synchronizer, coordIP, coordPort, port_no, synchID, std::ref(rabbitMQ));

    // Create a consumer thread for user lists, client relations, and timelines
    std::thread consumerThread([&rabbitMQ]()
                               {
        while (true) {
            rabbitMQ.consumeUserLists();
            rabbitMQ.consumeClientRelations();
            rabbitMQ.consumeTimelines();
            std::this_thread::sleep_for(std::chrono::seconds(5));
        } });

    server->Wait();

    //   t1.join();
    //   consumerThread.join();
}

int main(int argc, char **argv)
{
    int opt = 0;
    std::string coordIP;
    std::string coordPort;
    std::string port = "3029";

    while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1)
    {
        switch (opt)
        {
        case 'h':
            coordIP = optarg;
            break;
        case 'k':
            coordPort = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        case 'i':
            synchID = std::stoi(optarg);
            break;
        default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }

    std::string log_file_name = std::string("synchronizer-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    coordAddr = coordIP + ":" + coordPort;
    clusterID = ((synchID - 1) % 3) + 1;

    // Initialize global ServerInfo
    myServerInfo.set_hostname("localhost");
    myServerInfo.set_port(port);
    myServerInfo.set_type("synchronizer");
    myServerInfo.set_serverid(synchID);
    myServerInfo.set_clusterid(clusterID);

    // Initialize global coordinator stub
    std::string coordinatorInfo = coordIP + ":" + coordPort;
    coordinator_stub_ = std::unique_ptr<CoordService::Stub>(
        CoordService::NewStub(grpc::CreateChannel(coordinatorInfo, grpc::InsecureChannelCredentials()))
    );

    // Send initial heartbeat
    log(INFO, "Sending initial heartbeat to coordinator");
    Heartbeat();

    std::string role = isMaster ? "Master" : "Slave";
    log(INFO, "Synchronizer registered - Role: " + role +
              ", ClusterID: " + std::to_string(clusterID) +
              ", SyncID: " + std::to_string(synchID) +
              ", Directory: cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/");

    // Start periodic heartbeat thread
    std::thread hbThread(heartbeatThread);
    hbThread.detach();
    log(INFO, "Heartbeat thread started");

    RunServer(coordIP, coordPort, port, synchID);
    return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ)
{
    // setup coordinator stub
    std::string target_str = coordIP + ":" + coordPort;
    std::unique_ptr<CoordService::Stub> coord_stub_;
    coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));

    ServerInfo msg;
    Confirmation c;

    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1");
    msg.set_port(port);
    msg.set_type("follower");

    // TODO: begin synchronization process
    while (true)
    {
        // the synchronizers sync files every 5 seconds
        sleep(5);

        grpc::ClientContext context;
        ServerList followerServers;
        ID id;
        id.set_id(synchID);

        // making a request to the coordinator to see count of follower synchronizers
        coord_stub_->GetAllFollowerServers(&context, id, &followerServers);

        std::vector<int> server_ids;
        std::vector<std::string> hosts, ports;
        for (std::string host : followerServers.hostname())
        {
            hosts.push_back(host);
        }
        for (std::string port : followerServers.port())
        {
            ports.push_back(port);
        }
        for (int serverid : followerServers.serverid())
        {
            server_ids.push_back(serverid);
        }

        // update the count of how many follower sychronizer processes the coordinator has registered

        // below here, you run all the update functions that synchronize the state across all the clusters
        // make any modifications as necessary to satisfy the assignments requirements

        // Publish user list
        rabbitMQ.publishUserList();

        // Publish client relations
        rabbitMQ.publishClientRelations();

        // Publish timelines
        rabbitMQ.publishTimelines();
    }
    return;
}

std::vector<std::string> get_lines_from_file(std::string filename)
{
    std::vector<std::string> users;
    std::string user;
    std::ifstream file;
    std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + filename;
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
    file.open(filename);
    if (file.peek() == std::ifstream::traits_type::eof())
    {
        // return empty vector if empty file
        // std::cout<<"returned empty vector bc empty file"<<std::endl;
        file.close();
        sem_close(fileSem);
        return users;
    }
    while (file)
    {
        getline(file, user);

        if (!user.empty())
            users.push_back(user);
    }

    file.close();
    sem_close(fileSem);

    return users;
}

void Heartbeat()
{
    // Send heartbeat to the coordinator to check role and register/update status
    ClientContext context;
    context.AddMetadata("clusterid", std::to_string(clusterID));
    Confirmation confirmation;

    Status status = coordinator_stub_->Heartbeat(&context, myServerInfo, &confirmation);

    if (status.ok() && confirmation.status()) {
        // Update role based on Coordinator's response
        isMaster = confirmation.ismaster();

        // Set clusterSubdirectory based on role: "1" for Master, "2" for Slave
        clusterSubdirectory = isMaster ? "1" : "2";

        std::string role = isMaster ? "Master" : "Slave";

        // Log heartbeat
        //log(INFO, "Heartbeat sent, Role: " + role);
    } else {
        log(ERROR, "Heartbeat failed: " + status.error_message());
    }
}

void heartbeatThread()
{
    while (true) {
        sleep(10);  // Send heartbeat every 10 seconds
        Heartbeat();
    }
}

bool file_contains_user(std::string filename, std::string user)
{
    std::vector<std::string> users;
    // check username is valid
    std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + filename;
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
    users = get_lines_from_file(filename);
    for (int i = 0; i < users.size(); i++)
    {
        // std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
        if (user == users[i])
        {
            // std::cout<<"found"<<std::endl;
            sem_close(fileSem);
            return true;
        }
    }
    // std::cout<<"not found"<<std::endl;
    sem_close(fileSem);
    return false;
}

std::vector<std::string> get_all_users_func(int synchID)
{
    // read all_users file master and client for correct serverID
    // std::string master_users_file = "./master"+std::to_string(synchID)+"/all_users";
    // std::string slave_users_file = "./slave"+std::to_string(synchID)+"/all_users";
    std::string clusterID = std::to_string(((synchID - 1) % 3) + 1);
    std::string master_users_file = "./cluster_" + clusterID + "/1/all_users.txt";
    std::string slave_users_file = "./cluster_" + clusterID + "/2/all_users.txt";
    // take longest list and package into AllUsers message
    std::vector<std::string> master_user_list = get_lines_from_file(master_users_file);
    std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file);

    if (master_user_list.size() >= slave_user_list.size())
        return master_user_list;
    else
        return slave_user_list;
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl)
{
    // std::string master_fn = "./master"+std::to_string(synchID)+"/"+std::to_string(clientID);
    // std::string slave_fn = "./slave"+std::to_string(synchID)+"/" + std::to_string(clientID);
    std::string master_fn = "cluster_" + std::to_string(clusterID) + "/1/" + std::to_string(clientID);
    std::string slave_fn = "cluster_" + std::to_string(clusterID) + "/2/" + std::to_string(clientID);
    if (tl)
    {
        master_fn.append(".txt");
        slave_fn.append(".txt");
    }
    else
    {
        master_fn.append("_followers.txt");
        slave_fn.append("_followers.txt");
    }

    std::vector<std::string> m = get_lines_from_file(master_fn);
    std::vector<std::string> s = get_lines_from_file(slave_fn);

    if (m.size() >= s.size())
    {
        return m;
    }
    else
    {
        return s;
    }
}

std::vector<std::string> getFollowersOfUser(int ID)
{
    std::vector<std::string> followers;
    std::string clientID = std::to_string(ID);
    std::vector<std::string> usersInCluster = get_all_users_func(synchID);

    for (auto userID : usersInCluster)
    { // Examine each user's following file
        std::string file = "cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + userID + "_following.txt";
        std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + userID + "_following";
        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
        // std::cout << "Reading file " << file << std::endl;
        if (file_contains_user(file, clientID))
        {
            followers.push_back(userID);
        }
        sem_close(fileSem);
    }

    return followers;
}
