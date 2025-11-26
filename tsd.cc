/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <vector>
#include <algorithm>
#include <thread>
#include <semaphore.h>
#include <fcntl.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity);

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"


using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::ClientContext;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;


struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;

  // Track when follow relationships started
  std::map<std::string, std::time_t> following_since;  // When I started following each user
  std::map<std::string, std::time_t> follower_since;   // When each user started following me

  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;

// Global variables for server configuration
std::string g_clusterID;
std::string g_serverID;
std::string g_coordinatorIP;
std::string g_coordinatorPort;
std::string g_serverPort;
std::string g_serverDirectory;  // Directory where this server stores files
bool g_isMaster = false;        // Is this server currently acting as Master?
std::unique_ptr<CoordService::Stub> coordinator_stub_;
std::unique_ptr<SNSService::Stub> slave_stub_;  // Master uses this to mirror to Slave

// Track what we've written to synchronizer file to avoid redundant writes
std::set<std::string> g_lastWrittenUsers;

// Function to write current users to the synchronizer's all_users file
void writeUsersToSynchronizerFile() {
    // Collect all usernames from client_db
    std::set<std::string> currentUsers;
    for (Client* client : client_db) {
        currentUsers.insert(client->username);
    }

    // Check if there are any changes compared to last write
    if (currentUsers == g_lastWrittenUsers) {
        // No changes, skip writing
        return;
    }

    // Determine synchronizer subdirectory based on role
    std::string syncSubdir = g_isMaster ? "1" : "2";
    std::string syncDir = "./cluster_" + g_clusterID + "/" + syncSubdir + "/";
    std::string allUsersFile = syncDir + "all_users.txt";
    std::string role = g_isMaster ? "Master" : "Slave";

    log(INFO, "Writing user list to synchronizer file (" + role + " - Cluster " + g_clusterID + ")");

    // Create synchronizer directory if it doesn't exist
    std::string mkdir_command = "mkdir -p " + syncDir;
    system(mkdir_command.c_str());

    // Use named semaphore for synchronization with synchronizer process
    std::string semName = "/" + g_clusterID + "_" + syncSubdir + "_all_users";
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

    if (fileSem == SEM_FAILED) {
        log(ERROR, "Failed to open semaphore " + semName + " for " + allUsersFile);
        return;
    }

    // Acquire semaphore lock
    sem_wait(fileSem);
    log(INFO, "Acquired semaphore lock for " + allUsersFile);

    // Read existing users from file into a set (to avoid duplicates)
    std::set<std::string> existingUsers;
    std::ifstream readFile(allUsersFile);
    if (readFile.is_open()) {
        std::string line;
        while (std::getline(readFile, line)) {
            if (!line.empty()) {
                existingUsers.insert(line);
            }
        }
        readFile.close();
    }

    // Append new users only (users not already in the file)
    int newUsersCount = 0;
    std::ofstream file(allUsersFile, std::ios::app);
    if (file.is_open()) {
        for (const auto& username : currentUsers) {
            // Only append if user doesn't already exist in file
            if (existingUsers.find(username) == existingUsers.end()) {
                file << username << std::endl;
                newUsersCount++;
                log(INFO, "Appending new user: " + username + " to " + allUsersFile);
            }
        }
        file.close();

        // Update our tracking variable
        g_lastWrittenUsers = currentUsers;

        if (newUsersCount > 0) {
            log(INFO, "Successfully wrote " + std::to_string(newUsersCount) + " new users to " + allUsersFile +
                      " (Total users in memory: " + std::to_string(currentUsers.size()) + ")");
        } else {
            log(INFO, "No new users to append to " + allUsersFile +
                      " (All " + std::to_string(currentUsers.size()) + " users already present)");
        }
    } else {
        log(ERROR, "Failed to open users file " + allUsersFile + " for writing");
    }

    // Release semaphore lock
    sem_post(fileSem);
    sem_close(fileSem);
    log(INFO, "Released semaphore lock for " + allUsersFile);
}

// Helper function to add a user to a file (following or followers)
// Format: "username" (timestamp support commented out for now)
void addUserToFile(const std::string& filename, const std::string& username, const std::string& semName) {
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

    if (fileSem == SEM_FAILED) {
        log(ERROR, "Failed to open semaphore " + semName + " for " + filename);
        return;
    }

    sem_wait(fileSem);

    // Check if user already exists in file
    std::ifstream readFile(filename);
    std::set<std::string> existingUsers;
    if (readFile.is_open()) {
        std::string line;
        while (std::getline(readFile, line)) {
            if (!line.empty()) {
                // Just store the username directly (no timestamp parsing needed)
                existingUsers.insert(line);
            }
        }
        readFile.close();
    }

    // Add user if not already present
    if (existingUsers.find(username) == existingUsers.end()) {
        std::ofstream file(filename, std::ios::app);
        if (file.is_open()) {
            file << username << std::endl;
            file.close();
            log(INFO, "Added " + username + " to " + filename);
        }
    }

    sem_post(fileSem);
    sem_close(fileSem);
}

// Helper function to remove a user from a file (following or followers)
void removeUserFromFile(const std::string& filename, const std::string& username, const std::string& semName) {
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

    if (fileSem == SEM_FAILED) {
        log(ERROR, "Failed to open semaphore " + semName + " for " + filename);
        return;
    }

    sem_wait(fileSem);

    // Read all lines except the username to remove
    std::vector<std::string> remainingLines;
    std::ifstream readFile(filename);
    if (readFile.is_open()) {
        std::string line;
        while (std::getline(readFile, line)) {
            if (!line.empty() && line != username) {
                remainingLines.push_back(line);
            }
        }
        readFile.close();
    }

    // Rewrite file without the removed user
    std::ofstream file(filename, std::ios::trunc);
    if (file.is_open()) {
        for (const auto& line : remainingLines) {
            file << line << std::endl;
        }
        file.close();
        log(INFO, "Removed " + username + " from " + filename);
    }

    sem_post(fileSem);
    sem_close(fileSem);
}

// Helper function to read users from a file (returns usernames)
std::vector<std::string> readUsersFromFile(const std::string& filename, const std::string& semName) {
    std::vector<std::string> users;
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

    if (fileSem == SEM_FAILED) {
        log(ERROR, "Failed to open semaphore " + semName + " for " + filename);
        return users;
    }

    sem_wait(fileSem);

    std::ifstream file(filename);
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            if (!line.empty()) {
                // Just read the username directly (no timestamp parsing)
                users.push_back(line);
            }
        }
        file.close();
    }

    sem_post(fileSem);
    sem_close(fileSem);

    return users;
}

// Helper function to check if a user exists in a file
bool userExistsInFile(const std::string& filename, const std::string& username, const std::string& semName) {
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

    if (fileSem == SEM_FAILED) {
        log(ERROR, "Failed to open semaphore " + semName + " for " + filename);
        return false;
    }

    sem_wait(fileSem);

    bool found = false;
    std::ifstream file(filename);
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            if (!line.empty() && line == username) {
                found = true;
                break;
            }
        }
        file.close();
    }

    sem_post(fileSem);
    sem_close(fileSem);

    return found;
}

// Forward declarations
void initializeSlaveConnection();

// Helper function to write a message to a user's timeline file
void writeToTimelineFile(const std::string& username, const Message& message) {
    // Determine synchronizer directory based on role
    std::string syncSubdir = g_isMaster ? "1" : "2";
    std::string syncDir = "./cluster_" + g_clusterID + "/" + syncSubdir + "/";
    std::string filename = syncDir + username + ".txt";

    // Create directory if it doesn't exist
    std::string mkdir_command = "mkdir -p " + syncDir;
    system(mkdir_command.c_str());

    // Use semaphore for synchronization with synchronizer process
    std::string semName = "/" + g_clusterID + "_" + syncSubdir + "_" + username + "_timeline";
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

    if (fileSem == SEM_FAILED) {
        log(ERROR, "Failed to open semaphore " + semName + " for " + filename);
        return;
    }

    // Acquire semaphore lock
    sem_wait(fileSem);

    std::ofstream file(filename, std::ios::app);  // Append mode
    if (file.is_open()) {
        file << "T " << message.timestamp().seconds() << std::endl;
        file << "U " << message.username() << std::endl;
        file << "W " << message.msg() << std::endl;
        file << std::endl;  // Empty line after each message
        file.close();
    }

    // Release semaphore lock
    sem_post(fileSem);
    sem_close(fileSem);
}

// Helper function to read last N messages from a user's timeline file after a specific timestamp
std::vector<Message> readTimelineFile(const std::string& username, int max_messages, std::time_t after_time) {
    std::vector<Message> messages;

    // Determine synchronizer directory based on role
    std::string syncSubdir = g_isMaster ? "1" : "2";
    std::string syncDir = "./cluster_" + g_clusterID + "/" + syncSubdir + "/";
    std::string filename = syncDir + username + ".txt";

    // Use semaphore for synchronization with synchronizer process
    std::string semName = "/" + g_clusterID + "_" + syncSubdir + "_" + username + "_timeline";
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);

    if (fileSem == SEM_FAILED) {
        log(ERROR, "Failed to open semaphore " + semName + " for " + filename);
        return messages;  // Return empty vector if semaphore fails
    }

    // Acquire semaphore lock
    sem_wait(fileSem);

    std::ifstream file(filename);
    if (!file.is_open()) {
        sem_post(fileSem);
        sem_close(fileSem);
        return messages;  // Return empty vector if file doesn't exist
    }

    std::vector<std::string> all_lines;
    std::string line;

    // Read all lines from file
    while (std::getline(file, line)) {
        all_lines.push_back(line);
    }
    file.close();

    // Release semaphore lock
    sem_post(fileSem);
    sem_close(fileSem);

    // Parse messages from the end (most recent first)
    for (int i = all_lines.size() - 1; i >= 0 && messages.size() < max_messages; ) {
        // Skip empty lines at the end
        while (i >= 0 && all_lines[i].empty()) {
            i--;
        }

        if (i < 2) break;  // Need at least 3 lines for a complete message

        // Parse message in reverse order (W, U, T)
        std::string msg_content = "";
        std::string msg_user = "";
        std::time_t msg_time = 0;

        // Parse W line (message content)
        if (i >= 0 && all_lines[i].substr(0, 2) == "W ") {
            msg_content = all_lines[i].substr(2);
            i--;
        } else {
            i--;
            continue;
        }

        // Parse U line (username)
        if (i >= 0 && all_lines[i].substr(0, 2) == "U ") {
            msg_user = all_lines[i].substr(2);
            i--;
        } else {
            i--;
            continue;
        }

        // Parse T line (timestamp)
        if (i >= 0 && all_lines[i].substr(0, 2) == "T ") {
            msg_time = std::stoll(all_lines[i].substr(2));
            i--;
        } else {
            i--;
            continue;
        }

        // Only include messages after the specified time
        if (msg_time >= after_time) {
            Message msg;
            msg.set_username(msg_user);
            msg.set_msg(msg_content);
            google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
            timestamp->set_seconds(msg_time);
            timestamp->set_nanos(0);
            msg.set_allocated_timestamp(timestamp);

            messages.push_back(msg);
        }

        // Skip the empty line
        if (i >= 0 && all_lines[i].empty()) {
            i--;
        }
    }

    // Reverse to get chronological order (oldest first)
    std::reverse(messages.begin(), messages.end());
    return messages;
}


class SNSServiceImpl final : public SNSService::Service {
  
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    std::string username = request->username();

    // Step 1: Add all existing users to the reply (read from synchronizer's all_users file)
    // Use "1" for Master synchronizer, "2" for Slave synchronizer
    std::string syncSubdir = g_isMaster ? "1" : "2";
    std::string allUsersFile = "./cluster_" + g_clusterID + "/" + syncSubdir + "/all_users.txt";
    std::ifstream file(allUsersFile);

    if (file.is_open()) {
        std::string user;
        while (std::getline(file, user)) {
            if (!user.empty()) {
                list_reply->add_all_users(user);
            }
        }
        file.close();
        log(WARNING, "all_users.txt  found, did read from file");
    } else {
        // Fallback to in-memory client_db if file doesn't exist yet
        log(WARNING, "all_users.txt not found, using in-memory client_db");
        for (Client* client : client_db) {
            list_reply->add_all_users(client->username);
        }
    }

    // Step 2: Read the user's followers from file
    std::string followersFile = "./cluster_" + g_clusterID + "/" + syncSubdir + "/" + username + "_followers.txt";
    std::string semName = "/" + g_clusterID + "_" + syncSubdir + "_" + username + "_followers";

    std::vector<std::string> followers = readUsersFromFile(followersFile, semName);
    for (const auto& follower : followers) {
        list_reply->add_followers(follower);
    }

    log(INFO, "List RPC for user " + username + " - returned " +
              std::to_string(followers.size()) + " followers from file");

    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string follower = request->username();          // Who is following
    std::string target = request->arguments(0);          // Who to follow

    std::string role = g_isMaster ? "Master" : "Slave";
    log(INFO, role + " received Follow request: " + follower + " -> " + target);

    // Step 1: Check if user is trying to follow themselves
    if (follower == target) {
        reply->set_msg("FAILURE_ALREADY_EXISTS");
        log(INFO, "User is trying to follow themselves: " + follower);
        return Status::OK;
    }

    // Step 2: Determine synchronizer directory
    std::string syncSubdir = g_isMaster ? "1" : "2";
    std::string syncDir = "./cluster_" + g_clusterID + "/" + syncSubdir + "/";
    std::string allUsersFile = syncDir + "all_users.txt";
    std::string allUsersSem = "/" + g_clusterID + "_" + syncSubdir + "_all_users";

    // Step 3: Check if target user exists by reading all_users.txt
    bool targetExists = userExistsInFile(allUsersFile, target, allUsersSem);
    if (!targetExists) {
        reply->set_msg("FAILURE_INVALID_USERNAME");
        log(INFO, "Target user does not exist: " + target);
        return Status::OK;
    }

    // Step 4: Check if already following by reading follower's following file
    std::string followerFollowingFile = syncDir + follower + "_following.txt";
    std::string followerFollowingSem = "/" + g_clusterID + "_" + syncSubdir + "_" + follower + "_following";

    bool alreadyFollowing = userExistsInFile(followerFollowingFile, target, followerFollowingSem);
    if (alreadyFollowing) {
        reply->set_msg("FAILURE_ALREADY_EXISTS");
        log(INFO, follower + " is already following " + target);
        return Status::OK;
    }

    // OLD CODE - Using in-memory client_db (COMMENTED OUT)
    /*
    // Step 1: Find the target user
    Client* target_client = nullptr;
    for (Client* client : client_db) {
        if (client->username == target) {
            target_client = client;
            break;
        }
    }

    // Step 2: Check if target user exists
    if (target_client == nullptr) {
        reply->set_msg("FAILURE_INVALID_USERNAME");
        log(INFO, "User is trying to follow an invalid username" + target);
        return Status::OK;
    }

    // Step 3: Find the follower user
    Client* follower_client = nullptr;
    for (Client* client : client_db) {
        if (client->username == follower) {
            follower_client = client;
            break;
        }
    }

    // Step 4: Check if already following
    for (Client* existing_follower : target_client->client_followers) {
        if (existing_follower->username == follower) {
            reply->set_msg("FAILURE_ALREADY_EXISTS");
            log(INFO, "User is already following" + follower + " and " + target);
            return Status::OK;
        }
    }

    // Step 5: Add follower relationship
    target_client->client_followers.push_back(follower_client);
    follower_client->client_following.push_back(target_client);

    // Step 6: Record when this follow relationship started
    std::time_t now = time(nullptr);
    target_client->follower_since[follower] = now;     // Target knows when follower started following
    follower_client->following_since[target] = now;    // Follower knows when they started following target
    */

    // Step 5: Write to follower/following files in synchronizer directory
    // Create synchronizer directory if it doesn't exist
    std::string mkdir_command = "mkdir -p " + syncDir;
    system(mkdir_command.c_str());

    // Update target's followers file (target now has follower as a follower)
    std::string targetFollowersFile = syncDir + target + "_followers.txt";
    std::string targetFollowersSem = "/" + g_clusterID + "_" + syncSubdir + "_" + target + "_followers";
    addUserToFile(targetFollowersFile, follower, targetFollowersSem);

    // Update follower's following file (follower is now following target)
    addUserToFile(followerFollowingFile, target, followerFollowingSem);

    log(INFO, "Successfully added follow relationship: " + follower + " -> " + target);

    // Mirror to Slave if we're Master
    if (g_isMaster) {
        if (!slave_stub_) {
            initializeSlaveConnection();
        }

        if (slave_stub_) {
            ClientContext slave_context;
            Reply slave_reply;
            Status slave_status = slave_stub_->Follow(&slave_context, *request, &slave_reply);

            if (slave_status.ok()) {
                log(INFO, "Mirrored Follow to Slave: " + follower + " -> " + target);
            } else {
                log(WARNING, "Failed to mirror Follow to Slave: " + slave_status.error_message());
            }
        }
    }

    reply->set_msg("SUCCESS");
    return Status::OK;
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string follower = request->username();          // Who is unfollowing
    std::string target = request->arguments(0);          // Who to unfollow

    std::string role = g_isMaster ? "Master" : "Slave";
    log(INFO, role + " received Unfollow request: " + follower + " -> " + target);

    // Step 1: Determine synchronizer directory
    std::string syncSubdir = g_isMaster ? "1" : "2";
    std::string syncDir = "./cluster_" + g_clusterID + "/" + syncSubdir + "/";
    std::string allUsersFile = syncDir + "all_users.txt";
    std::string allUsersSem = "/" + g_clusterID + "_" + syncSubdir + "_all_users";

    // Step 2: Check if target user exists by reading all_users.txt
    bool targetExists = userExistsInFile(allUsersFile, target, allUsersSem);
    if (!targetExists) {
        reply->set_msg("FAILURE_INVALID_USERNAME");
        log(INFO, "Target user does not exist: " + target);
        return Status::OK;
    }

    // Step 3: Check if actually following by reading follower's following file
    std::string followerFollowingFile = syncDir + follower + "_following.txt";
    std::string followerFollowingSem = "/" + g_clusterID + "_" + syncSubdir + "_" + follower + "_following";

    bool wasFollowing = userExistsInFile(followerFollowingFile, target, followerFollowingSem);
    if (!wasFollowing) {
        reply->set_msg("FAILURE_NOT_A_FOLLOWER");
        log(INFO, follower + " was not following " + target);
        return Status::OK;
    }

    // OLD CODE - Using in-memory client_db (COMMENTED OUT)
    /*
    // Step 1: Find the target user
    Client* target_client = nullptr;
    for (Client* client : client_db) {
        if (client->username == target) {
            target_client = client;
            break;
        }
    }

    // Step 2: Check if target user exists
    if (target_client == nullptr) {
        reply->set_msg("FAILURE_INVALID_USERNAME");
        return Status::OK;
    }

    // Step 3: Find the follower user
    Client* follower_client = nullptr;
    for (Client* client : client_db) {
        if (client->username == follower) {
            follower_client = client;
            break;
        }
    }

    // Step 4: Check if actually following and remove from target's followers
    bool was_following = false;
    for (auto it = target_client->client_followers.begin(); it != target_client->client_followers.end(); ++it) {
        if ((*it)->username == follower) {
            target_client->client_followers.erase(it);
            was_following = true;
            break;
        }
    }

    // Step 5: If not following, return error
    if (!was_following) {
        reply->set_msg("FAILURE_NOT_A_FOLLOWER");
        return Status::OK;
    }

    // Step 6: Remove from follower's following list
    for (auto it = follower_client->client_following.begin(); it != follower_client->client_following.end(); ++it) {
        if ((*it)->username == target) {
            follower_client->client_following.erase(it);
            break;
        }
    }

    // Step 7: Remove timestamp records
    target_client->follower_since.erase(follower);      // Remove from target's follower timestamps
    follower_client->following_since.erase(target);     // Remove from follower's following timestamps
    */

    // Step 4: Remove from follower/following files in synchronizer directory
    // Remove follower from target's followers file
    std::string targetFollowersFile = syncDir + target + "_followers.txt";
    std::string targetFollowersSem = "/" + g_clusterID + "_" + syncSubdir + "_" + target + "_followers";
    removeUserFromFile(targetFollowersFile, follower, targetFollowersSem);

    // Remove target from follower's following file
    removeUserFromFile(followerFollowingFile, target, followerFollowingSem);

    log(INFO, "Successfully removed follow relationship: " + follower + " -/-> " + target);

    // Mirror to Slave if we're Master
    if (g_isMaster) {
        if (!slave_stub_) {
            initializeSlaveConnection();
        }

        if (slave_stub_) {
            ClientContext slave_context;
            Reply slave_reply;
            Status slave_status = slave_stub_->UnFollow(&slave_context, *request, &slave_reply);

            if (slave_status.ok()) {
                log(INFO, "Mirrored UnFollow to Slave: " + follower + " -> " + target);
            } else {
                log(WARNING, "Failed to mirror UnFollow to Slave: " + slave_status.error_message());
            }
        }
    }

    reply->set_msg("SUCCESS");
    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    std::string username = request->username();
    // Check if user already exists and is connected
    std::string role = g_isMaster ? "Master" : "Slave";
    log(INFO, role + " received login request: " + username);

    for (Client* client : client_db) {
        if (client->username == username && client->connected) {
            // User already logged in - FAIL
            reply->set_msg("FAILURE_ALREADY_EXISTS");
            return Status::OK;
        }
    }
    // User doesn't exist or isn't connected - create/reconnect
    Client* new_client = new Client();
    new_client->username = username;
    new_client->connected = true;
    client_db.push_back(new_client);

    // Mirror to Slave if we're Master
    if (g_isMaster) {
        if (!slave_stub_) {
            initializeSlaveConnection();
        }

        if (slave_stub_) {
            ClientContext slave_context;
            Reply slave_reply;
            Status slave_status = slave_stub_->Login(&slave_context, *request, &slave_reply);

            if (slave_status.ok()) {
                log(INFO, "Mirrored Login to Slave for user: " + username);
            } else {
                log(WARNING, "Failed to mirror Login to Slave: " + slave_status.error_message());
            }
        }
    }
    //writeUsersToSynchronizerFile();  // Write user list for synchronizers to read
    reply->set_msg("SUCCESS");
    writeUsersToSynchronizerFile();
    return Status::OK;
  }

  // RPC MirrorPost - Slave receives timeline posts from Master
  Status MirrorPost(ServerContext* context, const Message* message, Reply* reply) override {
    std::string username = message->username();

    // Write the message to the poster's timeline file
    writeToTimelineFile(username, *message);

    log(INFO, "Slave mirrored post from user: " + username);
    reply->set_msg("SUCCESS");
    return Status::OK;
  }

  Status Timeline(ServerContext* context,
		ServerReaderWriter<Message, Message>* stream) override {

    // Section 1: Get the user entering timeline mode
    // We need to read the first message to identify the user
    Message initial_message;
    if (!stream->Read(&initial_message)) {
        return Status::CANCELLED;  // Client disconnected immediately
    }

    std::string username = initial_message.username();

    // Section 2: Find user and store stream
    Client* user = nullptr;
    for (Client* client : client_db) {
        if (client->username == username) {
            user = client;
            break;
        }
    }

    if (user == nullptr) {
        return Status(grpc::StatusCode::NOT_FOUND, "User not found");
    }

    // Store stream for broadcasting
    user->stream = stream;

    // Section 3: Send initial timeline (last 20 posts from followed users)
    std::vector<Message> initial_timeline;

    // Read who this user follows from the synchronizer file
    std::string syncSubdir = g_isMaster ? "1" : "2";
    std::string followingFile = "./cluster_" + g_clusterID + "/" + syncSubdir + "/" + username + "_following.txt";
    std::string followingSem = "/" + g_clusterID + "_" + syncSubdir + "_" + username + "_following";

    std::vector<std::string> followingList = readUsersFromFile(followingFile, followingSem);

    // For each user that this user follows (read from file)
    for (const auto& followed_username : followingList) {
        // Read posts from followed user (no timestamp filtering since we removed timestamps)
        std::vector<Message> user_messages = readTimelineFile(followed_username, 20, 0);

        // Add to initial timeline
        initial_timeline.insert(initial_timeline.end(), user_messages.begin(), user_messages.end());
    }

    // Sort by timestamp (most recent last) and limit to 20 messages
    std::sort(initial_timeline.begin(), initial_timeline.end(),
              [](const Message& a, const Message& b) {
                  return a.timestamp().seconds() > b.timestamp().seconds();
              });

    // Send last 20 messages to user FIRST
    int start_idx = std::max(0, (int)initial_timeline.size() - 20);
    for (int i = start_idx; i < initial_timeline.size(); i++) {
        stream->Write(initial_timeline[i]);
    }

    // Section 4: Check if initial message has content (is a real post)
    if (!initial_message.msg().empty()) {
        // Save the initial message to the poster's timeline file
        writeToTimelineFile(user->username, initial_message);

        // Mirror post to Slave if we're Master
        if (g_isMaster) {
            if (!slave_stub_) {
                initializeSlaveConnection();
            }

            if (slave_stub_) {
                ClientContext slave_context;
                Reply slave_reply;
                Status slave_status = slave_stub_->MirrorPost(&slave_context, initial_message, &slave_reply);

                if (slave_status.ok()) {
                    log(INFO, "Mirrored initial timeline post to Slave from: " + user->username);
                } else {
                    log(WARNING, "Failed to mirror initial post to Slave: " + slave_status.error_message());
                }
            }
        }

        // Broadcast to followers (read from file)
        std::string followersFile = "./cluster_" + g_clusterID + "/" + syncSubdir + "/" + username + "_followers.txt";
        std::string followersSem = "/" + g_clusterID + "_" + syncSubdir + "_" + username + "_followers";
        std::vector<std::string> followersList = readUsersFromFile(followersFile, followersSem);

        for (const auto& follower_username : followersList) {
            // Find follower client in memory to check if they have an active stream
            Client* follower_client = nullptr;
            for (Client* client : client_db) {
                if (client->username == follower_username) {
                    follower_client = client;
                    break;
                }
            }

            if (follower_client != nullptr && follower_client->stream != nullptr) {
                follower_client->stream->Write(initial_message);
            }
        }
    }

    // Section 6: Main message loop
    Message incoming_message;
    while (stream->Read(&incoming_message)) {
        std::string poster = incoming_message.username();

        // Find the poster
        Client* poster_client = nullptr;
        for (Client* client : client_db) {
            if (client->username == poster) {
                poster_client = client;
                break;
            }
        }

        if (poster_client == nullptr) continue;

        // Save the message to the poster's timeline file
        writeToTimelineFile(poster_client->username, incoming_message);

        // Mirror post to Slave if we're Master
        if (g_isMaster) {
            if (!slave_stub_) {
                initializeSlaveConnection();
            }
            if (slave_stub_) {
                ClientContext slave_context;
                Reply slave_reply;
                Status slave_status = slave_stub_->MirrorPost(&slave_context, incoming_message, &slave_reply);
                if (slave_status.ok()) {
                    log(INFO, "Mirrored timeline post to Slave from: " + poster_client->username);
                } else {
                    log(WARNING, "Failed to mirror post to Slave: " + slave_status.error_message());
                }
            }
        }

        // Broadcast to all followers of the poster (read from file)
        std::string posterFollowersFile = "./cluster_" + g_clusterID + "/" + syncSubdir + "/" + poster + "_followers.txt";
        std::string posterFollowersSem = "/" + g_clusterID + "_" + syncSubdir + "_" + poster + "_followers";
        std::vector<std::string> posterFollowersList = readUsersFromFile(posterFollowersFile, posterFollowersSem);

        for (const auto& follower_username : posterFollowersList) {
            // Find follower client in memory to check if they have an active stream
            Client* follower_client = nullptr;
            for (Client* client : client_db) {
                if (client->username == follower_username) {
                    follower_client = client;
                    break;
                }
            }

            if (follower_client != nullptr && follower_client->stream != nullptr) {
                follower_client->stream->Write(incoming_message);
            }
        }
    }

    // Section 8: Cleanup when user disconnects
    user->stream = nullptr;

    return Status::OK;
  }

};

// Function to initialize connection to coordinator
void initializeCoordinator() {
    std::string coordinator_address = g_coordinatorIP + ":" + g_coordinatorPort;
    coordinator_stub_ = CoordService::NewStub(
        grpc::CreateChannel(coordinator_address, grpc::InsecureChannelCredentials())
    );
    log(INFO, "Connected to Coordinator at " + coordinator_address);
}

// Function to initialize connection to Slave (only called by Master)
void initializeSlaveConnection() {
    if (!g_isMaster) {
        log(INFO, "Not a Master server, skipping slave connection initialization");
        return;
    }

    // Query Coordinator for Slave server info
    ClientContext context;
    ID request;
    request.set_id(std::stoi(g_clusterID));  // Pass cluster ID
    ServerInfo slaveInfo;

    Status status = coordinator_stub_->GetSlave(&context, request, &slaveInfo);

    if (status.ok()) {
        std::string slave_address = slaveInfo.hostname() + ":" + slaveInfo.port();
        slave_stub_ = SNSService::NewStub(
            grpc::CreateChannel(slave_address, grpc::InsecureChannelCredentials())
        );
        log(INFO, "Master connected to Slave at " + slave_address);
    } else {
        log(WARNING, "Failed to get Slave info from Coordinator: " + status.error_message());
        log(WARNING, "Master will operate without Slave mirroring for now");
    }
}

// Function to send heartbeat to coordinator
void sendHeartbeat() {
    // Create ServerInfo message
    ServerInfo info;
    info.set_serverid(std::stoi(g_serverID));
    info.set_hostname(g_coordinatorIP);
    info.set_port(g_serverPort);
    info.set_type("server");
    info.set_clusterid(std::stoi(g_clusterID));

    // Create context and add clusterID metadata (for backward compatibility)
    ClientContext context;
    context.AddMetadata("clusterid", g_clusterID);

    // Send heartbeat RPC
    Confirmation confirmation;
    Status status = coordinator_stub_->Heartbeat(&context, info, &confirmation);

    // Log result
    if (status.ok() && confirmation.status()) {
        // Update role based on Coordinator's response
        g_isMaster = confirmation.ismaster();

        std::string role = g_isMaster ? "Master" : "Slave";
        log(INFO, "Heartbeat sent successfully - " + role + " ClusterID: " + g_clusterID +
                  ", ServerID: " + g_serverID);
    } else {
        log(ERROR, "Heartbeat failed: " + status.error_message());
    }
}



// Background thread that sends heartbeats every 5 seconds
void heartbeatThread() {
    while (true) {
        sendHeartbeat();
        //writeUsersToSynchronizerFile()
        sleep(5);  // Wait 5 seconds before next heartbeat
    }
}

// void fileWriterThread() {
//     while (true) {
//         writeUsersToSynchronizerFile();
//         sleep(10);  // Wait 10 seconds before next write
//     }
// }

void RunServer(std::string port_no) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  server->Wait();
}

int main(int argc, char** argv) {

  std::string port = "3010";
  std::string clusterID = "1";
  std::string serverID = "1";
  std::string coordinatorIP = "localhost";
  std::string coordinatorPort = "9090";

  int opt = 0;
  while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1){
    switch(opt) {
      case 'c':
          clusterID = optarg;
          break;
      case 's':
          serverID = optarg;
          break;
      case 'h':
          coordinatorIP = optarg;
          break;
      case 'k':
          coordinatorPort = optarg;
          break;
      case 'p':
          port = optarg;
          break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }

  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  FLAGS_logtostderr = true;  // Output logs to terminal instead of files
  log(INFO, "Logging Initialized. Server starting...");
  log(INFO, "Server Config - ClusterID: " + clusterID + ", ServerID: " + serverID +
            ", Port: " + port + ", Coordinator: " + coordinatorIP + ":" + coordinatorPort);

  // Store configuration in global variables
  g_clusterID = clusterID;
  g_serverID = serverID;
  g_coordinatorIP = coordinatorIP;
  g_coordinatorPort = coordinatorPort;
  g_serverPort = port;

  // Set up directory structure: ./cluster_<clusterID>/<serverID>/
  g_serverDirectory = "./cluster_" + clusterID + "/" + serverID + "/";

  // Create directory if it doesn't exist
  std::string mkdir_command = "mkdir -p " + g_serverDirectory;
  int mkdir_result = system(mkdir_command.c_str());
  if (mkdir_result == 0) {
    log(INFO, "Server directory created/verified: " + g_serverDirectory);
  } else {
    log(ERROR, "Failed to create server directory: " + g_serverDirectory);
  }

  // Initialize connection to coordinator
  initializeCoordinator();

  // Send initial registration heartbeat
  log(INFO, "Sending registration heartbeat to Coordinator...");
  sendHeartbeat();

  // Start periodic heartbeat thread in background
  std::thread hb_thread(heartbeatThread);
  hb_thread.detach();  // Detach so it runs independently

//   std::thread fw_thread(fileWriterThread);
//   fw_thread.detach();  // Detach so it runs independently
//   log(INFO, "File Writer thread started");

  // Start the server
  RunServer(port);

  return 0;
}
