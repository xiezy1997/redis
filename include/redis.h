/*================================================================
*   
*   File Name:    redis.h
*   Creator:      Xie Zhiyong
*   Create Time:  2019.11.22
*
================================================================*/


#ifndef _REDIS_H
#define _REDIS_H

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <hiredis/hiredis.h>

#include <string>
#include <vector>
#include <sstream>

using std::string;
using std::vector;
using std::ostringstream;
using std::istringstream;

enum ErrorLevel {
    EL_OK,
    EL_COMMON_ERROR,
    EL_CONTEXT_FAILURE,
    EL_SERVER_CRASH,
    EL_NONE
};

class RedisClient{
    public:
        RedisClient(const string& host, uint32_t port, const string& password = "", uint32_t database_num = 0, \
                    uint64_t reconnection_interval_usec = 1000 * 1000UL, \
                    uint64_t connection_timeout_usec = 1000 * 1000 * 1000UL, \
                    uint64_t operation_timeout_usec =1000 * 1000 * 1000UL);
        ~RedisClient();
        void SetReconnectionInterval(uint64_t reconnection_interval_usec);
        void SetConnectionTimeout(uint64_t connection_timeout_usec);
        void SetOperationTimeout(uint64_t operation_timeout_usec);
        bool Connect();
        void DisConnect();

    public:
        bool Select(uint32_t database_num);
        bool Del(const string& key);
        bool Del(const vector<string>& keys);
        bool Exists(const string& key, bool& is_exists);

    public:
        bool Set(const string& key, const string& value);
        bool Get(const string& key, string& value);
        bool MSet(const vector<string>& keys, const vector<string>& values);
        bool MGet(const vector<string>& keys, vector<string>& values);
        bool GetSet(const string& key, const string& new_value, string& old_value);
        bool StrLen(const string& key, int& length);
        bool Incr(const string& key);
        bool IncrBy(const string& key, const int& increment);
        bool IncrByFloat(const string& key, const double& increment);
        bool Decr(const string& key);
        bool DecrBy(const string& key, const int& decrement);
        bool Append(const string& key, const string& value);

    public:
        bool HSet(const string& key, const string& field, const string& value);
        bool HGet(const string& key, const string& field, string& value);
        bool HMSet(const string& key, const vector<string>& fields, const vector<string>& values);
        bool HMGet(const string& key, const vector<string>& fields, vector<string>& values);
        bool HLen(const string& key, int& length);
        bool HKeys(const string& key, vector<string>& fields);
        bool HVals(const string& key, vector<string>& values);
        bool HGetAll(const string& key, vector<string>& fields, vector<string>& values);
        bool HExists(const string& key, const string& field, bool& is_exists);
        bool HDel(const string& key, const string& field);
        bool HDel(const string& key, const vector<string>& fields);
        bool HIncrBy(const string& key, const string& field, const int& increment);
        bool HIncrByFloat(const string& key, const string& field, const double& increment);

    public:
        bool LPush(const string& key, const string& value);
        bool LPush(const string& key, const vector<string>& values);
        bool LPop(const string& key, string& value);
        bool LLen(const string& key, int& length);
        bool LIndex(const string& key, const int& index, string& value);
        bool LRange(const string& key, const int& start_idx, const int& end_idx, vector<string>& values);
        bool LRem(const string& key, const int& count, const string& value);
        bool LTrim(const string& key, const int& start_idx, const int& end_idx);
        bool RPush(const string& key, const string& value);
        bool RPush(const string& keys, const vector<string>& values);
        bool RPop(const string& key, string& value);

    public:
        bool SAdd(const string& key, const string& member);
        bool SAdd(const string& key, const vector<string>& members);
        bool SPop(const string& key, string& member);
        bool SCard(const string& key, int& count);
        bool SDiff(const vector<string>& keys, vector<string>& members); // Difference set
        bool SInter(const vector<string>& keys, vector<string>& members); // Intersection set
        bool SUnion(const vector<string>&keys, vector<string>& members); // Union set
        bool SIsMember(const string& key, const string& member, bool& is_member);
        bool SMembers(const string& key, vector<string>& members);
        bool SRem(const string& key, const string& member);
        bool SRem(const string& key, const vector<string>& members);

    public:
        bool ZAdd(const string& key, const double& score, const string& member);
        bool ZAdd(const string& key, const vector<double>& scores, const vector<string>& members);
        bool ZCard(const string& key, int& count);
        bool ZCount(const string& key, const double& min, const double& max, int& count);
        bool ZIncrBy(const string& key, const double& increment, const string& menber);
        bool ZRange(const string& key, const int& start, const int& end, vector<string>& members, vector<double>& scores);
        bool ZRangeByScore(const string& key, const double& min, const double& max, vector<string>& members, vector<double>& scores);
        bool ZRank(const string& key, const string& member, int& index);
        bool ZRem(const string& key, const string& member);
        bool ZRem(const string& key, const vector<string>& members);
        bool ZRemRangeByRank(const string& key, const int& start, const int& end);
        bool ZRemRangeByScore(const string& key, const double& min, const double& max);
        bool ZScore(const string& key, const string& member, double& score);

    private:
        bool ReconnectIfNeed();
        redisReply* CommandArgv(int argc, const char** argv, const size_t* argvlen);
        redisReply* Command(const char* command);
        void MarkServerCrash();
        ErrorLevel GetErrorLevel(const redisReply* reply);
        ErrorLevel GetErrorLevel(const redisContext* context);
        void FreeReply(redisReply** reply);

    private:
        string m_host;
        uint32_t m_port;
        string m_password;
        uint64_t m_connection_timeout_usec;
        uint64_t m_operation_timeout_usec;
        uint64_t m_reconnection_interval_usec;
        struct timeval m_last_server_crash_time;
        uint32_t m_current_database;
        redisContext* m_context;
};

#endif //REDIS_H
