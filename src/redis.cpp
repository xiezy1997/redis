/*================================================================
*   
*   File Name:    redis.cpp
*   Creator:      Xie Zhiyong
*   Create Time:  2019.11.23
*
================================================================*/


#include "redis.h"
#include <iostream>

// public <command>
RedisClient::RedisClient(
        const string& host,
        uint32_t port,
        const string& password,
        uint32_t database_num,
        uint64_t reconnection_interval_usec,
        uint64_t connection_timeout_usec,
        uint64_t operation_timeout_usec)
    :
        m_host(host),
        m_port(port),
        m_password(password),
        m_connection_timeout_usec(connection_timeout_usec),
        m_operation_timeout_usec(operation_timeout_usec),
        m_reconnection_interval_usec(reconnection_interval_usec),
        m_current_database(database_num),
        m_context(NULL) {
            m_last_server_crash_time.tv_sec = 0;
            m_last_server_crash_time.tv_usec = 0;
        }

RedisClient::~RedisClient() {
    DisConnect();
    if (m_context != NULL) {
        m_context = NULL;
    }
}

void RedisClient::SetReconnectionInterval(uint64_t reconnection_interval_usec) {
    m_reconnection_interval_usec = reconnection_interval_usec;
}

void RedisClient::SetConnectionTimeout(uint64_t connection_timeout_usec) {
    m_connection_timeout_usec = connection_timeout_usec;
}

void RedisClient::SetOperationTimeout(uint64_t operation_timeout_usec) {
    m_operation_timeout_usec = operation_timeout_usec;
}

bool RedisClient::Connect() {
    if (NULL != m_context) {
        return true;
    }

    ErrorLevel error_level = EL_NONE;

    if (0 == m_connection_timeout_usec) {
        m_context = redisConnect(m_host.c_str(), m_port);
    }
    else {
        struct timeval connection_timeout;
        connection_timeout.tv_sec = m_connection_timeout_usec / 1000000UL;
        connection_timeout.tv_usec = m_connection_timeout_usec % 1000000UL;

        m_context = redisConnectWithTimeout(m_host.c_str(), m_port, connection_timeout);
    }

    error_level = GetErrorLevel(m_context);
    if (EL_SERVER_CRASH == error_level) {
        MarkServerCrash();
        return false;
    }
    else if (EL_CONTEXT_FAILURE == error_level) {
        DisConnect();
        return false;
    }

    if (0 != m_operation_timeout_usec) {
        struct timeval operation_timeout;
        operation_timeout.tv_sec = m_operation_timeout_usec / 1000000UL;
        operation_timeout.tv_usec = m_operation_timeout_usec % 1000000UL;

        if (REDIS_OK != redisSetTimeout(m_context, operation_timeout)) {
            DisConnect();

            return false;
        }
    }

    if (0 != m_password.length()) {
        redisReply* reply = (redisReply*)redisCommand(m_context, "AUTH %s", m_password.c_str());
        error_level = GetErrorLevel(reply);

        if (EL_SERVER_CRASH == error_level) {
            MarkServerCrash();
            FreeReply(&reply);
            return false;
        }

        if (EL_OK != error_level || REDIS_REPLY_STATUS != reply->type) {
            DisConnect();
            FreeReply(&reply);
            return false;
        }

        FreeReply(&reply);
    }

    if (0 != m_current_database) {
        redisReply* reply = (redisReply*)redisCommand(m_context, "SELECT %u", m_current_database);
        error_level = GetErrorLevel(reply);

        if (EL_SERVER_CRASH == error_level) {
            MarkServerCrash();
            FreeReply(&reply);
            return false;
        }

        if (EL_OK != error_level || REDIS_REPLY_STATUS != reply->type) {
            DisConnect();
            FreeReply(&reply);
            return false;
        }

        FreeReply(&reply);
    }

    return true;
}

void RedisClient::DisConnect() {
    if (NULL != m_context) {
        redisFree(m_context);
        m_context = NULL;
    }
}


// public <keys>
bool RedisClient::Select(uint32_t database_num) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SELECT";
    argvlen[0] = sizeof("SELECT") - 1;
   
    ostringstream oss;
    oss << database_num;
    string database_num_string = oss.str();
    
    argv[1] = database_num_string.c_str();
    argvlen[1] = database_num_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STATUS != reply->type) {
        DisConnect();
        FreeReply(&reply);

        return false;
    }
    m_current_database = database_num;
    FreeReply(&reply);
    return true;
}

bool RedisClient::Del(const string& key) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "DEL";
    argvlen[0] = sizeof("DEL") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::Del(const vector<string>& keys) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = keys.size() + 1;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "DEL";
    argvlen[0] = sizeof("DEL") - 1;

    for (int i = 1; i < argc; i++) {
        argv[i] = keys[i - 1].c_str();
        argvlen[i] = keys[i - 1].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::Exists(const string& key, bool& is_exists) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "EXISTS";
    argvlen[0] = sizeof("EXISTS") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    is_exists = reply->integer == 1 ? true : false;

    FreeReply(&reply);
    return true;
}


// public <string>
bool RedisClient::Set(const string& key, const string& value) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SET";
    argvlen[0] = sizeof("SET") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = value.c_str();
    argvlen[2] = value.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STATUS != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}


bool RedisClient::Get(const string& key, string& value) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "GET";
    argvlen[0] = sizeof("GET") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STRING != reply->type && REDIS_REPLY_NIL != reply->type) {
        FreeReply(&reply);
        return false;
    }

    value = "";
    if (REDIS_REPLY_STRING == reply->type) {
        value.assign(reply->str, reply->len);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::MSet(const vector<string>& keys, const vector<string>& values) {
    if (!ReconnectIfNeed()) {
        return false;
    }
    
    int len = keys.size();

    if (len != values.size()) {
        return false;
    }

    int argc = len * 2 + 1;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "MSET";
    argvlen[0] = sizeof("MSET") - 1;

    for (int i = 0; i < len; i++) {
        argv[i * 2 + 1] = keys[i].c_str();
        argvlen[i * 2 + 1] = keys[i].length();
        argv[i * 2 + 2] = values[i].c_str();
        argvlen[i * 2 + 2] = values[i].length();
    }

    redisReply * reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STATUS != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::MGet(const vector<string>& keys, vector<string>& values) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = keys.size() + 1;
    const char* argv[argc];
    size_t argvlen[argc];
   
    argv[0] = "MGET";
    argvlen[0] = sizeof("MGET") - 1;

    for (int i = 1; i < argc; i++) {
        argv[i] = keys[i - 1].c_str();
        argvlen[i] = keys[i - 1].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string tmp_value = "";
    for (int i = 0; i < reply->elements; i++) {
        redisReply* childReply = reply->element[i];
        tmp_value = "";
        if (childReply->type == REDIS_REPLY_STRING) {
            tmp_value.assign(childReply->str, childReply->len);
        }
        values.push_back(tmp_value);
    }
    
    FreeReply(&reply);
    return true;
}

bool RedisClient::GetSet(const string& key, const string& new_value, string& old_value){
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "GETSET";
    argvlen[0] = sizeof("GETSET") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = new_value.c_str();
    argvlen[2] = new_value.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STRING != reply->type && REDIS_REPLY_NIL != reply->type) {
        FreeReply(&reply);
        return false;
    }

    old_value = "";
    if(REDIS_REPLY_STRING == reply->type) {
        old_value.assign(reply->str, reply->len);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::StrLen(const string& key, int& length) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "STRLEN";
    argvlen[0] = sizeof("STRLEN") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        length = 0;
        FreeReply(&reply);
        return false;
    }

    length = reply->integer;

    FreeReply(&reply);
    return true;
}

bool RedisClient::Incr(const string& key) {
    if (!ReconnectIfNeed()) {
        return false;
    }
    
    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "INCR";
    argvlen[0] = sizeof("INCR") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::IncrBy(const string& key, const int& increment) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "INCRBY";
    argvlen[0] = sizeof("INCRBY") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    oss << increment;
    string increment_string = oss.str();
    
    argv[2] = increment_string.c_str();
    argvlen[2] = increment_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::IncrByFloat(const string& key, const double& increment) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "INCRBYFLOAT";
    argvlen[0] = sizeof("INCRBYFLOAT") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    oss << increment;
    string increment_string = oss.str();

    argv[2] = increment_string.c_str();
    argvlen[2] = increment_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STRING != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::Decr(const string& key) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "DECR";
    argvlen[0] = sizeof("DECR") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::DecrBy(const string& key, const int& decrement) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "DECRBY";
    argvlen[0] = sizeof("DECRBY") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    oss << decrement;
    string decrement_string = oss.str();

    argv[2] = decrement_string.c_str();
    argvlen[2] = decrement_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::Append(const string& key, const string& value) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "APPEND";
    argvlen[0] = sizeof("APPEND") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = value.c_str();
    argvlen[2] = value.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}


// public <hash>
bool RedisClient::HSet(const string& key, const string& field, const string& value) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HSET";
    argvlen[0] = sizeof("HSET") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = field.c_str();
    argvlen[2] = field.length();

    argv[3] = value.c_str();
    argvlen[3] = value.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::HGet(const string& key, const string& field, string& value) {
    if (!ReconnectIfNeed()) {
        return false;
    }
    
    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HGET";
    argvlen[0] = sizeof("HGET") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = field.c_str();
    argvlen[2] = field.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STRING != reply->type && REDIS_REPLY_NIL != reply->type) {
        FreeReply(&reply);
        return false;
    }

    value = "";
    if (REDIS_REPLY_STRING == reply->type) {
        value.assign(reply->str, reply->len);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::HMSet(const string& key, const vector<string>& fields, const vector<string>& values) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int length = fields.size();
    if (length != values.size()) {
        return false;
    }

    int argc = length * 2 + 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HMSET";
    argvlen[0] = sizeof("HMSET") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    for (int i = 0; i < length; i++) {
        argv[i * 2 + 2] = fields[i].c_str();
        argvlen[i * 2 + 2] = fields[i].length();
        argv[i * 2 + 3] = values[i].c_str();
        argvlen[i * 2 + 3] = values[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STATUS != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::HMGet(const string& key, const vector<string>& fields, vector<string>& values) {
    if  (!ReconnectIfNeed()) {
        return false;
    }

    int argc = fields.size() + 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HMGET";
    argvlen[0] = sizeof("HMGET") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    for (int i = 0; i < argc - 2; i++) {
        argv[i + 2] = fields[i].c_str();
        argvlen[i + 2] = fields[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string tmp_value;
    for (int i = 0; i < reply->elements; i++) {
        tmp_value = "";
        redisReply* childReply = reply->element[i];
        if (REDIS_REPLY_STRING == childReply->type) {
            tmp_value.assign(childReply->str, childReply->len);
        }
        values.push_back(tmp_value);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::HLen(const string& key, int& length) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HLEN";
    argvlen[0] = sizeof("HLEN") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    length = reply->integer;

    FreeReply(&reply);
    return true;
}

bool RedisClient::HKeys(const string& key, vector<string>& fields) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HKEYS";
    argvlen[0] = sizeof("HKEYS") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }
    
    string tmp_field;
    for (int i = 0; i < reply->elements; i++) {
        tmp_field = "";
        redisReply* childReply = reply->element[i];
        if (REDIS_REPLY_STRING == childReply->type) {
            tmp_field.assign(childReply->str, childReply->len);
        }
        fields.push_back(tmp_field);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::HVals(const string& key, vector<string>& values) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HVALS";
    argvlen[0] = sizeof("HVALS") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string tmp_value;
    for (int i = 0; i < reply->elements; i++) {
        tmp_value = "";
        redisReply* childReply = reply->element[i];
        if (REDIS_REPLY_STRING == childReply->type) {
            tmp_value.assign(childReply->str, childReply->len);
        }
        values.push_back(tmp_value);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::HGetAll(const string& key, vector<string>& fields, vector<string>& values) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HGETALL";
    argvlen[0] = sizeof("HGETALL") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string tmp;
    for (int i = 0; i < reply->elements; i++) {
        tmp = "";
        redisReply*childReply = reply->element[i];
        if (REDIS_REPLY_STRING == childReply->type) {
            tmp.assign(childReply->str, childReply->len);
        }
        if (i & 1) {
            values.push_back(tmp);
        }
        else {
            fields.push_back(tmp);
        }
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::HExists(const string& key, const string& field, bool& is_exists) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HEXISTS";
    argvlen[0] = sizeof("HEXISTS") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = field.c_str();
    argvlen[2] = field.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    is_exists = reply->integer == 1 ? true : false;

    FreeReply(&reply);
    return true;
}

bool RedisClient::HDel(const string& key, const string& field) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HDEL";
    argvlen[0] = sizeof("HDEL") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = field.c_str();
    argvlen[2] = field.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::HDel(const string& key, const vector<string>& fields) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = fields.size() + 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HDEL";
    argvlen[0] = sizeof("HDEL") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    for (int i = 0; i < argc - 2; i++) {
        argv[i + 2] = fields[i].c_str();
        argvlen[i + 2] = fields[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::HIncrBy(const string& key, const string& field, const int& increment) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HINCRBY";
    argvlen[0] = sizeof("HINCRBY") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = field.c_str();
    argvlen[2] = field.length();

    ostringstream oss;
    oss << increment;
    string increment_string = oss.str();

    argv[3] = increment_string.c_str();
    argvlen[3] = increment_string.length();
    
    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::HIncrByFloat(const string& key, const string& field,const double& increment) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "HINCRBYFLOAT";
    argvlen[0] = sizeof("HINCRBYFLOAT") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = field.c_str();
    argvlen[2] = field.length();

    ostringstream oss;
    oss << increment;
    string increment_string = oss.str();

    argv[3] = increment_string.c_str();
    argvlen[3] = increment_string.length();
    
    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STRING != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}


// public <list>
bool RedisClient::LPush(const string& key, const string& value) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "LPUSH";
    argvlen[0] = sizeof("LPUSH") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = value.c_str();
    argvlen[2] = value.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::LPush(const string& key, const vector<string>& values) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = values.size() + 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "LPUSH";
    argvlen[0] = sizeof("LPUSH") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    for (int i = 0; i < argc - 2; i++) {
        argv[i + 2] = values[i].c_str();
        argvlen[i + 2] = values[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::LPop(const string& key, string& value) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "LPOP";
    argvlen[0] = sizeof("LPOP") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STRING != reply->type && REDIS_REPLY_NIL != reply->type) {
        FreeReply(&reply);
        return false;
    }
    
    value = "";
    if (REDIS_REPLY_STRING == reply->type) {
        value.assign(reply->str, reply->len);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::LLen(const string& key, int& length) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "LLEN";
    argvlen[0] = sizeof("LLEN") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }
    
    length = reply->integer;

    FreeReply(&reply);
    return true;
}

bool RedisClient::LIndex(const string& key, const int& index, string& value) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "LINDEX";
    argvlen[0] = sizeof("LINDEX") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    oss << index;
    string index_string = oss.str();

    argv[2] = index_string.c_str();
    argvlen[2] = index_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STRING != reply->type && REDIS_REPLY_NIL != reply->type) {
        FreeReply(&reply);
        return false;
    }

    value = "";
    if (REDIS_REPLY_STRING == reply->type) {
        value.assign(reply->str, reply->len);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::LRange(const string& key, const int& start_idx, const int& end_idx, vector<string>& values) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "LRANGE";
    argvlen[0] = sizeof("LRANGE") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    oss << start_idx;
    string start_string = oss.str();
    
    argv[2] = start_string.c_str();
    argvlen[2] = start_string.length();

    oss.str("");
    oss << end_idx;
    string end_string = oss.str();

    argv[3] = end_string.c_str();
    argvlen[3] = end_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string tmp_value;
    for (int i = 0; i < reply->elements; i++) {
        tmp_value = "";
        redisReply* childReply = reply->element[i];
        if (REDIS_REPLY_STRING == childReply->type) {
            tmp_value.assign(childReply->str, childReply->len);
        }
        values.push_back(tmp_value);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::LRem(const string& key, const int& count, const string& value) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "LREM";
    argvlen[0] = sizeof("LREM") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    oss << count;
    string count_string = oss.str();

    argv[2] = count_string.c_str();
    argvlen[2] = count_string.length();

    argv[3] = value.c_str();
    argvlen[3] = value.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::LTrim(const string& key, const int& start_idx, const int& end_idx) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "LTRIM";
    argvlen[0] = sizeof("LTRIM") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    oss << start_idx;
    string start_string = oss.str();
    
    argv[2] = start_string.c_str();
    argvlen[2] = start_string.length();

    oss.str("");
    oss << end_idx;
    string end_string = oss.str();

    argv[3] = end_string.c_str();
    argvlen[3] = end_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STATUS != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::RPush(const string& key, const string& value) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "RPUSH";
    argvlen[0] = sizeof("RPUSH") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = value.c_str();
    argvlen[2] = value.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::RPush(const string& key, const vector<string>& values) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = values.size() + 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "RPUSH";
    argvlen[0] = sizeof("RPUSH") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    for (int i = 0; i < argc - 2; i++) {
        argv[i + 2] = values[i].c_str();
        argvlen[i + 2] = values[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::RPop(const string& key, string& value) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "RPOP";
    argvlen[0] = sizeof("RPOP") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STRING != reply->type && REDIS_REPLY_NIL != reply->type) {
        FreeReply(&reply);
        return false;
    }
    
    value = "";
    if (REDIS_REPLY_STRING == reply->type) {
        value.assign(reply->str, reply->len);
    }

    FreeReply(&reply);
    return true;
}


// public <set>
bool RedisClient::SAdd(const string& key, const string& member) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SADD";
    argvlen[0] = sizeof("SADD") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = member.c_str();
    argvlen[2] = member.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::SAdd(const string& key, const vector<string>& members) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = members.size() + 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SADD";
    argvlen[0] = sizeof("SADD") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    for (int i = 0; i < argc - 2; i++) {
        argv[i + 2] = members[i].c_str();
        argvlen[i + 2] = members[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}
bool RedisClient::SPop(const string& key, string& member) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SPOP";
    argvlen[0] = sizeof("SPOP") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STRING != reply->type && REDIS_REPLY_NIL != reply->type) {
        FreeReply(&reply);
        return false;
    }

    member = "";
    if (REDIS_REPLY_STRING == reply->type) {
        member.assign(reply->str, reply->len);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::SCard(const string& key, int& count) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SCARD";
    argvlen[0] = sizeof("SCARD") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    count = reply->integer;

    FreeReply(&reply);
    return true;
}

bool RedisClient::SDiff(const vector<string>& keys, vector<string>& members) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = keys.size() + 1;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SDIFF";
    argvlen[0] = sizeof("SDIFF") - 1;

    for (int i = 0; i < argc - 1; i++) {
        argv[i + 1] = keys[i].c_str();
        argvlen[i + 1] = keys[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string tmp_member;
    for (int i = 0; i < reply->elements; i++) {
        tmp_member = "";
        redisReply* childReply = reply->element[i];
        if (REDIS_REPLY_STRING == childReply->type) {
            tmp_member.assign(childReply->str, childReply->len);
        }
        members.push_back(tmp_member);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::SInter(const vector<string>& keys, vector<string>& members) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = keys.size() + 1;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SINTER";
    argvlen[0] = sizeof("SINTER") - 1;

    for (int i = 0; i < argc - 1; i++) {
        argv[i + 1] = keys[i].c_str();
        argvlen[i + 1] = keys[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string tmp_member;
    for (int i = 0; i < reply->elements; i++) {
        tmp_member = "";
        redisReply* childReply = reply->element[i];
        if (REDIS_REPLY_STRING == childReply->type) {
            tmp_member.assign(childReply->str, childReply->len);
        }
        members.push_back(tmp_member);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::SUnion(const vector<string>& keys, vector<string>& members) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = keys.size() + 1;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SUNION";
    argvlen[0] = sizeof("SUNION") - 1;

    for (int i = 0; i < argc - 1; i++) {
        argv[i + 1] = keys[i].c_str();
        argvlen[i + 1] = keys[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string tmp_member;
    for (int i = 0; i < reply->elements; i++) {
        tmp_member = "";
        redisReply* childReply = reply->element[i];
        if (REDIS_REPLY_STRING == childReply->type) {
            tmp_member.assign(childReply->str, childReply->len);
        }
        members.push_back(tmp_member);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::SIsMember(const string& key, const string& member, bool& is_member) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SISMEMBER";
    argvlen[0] = sizeof("SISMEMBER") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = member.c_str();
    argvlen[2] = member.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    is_member = reply->integer == 1 ? true : false;

    FreeReply(&reply);
    return true;
}

bool RedisClient::SMembers(const string& key, vector<string>& members) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SMEMBERS";
    argvlen[0] = sizeof("SMEMBERS") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string tmp_member;
    for (int i = 0; i < reply->elements; i++) {
        tmp_member = "";
        redisReply* childReply = reply->element[i];
        if (REDIS_REPLY_STRING == childReply->type) {
            tmp_member.assign(childReply->str, childReply->len);
        }
        members.push_back(tmp_member);
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::SRem(const string& key, const string& member) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SREM";
    argvlen[0] = sizeof("SREM") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = member.c_str();
    argvlen[2] = member.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::SRem(const string& key, const vector<string>& members) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = members.size() + 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "SREM";
    argvlen[0] = sizeof("SREM") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    for (int i = 0; i < argc - 2; i++) {
        argv[i + 2] = members[i].c_str();
        argvlen[i + 2] = members[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}


// public <zset>
bool RedisClient::ZAdd(const string& key, const double& score, const string& member) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZADD";
    argvlen[0] = sizeof("ZADD") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    string score_string;
    oss << score;
    score_string = oss.str();
    
    argv[2] = score_string.c_str();
    argvlen[2] = score_string.length();

    argv[3] = member.c_str();
    argvlen[3] = member.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::ZAdd(const string& key, const vector<double>& scores, const vector<string>& members){
    if (!ReconnectIfNeed()) {
        return false;
    }

    int length = scores.size();

    if (length != members.size()) {
        return false;
    }

    int argc = length * 2 + 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZADD";
    argvlen[0] = sizeof("ZADD") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    string score_string;
    
    for(int i = 0; i < length; i++) {
        oss.str("");
        oss << scores[i];
        score_string = oss.str();

        argv[i * 2 + 2] = score_string.c_str();
        argvlen[i * 2 + 2] = score_string.length();

        argv[i * 2 + 3] = members[i].c_str();
        argvlen[i * 2 + 3] = members[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;

}

bool RedisClient::ZCard(const string& key, int& count){
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZCARD";
    argvlen[0] = sizeof("ZCARD") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    count = reply->integer;

    FreeReply(&reply);
    return true;
}
bool RedisClient::ZCount(const string& key, const double& min, const double& max, int& count) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZCOUNT";
    argvlen[0] = sizeof("ZCOUNT") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    string min_string, max_string;

    oss.str("");
    oss << min;
    min_string = oss.str();

    argv[2] = min_string.c_str();
    argvlen[2] = min_string.length();
    
    oss.str("");
    oss << max;
    max_string = oss.str();

    argv[3] = max_string.c_str();
    argvlen[3] = max_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    count = reply->integer;

    FreeReply(&reply);
    return true;
}
bool RedisClient::ZIncrBy(const string& key, const double& increment, const string& member){
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZINCRBY";
    argvlen[0] = sizeof("ZINCRBY") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    string increment_string;
    oss << increment;
    increment_string = oss.str();

    argv[2] = increment_string.c_str();
    argvlen[2] = increment_string.length();

    argv[3] = member.c_str();
    argvlen[3] = member.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STRING != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::ZRange(const string& key, const int& start, const int& end, vector<string>& members, vector<double>& scores) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZRANGE";
    argvlen[0] = sizeof("ZRANGE") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    string start_string, end_string;
    
    oss.str("");
    oss << start;
    start_string = oss.str();

    oss.str("");
    oss << end;
    end_string = oss.str();

    argv[2] = start_string.c_str();
    argvlen[2] = start_string.length();

    argv[3] = end_string.c_str();
    argvlen[3] = end_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string tmp;
    for (int i = 0; i < reply->elements; i++) {
        tmp = "";
        redisReply* childReply = reply->element[i];
        if (REDIS_REPLY_STRING == childReply->type) {
            tmp.assign(childReply->str, childReply->len);
        }
        if (i & 1) {
            double score;
            istringstream iss(tmp);
            iss >> score;
            scores.push_back(score);
        }
        else {
            members.push_back(tmp);
        }
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::ZRangeByScore(const string& key, const double& min, const double& max, vector<string>& members, vector<double>& scores) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZRANGEBYSCORE";
    argvlen[0] = sizeof("ZRANGEBYSCORE") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    string min_string, max_string;
    
    oss.str("");
    oss << min;
    min_string = oss.str();

    oss.str("");
    oss << max;
    max_string = oss.str();

    argv[2] = min_string.c_str();
    argvlen[2] = min_string.length();

    argv[3] = max_string.c_str();
    argvlen[3] = max_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_ARRAY != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string tmp;
    for (int i = 0; i < reply->elements; i++) {
        tmp = "";
        redisReply* childReply = reply->element[i];
        if (REDIS_REPLY_STRING == childReply->type) {
            tmp.assign(childReply->str, childReply->len);
        }
        if (i & 1) {
            double score;
            istringstream iss(tmp);
            iss >> score;
            scores.push_back(score);
        }
        else {
            members.push_back(tmp);
        }
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::ZRank(const string& key, const string& member, int& index) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZRANK";
    argvlen[0] = sizeof("ZRANK") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = member.c_str();
    argvlen[2] = member.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    index = reply->integer;

    FreeReply(&reply);
    return true;
}

bool RedisClient::ZRem(const string& key, const string& member) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZREM";
    argvlen[0] = sizeof("ZREM") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = member.c_str();
    argvlen[2] = member.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}
bool RedisClient::ZRem(const string& key, const vector<string>& members) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = members.size() + 2;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZREM";
    argvlen[0] = sizeof("ZREM") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();
    
    for (int i = 0; i < argc - 2; i++) {
        argv[i + 2] = members[i].c_str();
        argvlen[i + 2] = members[i].length();
    }

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}
bool RedisClient::ZRemRangeByRank(const string& key, const int& start, const int& end) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZREMRANGEBYRANK";
    argvlen[0] = sizeof("ZREMRANGEBYRANK") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    string start_string, end_string;
    
    oss.str("");
    oss << start;
    start_string = oss.str();

    oss.str("");
    oss << end;
    end_string = oss.str();

    argv[2] = start_string.c_str();
    argvlen[2] = start_string.length();

    argv[3] = end_string.c_str();
    argvlen[3] = end_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::ZRemRangeByScore(const string& key, const double& min, const double& max) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 4;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZREMRANGEBYSCORE";
    argvlen[0] = sizeof("ZREMRANGEBYSCORE") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    ostringstream oss;
    string min_string, max_string;
    
    oss.str("");
    oss << min;
    min_string = oss.str();

    oss.str("");
    oss << max;
    max_string = oss.str();

    argv[2] = min_string.c_str();
    argvlen[2] = min_string.length();

    argv[3] = max_string.c_str();
    argvlen[3] = max_string.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_INTEGER != reply->type) {
        FreeReply(&reply);
        return false;
    }

    FreeReply(&reply);
    return true;
}

bool RedisClient::ZScore(const string& key, const string& member, double& score) {
    if (!ReconnectIfNeed()) {
        return false;
    }

    int argc = 3;
    const char* argv[argc];
    size_t argvlen[argc];

    argv[0] = "ZSCORE";
    argvlen[0] = sizeof("ZSCORE") - 1;

    argv[1] = key.c_str();
    argvlen[1] = key.length();

    argv[2] = member.c_str();
    argvlen[2] = member.length();

    redisReply* reply = CommandArgv(argc, &argv[0], argvlen);
    if (NULL == reply) {
        return false;
    }

    if (REDIS_REPLY_STRING != reply->type) {
        FreeReply(&reply);
        return false;
    }

    string score_string;
    score_string.assign(reply->str, reply->len);
    istringstream iss(score_string);
    iss >> score;

    FreeReply(&reply);
    return true;
}


// private
bool RedisClient::ReconnectIfNeed() {
    if (NULL != m_context) {
        return true;
    }

    struct timeval current_time;
    gettimeofday(&current_time, NULL);

    uint64_t interval_usec = (current_time.tv_sec - m_last_server_crash_time.tv_sec) * 1000000UL + (current_time.tv_usec - m_last_server_crash_time.tv_usec);

    if (interval_usec > m_reconnection_interval_usec && Connect()) {
        return true;
    }

    return false;
}

redisReply* RedisClient::CommandArgv(int argc, const char** argv, const size_t* argvlen) {
    ErrorLevel error_level = EL_NONE;
    redisReply* reply = NULL;

    reply = (redisReply*)redisCommandArgv(m_context, argc, argv, argvlen);
    error_level = GetErrorLevel(reply);
    if (EL_OK == error_level) {
        return reply;
    }
    else if (EL_COMMON_ERROR == error_level) {
        FreeReply(&reply);

        return NULL;
    }

    //retry
    FreeReply(&reply);
    DisConnect();

    if (!Connect()) {
        return NULL;
    }

    reply = (redisReply*)redisCommandArgv(m_context, argc, argv, argvlen);
    error_level = GetErrorLevel(reply);
    if (EL_SERVER_CRASH == error_level) {
        MarkServerCrash();
    }
    else if (EL_CONTEXT_FAILURE == error_level) {
        DisConnect();
    }

    if (EL_OK != error_level) {
        FreeReply(&reply);

        return NULL;
    }

    return reply;
}

redisReply* RedisClient::Command(const char* command) {
    ErrorLevel error_level = EL_NONE;
    redisReply* reply = NULL;

    reply = (redisReply*)redisCommand(m_context, command);
    error_level = GetErrorLevel(reply);
    if (EL_OK == error_level) {
        return reply;
    }
    else if (EL_COMMON_ERROR == error_level) {
        FreeReply(&reply);
        return NULL;
    }

    //retry
    FreeReply(&reply);
    DisConnect();

    if (!Connect()) {
        return NULL;
    }

    reply = (redisReply*)redisCommand(m_context, command);
    error_level = GetErrorLevel(reply);
    if (EL_SERVER_CRASH == error_level) {
        MarkServerCrash();
    }
    else if (EL_CONTEXT_FAILURE == error_level) {
        DisConnect();
    }

    if (EL_OK != error_level) {
        FreeReply(&reply);

        return NULL;
    }
    return reply;
}

void RedisClient::MarkServerCrash() {
    gettimeofday(&m_last_server_crash_time, NULL);
    DisConnect();
}

ErrorLevel RedisClient::GetErrorLevel(const redisReply* reply) {
    if (NULL == reply) {
        if (REDIS_ERR_IO == m_context->err || REDIS_ERR_EOF == m_context->err) {
            return EL_SERVER_CRASH;
        }
        else {
            return EL_CONTEXT_FAILURE;
        }
    }
    else {
        if (REDIS_REPLY_ERROR == reply->type) {
            return EL_COMMON_ERROR;
        }
        else if (REDIS_REPLY_STATUS == reply->type && 0 != strcasecmp(reply->str, "OK")) {
            return EL_COMMON_ERROR;
        }
        else {
            return EL_OK;
        }
    }

    return EL_CONTEXT_FAILURE;
}

ErrorLevel RedisClient::GetErrorLevel(const redisContext* context) {
    if (NULL == context) {
        return EL_CONTEXT_FAILURE;
    }
    else {
        if (REDIS_OK == context->err) {
            return EL_OK;
        }
        else if (REDIS_ERR_IO == context->err || REDIS_ERR_EOF == context->err) {
            return EL_SERVER_CRASH;
        }
        else {
            return EL_CONTEXT_FAILURE;
        }
    }

    return EL_CONTEXT_FAILURE;
}

void RedisClient::FreeReply(redisReply** reply) {
    if (NULL != reply && NULL != *reply) {
        freeReplyObject((void*)*reply);
        *reply = NULL;
    }
}
