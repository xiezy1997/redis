/*================================================================
*   
*   File Name:    example_string.cpp
*   Creator:      Xie Zhiyong
*   Create Time:  2019.11.26
*
================================================================*/

#include "redis.h"

#include <iostream>

using namespace std;

void TestStringOne(RedisClient* redis) {
    string key = "test";
    string value = "value";
    bool ret = redis->Set(key, value);
    if (!ret) {
        cout << "Set Error" << endl;
    }
    else {
        cout << "Set OK" << endl;
    }
    
    value = "";
    ret = redis->Get(key, value);
    if (!ret) {
        cout << "Get Error" << endl;
    }
    else {
        cout << "Get Value: " << value << endl;
    }
    
    ret = redis->Append(key, value);
    if (!ret) {
        cout << "Append Error" << endl;
    }
    else {
        cout << "Append OK" << endl;
    }
    
    value = "";
    ret = redis->Get(key, value);
    if (!ret) {
        cout << "Get Error" << endl;
    }
    else {
        cout << "Get Value: " << value << endl;
    }
    
    value = "";
    string new_value = "value";
    ret = redis->GetSet(key, new_value, value);
    if (!ret) {
        cout << "GetSet Error" << endl;
    }
    else {
        cout << "GetSet old_value: " << value << endl;
    }
    
    value = "";
    ret = redis->Get(key, value);
    if (!ret) {
        cout << "Get Error" << endl;
    }
    else {
        cout << "Get Value: " << value << endl;
    }
    
    bool is_exists = false;
    ret = redis->Exists(key, is_exists);
    if (!ret) {
        cout << "Exists Error" << endl;
    }
    else {
        cout << "Exists static: " << is_exists << endl;
    }
    
    ret = redis->Del(key);
    if (!ret) {
        cout << "Del Error" << endl;
    }
    else {
        cout << "Del OK" << endl;
    }
    
    is_exists = false;
    ret = redis->Exists(key, is_exists);
    if (!ret) {
        cout << "Exists Error" << endl;
    }
    else {
        cout << "Exists static: " << is_exists << endl;
    }
}

void TestStringMore(RedisClient* redis) {
    vector<string> keys{"key1", "key2", "key3", "num1", "num2", "float"};
    vector<string> values{"value1", "value2", "value3", "4", "4", "4.5"};
    bool ret = redis->MSet(keys, values);
    if (!ret) {
        cout << "MSet Error" << endl;
    }
    else {
        cout << "MSet OK" << endl;
    }
    
    vector<string> out;
    ret = redis->MGet(keys, out);
    if (!ret) {
        cout << "MGet Error" << endl;
    }
    else {
        cout << "MGet OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            cout << keys[i] << " : " << values[i] << " : " << out[i] << endl;
        }
    }

    int length = 0;
    ret = redis->StrLen(keys[0], length);
    if (!ret) {
        cout << "StrLen Error" << endl;
    }
    else {
        cout << "StrLen length: " << length << endl;
    }

    ret = redis->Incr(keys[3]);
    if (!ret) {
        cout << "Incr Error" << endl;
    }
    else {
        cout << "Incr OK" << endl;
    }

    ret = redis->IncrBy(keys[4], 5);
    if (!ret) {
        cout << "IncrBy Error" << endl;
    }
    else {
        cout << "IncrBy OK" << endl;
    }

    ret = redis->IncrByFloat(keys[5], 0.3);
    if (!ret) {
        cout << "IncrByFloat Error" << endl;
    }
    else {
        cout << "IncrByFloat OK" << endl;
    }

    ret = redis->Decr(keys[3]);
    if (!ret) {
        cout << "Decr Error" << endl;
    }
    else {
        cout << "Decr OK" << endl;
    }

    ret = redis->DecrBy(keys[4], 5);
    if (!ret) {
        cout << "DecrBy Error" << endl;
    }
    else {
        cout << "DecrBy OK" << endl;
    }

    out.clear();
    ret = redis->MGet(keys, out);
    if (!ret) {
        cout << "MGet Error" << endl;
    }
    else {
        cout << "MGet OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            cout << keys[i] << " : " << values[i] << " : " << out[i] << endl;
        }
    }

    ret = redis->Del(keys);
    if (!ret) {
        cout << "Del Error" << endl;
    }
    else {
        cout << "Del OK" << endl;
    }

    out.clear();
    ret = redis->MGet(keys, out);
    if (!ret) {
        cout << "MGet Error" << endl;
    }
    else {
        cout << "MGet OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            cout << keys[i] << " : " << values[i] << " : " << out[i] << endl;
        }
    }
}

int main(int argc, char** argv) {
    string redis_host = "127.0.0.1";
    uint32_t redis_port = 6379;
    RedisClient* redis = new RedisClient(redis_host, redis_port);
    TestStringOne(redis);
    TestStringMore(redis);
    delete redis;
    return 0;
}
