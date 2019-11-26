/*================================================================
*   
*   File Name:    example_list.cpp
*   Creator:      Xie Zhiyong
*   Create Time:  2019.11.26
*
================================================================*/


#include "redis.h"

#include <iostream>

using namespace std;

void TestList(RedisClient* redis) {
    bool ret;
    string key = "test";
    string value = "value1";
    vector<string> values {"value2", "value3", "value4", "value5"};
    vector<string> out;

    ret = redis->LPush(key, value);
    if (!ret) {
        cout << "LPush Error" << endl;
    }
    else {
        cout << "LPush OK" << endl;
    }

    ret = redis->LPush(key, values);
    if (!ret) {
        cout << "LPush Error" << endl;
    }
    else {
        cout << "LPush OK" << endl;
    }

    value = "";
    ret = redis->LPop(key, value);
    if (!ret) {
        cout << "LPop Error" << endl;
    }
    else {
        cout << "LPop OK value: " << value << endl;
    }

    int length = 0;
    ret = redis->LLen(key, length);
    if (!ret) {
        cout << "LLen Error" << endl;
    }
    else {
        cout << "LLen OK length: " << length << endl;
    }

    value = "";
    ret = redis->LIndex(key, 3, value);
    if (!ret) {
        cout << "LIndex Error" << endl;
    }
    else {
        cout << "LIndex OK value: " << value << endl;
    }

    out.clear();
    ret = redis->LRange(key, 0, -1, out);
    if (!ret) {
        cout << "LRange Error" << endl;
    }
    else {
        cout << "LRange OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            if (i) cout << " ";
            cout << out[i];
        }
        cout << endl;
    }

    ret = redis->LRem(key, 2, "value3");
    if (!ret) {
        cout << "LRem Error" << endl;
    }
    else {
        cout << "LRem OK" << endl;
    }
    
    ret = redis->LTrim(key, 0, 2);
    if (!ret) {
        cout << "LTrim Error" << endl;
    }
    else {
        cout << "LTrim OK" << endl;
    }
    
    out.clear();
    ret = redis->LRange(key, 0, -1, out);
    if (!ret) {
        cout << "LRange Error" << endl;
    }
    else {
        cout << "LRange OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            if (i) cout << " ";
            cout << out[i];
        }
        cout << endl;
    }
    
    value = "test";
    ret = redis->RPush(key, value);
    if (!ret) {
        cout << "RPush Error" << endl;
    }
    else {
        cout << "RPush OK" << endl;
    }

    ret = redis->RPush(key, values);
    if (!ret) {
        cout << "RPush Error" << endl;
    }
    else {
        cout << "RPush OK" << endl;
    }

    value = "";
    ret = redis->RPop(key, value);
    if (!ret) {
        cout << "RPop Error" << endl;
    }
    else {
        cout << "RPop OK value: " << value << endl;
    }

    out.clear();
    ret = redis->LRange(key, 0, -1, out);
    if (!ret) {
        cout << "LRange Error" << endl;
    }
    else {
        cout << "LRange OK" << endl;
        for (int i = 0; i < out.size(); i++) {
            if (i) cout << " ";
            cout << out[i];
        }
        cout << endl;
    }

    ret = redis->Del(key);
    if (!ret) {
        cout << "Del Error" << endl;
    }
    else {
        cout << "Del OK" << endl;
    }
}

int main(int argc, char** argv) {
    string redis_host = "127.0.0.1";
    uint32_t redis_port = 6379;
    RedisClient* redis = new RedisClient(redis_host, redis_port);
    TestList(redis);
    delete redis;
    return 0;
}
