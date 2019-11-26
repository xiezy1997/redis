/*================================================================
*   
*   File Name:    example_zset.cpp
*   Creator:      Xie Zhiyong
*   Create Time:  2019.11.26
*
================================================================*/

#include "redis.h"

#include <iostream>

using namespace std;

void TestZSet(RedisClient* redis) {
    string key = "test";
    string member = "member";
    double score = 1.5;
    bool ret;
    vector<string> members {"member1", "member2", "member3", "member4"};
    vector<double> scores {2.1, 2.2, 2.3, 2.4};

    ret = redis->ZAdd(key, score, member);
    if (!ret) {
        cout << "ZAdd Error" << endl;
    }
    else {
        cout << "ZAdd OK" << endl;
    }

    ret = redis->ZAdd(key, scores, members);
    if (!ret) {
        cout << "ZAdd Error" << endl;
    }
    else {
        cout << "ZAdd OK" << endl;
    }

    int count = 0;
    ret = redis->ZCard(key, count);
    if (!ret) {
        cout << "ZCard Error" << endl;
    }
    else {
        cout << "ZCard OK count: " << count << endl;
    }

    count = 0;
    ret  = redis->ZCount(key, 1.8, 2.3, count);
    if (!ret) {
        cout << "ZCount Error" << endl;
    }
    else {
        cout << "ZCount OK count: " << count << endl;
    }

    ret = redis->ZIncrBy(key, 5.4, "member2");
    if (!ret) {
        cout << "ZIncrBy Error" << endl;
    }
    else {
        cout << "ZIncrBy OK" << endl;
    }

    vector<string> out1;
    vector<double> out2;
    ret = redis->ZRange(key, 1, 3, out1, out2);
    if (!ret) {
        cout << "ZRange Error" << endl;
    }
    else {
        cout << "ZRange OK" << endl;
        for (int i = 0; i < out1.size(); i++) {
            cout << out1[i] << " : " << out2[i] << endl;
        }
    }

    out1.clear();
    out2.clear();
    ret = redis->ZRange(key, 2.0, 3.0, out1, out2);
    if (!ret) {
        cout << "ZRange Error" << endl;
    }
    else {
        cout << "ZRange OK" << endl;
        for (int i = 0; i < out1.size(); i++) {
            cout << out1[i] << " : " << out2[i] << endl;
        }
    }

    int idx = 0;
    ret = redis->ZRank(key, "member2", idx);
    if (!ret) {
        cout << "ZRank Error" << endl;
    }
    else {
        cout << "ZRank OK index: " << idx << endl;
    }

    ret = redis->ZRem(key, member);
    if (!ret) {
        cout << "ZRem Error" << endl;
    }
    else {
        cout << "ZRem OK" << endl;
    }

    ret = redis->ZRem(key, members);
    if (!ret) {
        cout << "ZRem Error" << endl;
    }
    else {
        cout << "ZRem OK" << endl;
    }

    ret = redis->ZAdd(key, score, member);
    if (!ret) {
        cout << "ZAdd Error" << endl;
    }
    else {
        cout << "ZAdd OK" << endl;
    }

    ret = redis->ZAdd(key, scores, members);
    if (!ret) {
        cout << "ZAdd Error" << endl;
    }
    else {
        cout << "ZAdd OK" << endl;
    }
    
    ret = redis->ZRemRangeByRank(key, 2, 3);
    if (!ret) {
        cout << "ZRemRangeByRank Error" << endl;
    }
    else {
        cout << "ZRemRangeByRank OK" << endl;
    }

    ret = redis->ZRemRangeByScore(key, 2.0, 2.8);
    if (!ret) {
        cout << "ZRemRangeByScore Error" << endl;
    }
    else {
        cout << "ZRemRangeByScore OK" << endl;
    }

    score = 0.0;
    ret = redis->ZScore(key, member, score);
    if (!ret) {
        cout << "ZScore Error" << endl;
    }
    else {
        cout << "ZScore OK score: " << score << endl;
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
    TestZSet(redis);
    delete redis;
    return 0;
}
