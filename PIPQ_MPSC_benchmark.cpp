#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <iomanip>
#include <string>

#include "PIPQ_MPSC.hpp"

using QueueType = pipq_mpsc::PIPQ_MPSC<int>;

const int TEST_DURATION_SECONDS = 3; // 运行 3 秒
const std::vector<int> PRODUCER_COUNTS = {1, 4, 8, 16}; 

// 原子计数器，用于统计吞吐量
std::atomic<long long> total_produced{0};
std::atomic<long long> total_consumed{0};
std::atomic<bool> stop_flag{false};

void run_mpsc_benchmark(int num_producers) {
    QueueType queue;
    std::vector<std::thread> producers;
    std::thread consumer;
    
    // 重置计数器
    total_produced = 0;
    total_consumed = 0;
    stop_flag = false;

    // 启动生产者
    for (int i = 0; i < num_producers; ++i) {
        producers.emplace_back([&, i]() {
            std::mt19937 rng(i + 100);
            std::uniform_int_distribution<int> prio_dist(0, 100000);
            
            // 注册 EBR
            pipq_mpsc::EBRManager::get().register_thread();

            while (!stop_flag.load(std::memory_order_relaxed)) {
                queue.push(i, prio_dist(rng));
                total_produced.fetch_add(1, std::memory_order_relaxed);
                
                // 简单的流控，防止生产太快把内存撑爆 (Benchmark保护)
                // 在生产环境中不需要，但在测试中如果生产 >> 消费，会OOM
                // 这里我们不做流控，测试极限生产性能
            }
            
            pipq_mpsc::EBRManager::get().deregister_thread(i);
        });
    }

    // 启动唯一消费者
    consumer = std::thread([&]() {
        pipq_mpsc::EBRManager::get().register_thread();
        while (!stop_flag.load(std::memory_order_relaxed)) {
            auto val = queue.try_pop();
            if (val) {
                total_consumed.fetch_add(1, std::memory_order_relaxed);
            } else {
                // 如果空了，稍微让渡 CPU
                std::this_thread::yield();
            }
        }
        // 把剩下的消费完 (可选)
        // while (queue.try_pop()) total_consumed++;
        
        pipq_mpsc::EBRManager::get().deregister_thread(-1); // dummy id logic
    });

    // 运行测试
    std::cout << "Running MPSC (1 Consumer, " << num_producers << " Producers) for " << TEST_DURATION_SECONDS << "s..." << std::flush;
    std::this_thread::sleep_for(std::chrono::seconds(TEST_DURATION_SECONDS));
    stop_flag = true;

    // 等待结束
    for (auto& t : producers) t.join();
    consumer.join();

    // 计算结果
    long long total_ops = total_produced + total_consumed;
    double mops = (double)total_ops / TEST_DURATION_SECONDS / 1000000.0;
    
    std::cout << " Done." << std::endl;
    std::cout << "  -> Produced: " << total_produced << ", Consumed: " << total_consumed << std::endl;
    std::cout << "  -> Total Throughput: " << std::fixed << std::setprecision(2) << mops << " Mops/s" << std::endl;
    std::cout << "----------------------------------------------------------" << std::endl;
}

int main() {
    std::cout << "==========================================================" << std::endl;
    std::cout << "      PIPQ MPSC (Multi-Producer Single-Consumer) Test     " << std::endl;
    std::cout << "==========================================================" << std::endl;

    for (int p : PRODUCER_COUNTS) {
        if (p > std::thread::hardware_concurrency()) break;
        run_mpsc_benchmark(p);
    }

    return 0;
}g++ -std=c++17 -O3 -pthread benchmark_mpsc.cpp -o mpsc_bench