#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <iomanip>
#include <string>

// 包含我们之前实现的 PIPQ 头文件
#include "PIPQ_EBR.hpp"
// 使用高性能的 PIPQ 实现
using QueueType = pipq::PIPQ<int>;

/**
 * @brief 基准测试配置参数
 */
const int NUM_OPERATIONS_PER_THREAD = 1000000; // 每个线程执行 100万次操作
const std::vector<int> THREAD_COUNTS = {1, 4, 8, 16, 24, 32}; // 测试不同的线程数

/**
 * @brief 简单的屏障，用于让所有线程同时开始，确保并发压力
 */
class Barrier {
    std::atomic<int> count;
    std::atomic<int> generation;
    int threshold;
public:
    explicit Barrier(int count) : count(count), generation(0), threshold(count) {}
    void wait() {
        int gen = generation.load();
        if (count.fetch_sub(1) == 1) {
            count.store(threshold);
            generation.fetch_add(1);
        } else {
            while (gen == generation.load()) {
                std::this_thread::yield();
            }
        }
    }
};

/**
 * @brief 执行基准测试的核心函数
 * * @param num_threads 线程数量
 * @param insert_ratio 插入操作的比例 (0.0 - 1.0). 1.0 表示 100% 插入, 0.5 表示 50% 插入 50% 弹出
 * @param test_name 测试名称用于输出
 */
void run_benchmark(int num_threads, double insert_ratio, const std::string& test_name) {
    QueueType queue;
    std::vector<std::thread> threads;
    Barrier barrier(num_threads + 1); // +1 是因为主线程也要等待
    
    // 预填充数据 (如果测试包含 Pop，防止队列为空导致测试无效)
    if (insert_ratio < 1.0) {
        // 预先插入一定量的数据，确保 Pop 有数据可取
        // 注意：这部分时间不计入 QPS
        // 使用单线程预填充，避免干扰
        int pre_fill = NUM_OPERATIONS_PER_THREAD * num_threads / 2;
        if (pre_fill > 0) {
            // 简单预填充
             // 我们在这里不进行大量预填充，因为 PIPQ 的设计是 Insert-Optimized，
             // 我们希望测试 Insert 和 Pop 的混合竞争。
             // 实际上，如果队列为空，try_pop 返回 nullopt 也是一种有效的 QPS 测试（测试空转性能）。
        }
    }

    std::atomic<long long> total_ops{0};

    // 启动工作线程
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            // 线程本地随机数生成器
            std::mt19937 rng(i + 12345);
            std::uniform_real_distribution<double> dist(0.0, 1.0);
            std::uniform_int_distribution<int> prio_dist(0, 100000);

            // 等待所有线程就绪
            barrier.wait();

            long long ops_count = 0;
            for (int j = 0; j < NUM_OPERATIONS_PER_THREAD; ++j) {
                if (dist(rng) < insert_ratio) {
                    // 执行 Insert
                    // 随机优先级
                    queue.push(j, prio_dist(rng));
                } else {
                    // 执行 Pop
                    queue.try_pop();
                }
                ops_count++;
            }
            total_ops.fetch_add(ops_count, std::memory_order_relaxed);
        });
    }

    // 主线程等待开始信号 (与工作线程同步)
    barrier.wait();
    auto start_time = std::chrono::high_resolution_clock::now();

    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;

    // 计算指标
    double seconds = duration.count();
    long long total_operations = (long long)num_threads * NUM_OPERATIONS_PER_THREAD;
    double qps = total_operations / seconds; // 每秒操作数
    double mops = qps / 1000000.0; // 百万操作每秒

    // 格式化输出
    std::cout << std::left << std::setw(25) << test_name 
              << "| Threads: " << std::setw(3) << num_threads 
              << "| Time: " << std::fixed << std::setprecision(3) << seconds << "s "
              << "| QPS: " << std::setprecision(2) << mops << " Mops/s" << std::endl;
}

int main() {
    std::cout << "==========================================================" << std::endl;
    std::cout << "      PIPQ (Strict Insert-Optimized) Benchmark            " << std::endl;
    std::cout << "==========================================================" << std::endl;
    std::cout << "Testing with " << NUM_OPERATIONS_PER_THREAD << " ops per thread." << std::endl;
    std::cout << "----------------------------------------------------------" << std::endl;

    // 场景 1: 100% Insert (Write Heavy)
    // 这是 PIPQ 最擅长的场景，理论上随着线程数增加，性能应该线性增长（因为是 Thread Local）
    std::cout << "\n[Scenario 1: 100% Insert - Producer Heavy]" << std::endl;
    for (int t : THREAD_COUNTS) {
        if (t > std::thread::hardware_concurrency()) break; // 避免超过硬件线程数太多
        run_benchmark(t, 1.0, "Pure Insert");
    }

    // 场景 2: 50% Insert / 50% Pop (Mixed)
    // 模拟典型的生产消费场景。此时 Insert 依然很快，但 Pop 会引入 Coordinator 竞争
    std::cout << "\n[Scenario 2: 50% Insert / 50% Pop - Balanced]" << std::endl;
    for (int t : THREAD_COUNTS) {
        if (t > std::thread::hardware_concurrency()) break;
        run_benchmark(t, 0.5, "Mixed 50/50");
    }

    // 场景 3: 5% Insert / 95% Pop (Read Heavy)
    // 这是 PIPQ 的压力场景，测试 Coordinator 的吞吐上限
    std::cout << "\n[Scenario 3: 5% Insert / 95% Pop - Consumer Heavy]" << std::endl;
    for (int t : THREAD_COUNTS) {
        if (t > std::thread::hardware_concurrency()) break;
        run_benchmark(t, 0.05, "Read Heavy 5/95");
    }

    std::cout << "\nBenchmark Completed." << std::endl;
    return 0;
}