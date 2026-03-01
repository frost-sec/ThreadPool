#ifndef PIPQ_RINGBUFFER_HPP
#define PIPQ_RINGBUFFER_HPP

#include <atomic>
#include <vector>
#include <thread>
#include <mutex>
#include <algorithm>
#include <optional>
#include <iostream>
#include <array>
#include <queue>
#include <cassert>
#include <new>

namespace pipq {

// ==========================================
// Part 0: Constants & Alignment
// ==========================================
constexpr size_t CACHE_LINE_SIZE = 64;

// ==========================================
// Part 1: High-Performance MPMC Ring Buffer
// ==========================================
// 这是一个基于 Sequence 的无锁有界队列
// 核心思想：利用 sequence number 解决 ABA 问题并标记 Slot 状态
template<typename T>
class RingBuffer {
public:
    struct Element {
        T data;
        int priority;
    };

private:
    struct Slot {
        std::atomic<size_t> sequence;
        Element element;
        // Padding to avoid false sharing between adjacent slots
        // 虽然浪费空间，但在高并发下这是吞吐量的保证
        char pad[CACHE_LINE_SIZE - sizeof(std::atomic<size_t>) - sizeof(Element)];
    };

    static_assert(sizeof(Slot) >= CACHE_LINE_SIZE, "Slot size too small");

    // RingBuffer 需要是 2 的幂次
    const size_t buffer_mask_;
    Slot* buffer_;
    
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> enqueue_pos_;
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> dequeue_pos_;

public:
    explicit RingBuffer(size_t buffer_size) 
        : buffer_mask_(buffer_size - 1) {
        // 强制 buffer_size 为 2 的幂
        assert((buffer_size >= 2) && ((buffer_size & (buffer_size - 1)) == 0));

        buffer_ = new Slot[buffer_size];
        for (size_t i = 0; i < buffer_size; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
        enqueue_pos_.store(0, std::memory_order_relaxed);
        dequeue_pos_.store(0, std::memory_order_relaxed);
    }

    ~RingBuffer() {
        delete[] buffer_;
    }

    // 返回 true 表示入队成功，false 表示队列满
    bool enqueue(T data, int priority) {
        Slot* slot;
        size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        
        while (true) {
            slot = &buffer_[pos & buffer_mask_];
            size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff = (intptr_t)seq - (intptr_t)pos;

            if (diff == 0) {
                // Slot 是空的，尝试抢占
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    // 抢占成功，写入数据
                    slot->element.data = std::move(data);
                    slot->element.priority = priority;
                    // 发布数据：seq + 1 表示该 Slot 已满，且轮次推进
                    slot->sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                // 队列满了 (seq < pos 表示该 slot 的数据还没被消费，仍然是上一轮的)
                return false;
            } else {
                // Pos 落后了，重新加载
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }
    }

    // 返回 true 表示出队成功，false 表示队列空
    bool dequeue(T& data, int& priority) {
        Slot* slot;
        size_t pos = dequeue_pos_.load(std::memory_order_relaxed);

        while (true) {
            slot = &buffer_[pos & buffer_mask_];
            size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t diff = (intptr_t)seq - (intptr_t)(pos + 1);

            if (diff == 0) {
                // Slot 是满的，尝试抢占
                if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    // 抢占成功，读取数据
                    data = std::move(slot->element.data);
                    priority = slot->element.priority;
                    // 释放 Slot：seq + mask + 1 表示该 Slot 变空，且为下一轮做好了准备
                    slot->sequence.store(pos + buffer_mask_ + 1, std::memory_order_release);
                    return true;
                }
            } else if (diff < 0) {
                // 队列空了 (seq < pos + 1 表示该 slot 还是空的，或者正被写入)
                return false;
            } else {
                // Pos 落后了
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }
    }
};

// ==========================================
// Part 2: Worker Heap (Thread Local)
// ==========================================

template<typename T>
class WorkerHeap {
    struct Item {
        T data;
        int priority;
        bool operator>(const Item& other) const { return priority > other.priority; }
    };
    
    // 这里的 vector 预分配也是性能关键点
    std::priority_queue<Item, std::vector<Item>, std::greater<Item>> pq;

public:
    void push(T val, int priority) {
        pq.push({std::move(val), priority});
    }

    std::optional<T> pop() {
        if (pq.empty()) return std::nullopt;
        Item top = std::move(const_cast<Item&>(pq.top()));
        pq.pop();
        return std::move(top.data);
    }

    // 批量导入，减少多次 push 的开销 (O(N) vs O(N log N))
    // 这里的实现简单用 push，如果追求极致可以用 make_heap
    void bulk_push(std::vector<std::pair<T, int>>& items) {
        for (auto& item : items) {
            pq.push({std::move(item.first), item.second});
        }
    }

    size_t size() const { return pq.size(); }
    
    std::optional<int> min_priority() const {
        if (pq.empty()) return std::nullopt;
        return pq.top().priority;
    }
};

// ==========================================
// Part 3: PIPQ (RingBuffer Edition)
// ==========================================

template<typename T>
class PIPQ {
    // 关键参数调整
    static constexpr size_t RING_BUFFER_SIZE = 1 << 22; // 必须是 2 的幂
    static constexpr size_t LOCAL_CAPACITY = 1024;    // 本地缓存大小
    static constexpr size_t BATCH_SIZE = 32;          // 每次从 Global 捞多少个
    
    RingBuffer<T> global_ring_;

    struct ThreadContext {
        WorkerHeap<T> local_heap;
    };

    static ThreadContext& get_ctx() {
        static thread_local ThreadContext ctx;
        return ctx;
    }

public:
    PIPQ() : global_ring_(RING_BUFFER_SIZE) {}

    // ---------------------------------------------------------
    // Push: Local -> Global Spill
    // ---------------------------------------------------------
    void push(T val, int priority) {
        auto& ctx = get_ctx();

        // 策略：
        // 1. 如果本地不满，且优先级合适（或者虽然不合适但为了速度先放本地），优先放本地。
        //    为了保持更好的全局有序性，我们限制：如果 Priority 很高（数值小），最好放 Global 让别人也能看见？
        //    不，PIPQ 的假设是本地最快。
        
        bool pushed_to_local = false;
        if (ctx.local_heap.size() < LOCAL_CAPACITY) {
            // 简单策略：本地没满就塞本地
             ctx.local_heap.push(std::move(val), priority);
             pushed_to_local = true;
        }

        if (!pushed_to_local) {
            // 本地满了，溢出到 Global RingBuffer
            // 这是一个 Wait-Free 尝试，如果 RingBuffer 也满了（极少见），我们可能需要自旋或扩容
            // 这里为了 Bench 简单，使用忙等待
            while (!global_ring_.enqueue(std::move(val), priority)) {
                std::this_thread::yield();
            }
        }
    }

    // ---------------------------------------------------------
    // Pop: Local <- Global Batch Fetch
    // ---------------------------------------------------------
    std::optional<T> try_pop() {
        auto& ctx = get_ctx();

        // 策略：
        // 1. 检查 Global RingBuffer 是否有积压的数据？
        //    如果有，为了防止“饿死”全局队列里的高优先级任务，我们应该时不时去捞一把。
        //    或者，如果本地快空了，必须去捞。
        
        // 优化策略：每次 try_pop 都尝试从 Global 偷一批数据填充本地。
        // 这实现了 "Eventual Priority Consistency"。
        
        // 临时缓冲区用于批量搬运
        // static thread_local vector 避免分配
        static thread_local std::vector<std::pair<T, int>> batch_buffer;
        batch_buffer.clear();

        // 尝试从 RingBuffer 捞取 BATCH_SIZE 个元素
        for (size_t i = 0; i < BATCH_SIZE; ++i) {
            T data;
            int prio;
            if (global_ring_.dequeue(data, prio)) {
                batch_buffer.emplace_back(std::move(data), prio);
            } else {
                break; // Global 空了
            }
        }

        // 如果捞到了，放入本地堆进行重排序
        if (!batch_buffer.empty()) {
            ctx.local_heap.bulk_push(batch_buffer);
        }

        // 2. 从本地堆弹出最小的
        return ctx.local_heap.pop();
    }
};

} // namespace pipq

#endif