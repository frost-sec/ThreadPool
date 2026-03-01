#ifndef PIPQ_MPSC_HPP
#define PIPQ_MPSC_HPP

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
#include <random>
#include <tuple>
#include <limits>
#include <new> 

namespace pipq_mpsc {

// ==========================================
// Part 0: Utils
// ==========================================

struct SpinLock {
    std::atomic<bool> lock_{false};

    inline void lock() {
        if (!lock_.exchange(true, std::memory_order_acquire)) return;
        while (lock_.load(std::memory_order_relaxed) || lock_.exchange(true, std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }
    
    inline bool try_lock() {
        return !lock_.exchange(true, std::memory_order_acquire);
    }

    inline void unlock() {
        lock_.store(false, std::memory_order_release);
    }
};

template <class T, class Container = std::vector<T>, class Compare = std::less<typename Container::value_type>>
class ReservedPriorityQueue : public std::priority_queue<T, Container, Compare> {
public:
    void reserve(size_t capacity) { this->c.reserve(capacity); }
};

// ==========================================
// Part 1: EBR Infrastructure
// ==========================================

class EBRManager {
    static constexpr size_t MAX_THREADS = 128;
    static constexpr size_t RETIRE_FREQUENCY = 64;

    struct ThreadState {
        std::atomic<bool> active{false};
        std::atomic<uint64_t> local_epoch{0};
        char padding[64 - sizeof(std::atomic<bool>) - sizeof(std::atomic<uint64_t>)];
    };

    struct GarbageBag {
        SpinLock mtx;
        std::vector<void(*)(void*)> deleters;
        std::vector<void*> ptrs;
        
        void push(void* ptr, void(*deleter)(void*)) {
            std::lock_guard<SpinLock> lock(mtx);
            ptrs.push_back(ptr);
            deleters.push_back(deleter);
        }
        
        void clear() {
            std::lock_guard<SpinLock> lock(mtx);
            for (size_t i = 0; i < ptrs.size(); ++i) {
                if (ptrs[i]) deleters[i](ptrs[i]);
            }
            ptrs.clear();
            deleters.clear();
        }
    };

    std::array<ThreadState, MAX_THREADS> thread_states;
    std::atomic<uint64_t> global_epoch{0};
    std::array<GarbageBag, 3> limbo_bags;

    struct ThreadLocalData {
        int id{-1};
        size_t counter{0};
        std::vector<std::pair<void*, void(*)(void*)>> local_buffer;
        ~ThreadLocalData() { if (id != -1) EBRManager::get().deregister_thread(id); }
    };

    static ThreadLocalData& get_tls() {
        static thread_local ThreadLocalData tls;
        return tls;
    }

public:
    static EBRManager& get() { static EBRManager instance; return instance; }

    int register_thread() {
        for (size_t i = 0; i < MAX_THREADS; ++i) {
            bool expected = false;
            if (thread_states[i].active.compare_exchange_strong(expected, true)) {
                thread_states[i].local_epoch.store(global_epoch.load(), std::memory_order_relaxed);
                return static_cast<int>(i);
            }
        }
        return -1;
    }

    void deregister_thread(int id) {
        if (id >= 0) thread_states[id].active.store(false, std::memory_order_release);
    }

    inline void enter_critical() {
        auto& tls = get_tls();
        if (tls.id == -1) tls.id = register_thread();
        uint64_t g_epoch = global_epoch.load(std::memory_order_seq_cst);
        thread_states[tls.id].local_epoch.store(g_epoch, std::memory_order_seq_cst);
    }

    inline void exit_critical() {}

    template<typename T>
    void retire(T* ptr) {
        auto& tls = get_tls();
        tls.local_buffer.push_back({ptr, [](void* p) { delete static_cast<T*>(p); }});
        tls.counter++;
        if (tls.counter >= RETIRE_FREQUENCY) {
            tls.counter = 0;
            size_t epoch = global_epoch.load(std::memory_order_acquire);
            auto& bag = limbo_bags[epoch % 3];
            {
                std::lock_guard<SpinLock> lock(bag.mtx);
                for(auto& pair : tls.local_buffer) {
                    bag.ptrs.push_back(pair.first);
                    bag.deleters.push_back(pair.second);
                }
            }
            tls.local_buffer.clear();
            try_advance_epoch();
        }
    }

private:
    void try_advance_epoch() {
        uint64_t g_epoch = global_epoch.load(std::memory_order_acquire);
        for (size_t i = 0; i < MAX_THREADS; ++i) {
            if (thread_states[i].active.load(std::memory_order_acquire)) {
                if (thread_states[i].local_epoch.load(std::memory_order_acquire) != g_epoch) return;
            }
        }
        if (global_epoch.compare_exchange_strong(g_epoch, g_epoch + 1)) {
            limbo_bags[(g_epoch + 2) % 3].clear();
        }
    }
};

struct EBRGuard {
    EBRGuard() { EBRManager::get().enter_critical(); }
    ~EBRGuard() { EBRManager::get().exit_critical(); }
};

template<typename T>
struct TaggedPtr {
    static const uintptr_t MARK_MASK = 1;
    static T* get_ptr(T* ptr) { return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(ptr) & ~MARK_MASK); }
    static T* get_ptr(std::atomic<T*>& atomic_ptr) { return get_ptr(atomic_ptr.load(std::memory_order_acquire)); }
    static bool is_marked(T* ptr) { return (reinterpret_cast<uintptr_t>(ptr) & MARK_MASK) != 0; }
    static T* mark(T* ptr) { return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(ptr) | MARK_MASK); }
};

// ==========================================
// Part 2: Leader Level
// ==========================================

template<typename T>
class LeaderList {
public:
    struct Node {
        T data;
        int priority;
        int tid; 
        std::atomic<Node*> next;
        Node(T d, int p, int t) : data(std::move(d)), priority(p), tid(t), next(nullptr) {}
    };

private:
    std::atomic<Node*> head;

public:
    LeaderList() {
        Node* dummy = new Node(T{}, std::numeric_limits<int>::min(), -1);
        head.store(dummy);
    }

    void insert(T val, int priority, int tid) {
        Node* new_node = new Node(std::move(val), priority, tid);
        while (true) {
            EBRGuard guard;
            Node* pred = head.load(std::memory_order_acquire);
            Node* curr = nullptr;
            while (true) {
                Node* next_ptr = pred->next.load(std::memory_order_acquire);
                if (TaggedPtr<Node>::is_marked(next_ptr)) { pred = head.load(std::memory_order_acquire); continue; }
                curr = TaggedPtr<Node>::get_ptr(next_ptr);
                if (curr) {
                    Node* succ_ptr = curr->next.load(std::memory_order_acquire);
                    if (TaggedPtr<Node>::is_marked(succ_ptr)) {
                        pred->next.compare_exchange_strong(curr, TaggedPtr<Node>::get_ptr(succ_ptr), std::memory_order_release);
                        continue; 
                    }
                }
                if (!curr || curr->priority > priority) break;
                pred = curr;
            }
            new_node->next.store(curr, std::memory_order_release);
            if (pred->next.compare_exchange_strong(curr, new_node, std::memory_order_release)) return;
        }
    }

    std::optional<std::tuple<T, int, int>> try_pop_min() {
        while (true) {
            EBRGuard guard;
            Node* dummy = head.load(std::memory_order_acquire);
            Node* next_ptr = dummy->next.load(std::memory_order_acquire);
            Node* first_real = TaggedPtr<Node>::get_ptr(next_ptr);

            if (!first_real) return std::nullopt;
            if (TaggedPtr<Node>::is_marked(next_ptr)) { std::this_thread::yield(); continue; }

            Node* marked_next = TaggedPtr<Node>::mark(first_real);
            if (dummy->next.compare_exchange_strong(first_real, marked_next, std::memory_order_release)) {
                Node* succ = TaggedPtr<Node>::get_ptr(first_real->next.load(std::memory_order_acquire));
                if (head.load() == dummy) dummy->next.compare_exchange_strong(marked_next, succ, std::memory_order_release);
                auto res = std::make_tuple(std::move(first_real->data), first_real->priority, first_real->tid);
                EBRManager::get().retire(first_real);
                return res;
            }
        }
    }
    
    std::optional<std::pair<T, int>> remove_max_of_thread(int target_tid) {
        while (true) {
            EBRGuard guard;
            Node* pred = head.load(std::memory_order_acquire);
            Node* target_pred = nullptr;
            Node* target_node = nullptr;

            while (true) {
                Node* next_ptr = pred->next.load(std::memory_order_acquire);
                if (TaggedPtr<Node>::is_marked(next_ptr)) { pred = head.load(std::memory_order_acquire); target_pred = nullptr; target_node = nullptr; continue; }
                Node* curr = TaggedPtr<Node>::get_ptr(next_ptr);
                if (!curr) break; 
                Node* succ_ptr = curr->next.load(std::memory_order_acquire);
                if (TaggedPtr<Node>::is_marked(succ_ptr)) {
                    pred->next.compare_exchange_strong(curr, TaggedPtr<Node>::get_ptr(succ_ptr), std::memory_order_release);
                    continue; 
                }
                if (curr->tid == target_tid) { target_pred = pred; target_node = curr; }
                pred = curr;
            }

            if (!target_node) return std::nullopt;
            Node* node_next_ptr = target_node->next.load(std::memory_order_acquire);
            if (TaggedPtr<Node>::is_marked(node_next_ptr)) continue; 
            
            Node* marked_node_next = TaggedPtr<Node>::mark(TaggedPtr<Node>::get_ptr(node_next_ptr));
            if (target_node->next.compare_exchange_strong(node_next_ptr, marked_node_next)) {
                Node* target_ptr_unmarked = target_node;
                target_pred->next.compare_exchange_strong(target_ptr_unmarked, TaggedPtr<Node>::get_ptr(node_next_ptr));
                auto res = std::make_pair(std::move(target_node->data), target_node->priority);
                EBRManager::get().retire(target_node);
                return res;
            }
        }
    }
};

// ==========================================
// Part 3: Worker Level
// ==========================================

template<typename T>
class WorkerHeap {
    struct Item {
        T data;
        int priority;
        bool operator>(const Item& other) const { return priority > other.priority; }
    };

    ReservedPriorityQueue<Item, std::vector<Item>, std::greater<Item>> pq;
    mutable SpinLock mtx; 

public:
    void reserve(size_t capacity) { std::lock_guard<SpinLock> lock(mtx); pq.reserve(capacity); }
    void push(T val, int priority) { std::lock_guard<SpinLock> lock(mtx); pq.push({std::move(val), priority}); }
    std::optional<std::pair<T, int>> pop() {
        std::lock_guard<SpinLock> lock(mtx);
        if (pq.empty()) return std::nullopt;
        Item top = pq.top(); pq.pop();
        return std::make_pair(std::move(top.data), top.priority);
    }
    std::optional<int> min_priority() const {
        std::lock_guard<SpinLock> lock(mtx);
        if (pq.empty()) return std::nullopt;
        return pq.top().priority;
    }
};

// ==========================================
// Part 4: PIPQ MPSC Main Class
// ==========================================

template<typename T>
class PIPQ_MPSC {
    static constexpr int CNTR_MIN = 10;
    static constexpr int CNTR_MAX = 100;
    static constexpr int MAX_THREADS = 128; 

    // NEW: Threshold to force feed leader list
    // If approx count is below this, producers MUST insert to leader list
    static constexpr int FORCE_FEED_THRESHOLD = 50; 

    LeaderList<T> leader_list;
    std::vector<std::unique_ptr<WorkerHeap<T>>> worker_heaps;
    SpinLock workers_mtx; 
    
    struct alignas(64) AlignedCounter {
        std::atomic<int> count{0};
    };
    std::array<AlignedCounter, MAX_THREADS> global_l_counts;

    // NEW: Approximate Global Leader Count (Relaxed)
    // Helps producers decide whether to fast-path or force-feed
    std::atomic<int> approx_leader_count{0};

    struct ThreadContext {
        int tid;
        WorkerHeap<T>* local_heap;
        ThreadContext() : tid(-1), local_heap(nullptr) {}
    };

    static ThreadContext& get_ctx() {
        static thread_local ThreadContext ctx;
        return ctx;
    }

    void ensure_registered() {
        auto& ctx = get_ctx();
        if (ctx.tid == -1) {
            ctx.tid = EBRManager::get().register_thread(); 
            auto heap = std::make_unique<WorkerHeap<T>>();
            heap->reserve(100000); 
            ctx.local_heap = heap.get();
            
            std::lock_guard<SpinLock> lock(workers_mtx);
            if (worker_heaps.size() <= (size_t)ctx.tid) worker_heaps.resize(ctx.tid + 1);
            worker_heaps[ctx.tid] = std::move(heap);
            global_l_counts[ctx.tid].count.store(0);
        }
    }

public:
    PIPQ_MPSC() = default;

    void push(T val, int priority) {
        ensure_registered();
        auto& ctx = get_ctx();
        int tid = ctx.tid;

        std::optional<int> local_min = ctx.local_heap->min_priority();
        
        // Strategy: 
        // 1. Normal Fast Path: if priority >= local_min
        // 2. BUT: If Leader List is starving (approx_leader_count < Threshold), FORCE SLOW PATH
        bool should_force_feed = approx_leader_count.load(std::memory_order_relaxed) < FORCE_FEED_THRESHOLD;

        if (!should_force_feed && local_min.has_value() && priority >= local_min.value()) {
            ctx.local_heap->push(std::move(val), priority);
        } else {
            // Slow Path (or Forced Slow Path)
            int current_count = global_l_counts[tid].count.load(std::memory_order_relaxed);
            
            if (current_count < CNTR_MAX) {
                leader_list.insert(std::move(val), priority, tid);
                global_l_counts[tid].count.fetch_add(1, std::memory_order_relaxed);
                approx_leader_count.fetch_add(1, std::memory_order_relaxed); // Update global count
            } else {
                leader_list.insert(std::move(val), priority, tid);
                // Inserted one, approx +1
                approx_leader_count.fetch_add(1, std::memory_order_relaxed); 

                auto max_node = leader_list.remove_max_of_thread(tid);
                if (max_node) {
                    ctx.local_heap->push(std::move(max_node->first), max_node->second);
                    // Removed one, approx -1
                    approx_leader_count.fetch_sub(1, std::memory_order_relaxed);
                } else {
                    global_l_counts[tid].count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    }

    std::optional<T> try_pop() {
        // 1. Try Leader List
        auto res_tuple = leader_list.try_pop_min();
        
        if (res_tuple) {
            auto [data, prio, origin_tid] = *res_tuple;
            if (origin_tid >= 0 && origin_tid < MAX_THREADS) {
                 global_l_counts[origin_tid].count.fetch_sub(1, std::memory_order_relaxed);
            }
            approx_leader_count.fetch_sub(1, std::memory_order_relaxed);
            return data;
        } 
        
        // 2. Leader Empty -> Upsert Scan
        // This should happen RARELY now because producers are force-feeding
        int best_tid = -1;
        int best_prio = std::numeric_limits<int>::max();

        {
            std::lock_guard<SpinLock> w_lock(workers_mtx); 
            for (size_t i = 0; i < worker_heaps.size(); ++i) {
                if (!worker_heaps[i]) continue;
                auto min_p = worker_heaps[i]->min_priority();
                if (min_p.has_value() && min_p.value() < best_prio) {
                    best_prio = min_p.value();
                    best_tid = i;
                }
            }
        }

        if (best_tid != -1) {
            auto item = worker_heaps[best_tid]->pop();
            if (item) {
                leader_list.insert(std::move(item->first), item->second, best_tid);
                global_l_counts[best_tid].count.fetch_add(1, std::memory_order_relaxed);
                approx_leader_count.fetch_add(1, std::memory_order_relaxed);
                
                auto retry = leader_list.try_pop_min();
                if (retry) {
                     auto [r_data, r_prio, r_tid] = *retry;
                     if (r_tid >= 0 && r_tid < MAX_THREADS) {
                         global_l_counts[r_tid].count.fetch_sub(1, std::memory_order_relaxed);
                     }
                     approx_leader_count.fetch_sub(1, std::memory_order_relaxed);
                     return r_data;
                }
            }
        }

        return std::nullopt;
    }
};

} // namespace pipq_mpsc

#endif