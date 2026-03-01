#ifndef PIPQ_EBR_DEBUG_HPP
#define PIPQ_EBR_DEBUG_HPP

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
#include <sstream>

// ==========================================
// DEBUG UTILS
// ==========================================
namespace pipq_debug {
    static std::atomic<bool> g_hang_detected{false};

    inline void report_hang(const char* location, int tid, uint64_t loops, const char* extra_info = "") {
        if (!g_hang_detected.exchange(true)) {
            std::cerr << "\n[FATAL ERROR] Hang detected!" << std::endl;
            std::cerr << "Location: " << location << std::endl;
            std::cerr << "Thread ID: " << tid << std::endl;
            std::cerr << "Loop Count: " << loops << std::endl;
            std::cerr << "Extra Info: " << extra_info << std::endl;
            std::abort();
        }
    }
}

namespace pipq {

// ==========================================
// Part 1: EBR Infrastructure (Instrumented)
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
        std::mutex mtx;
        std::vector<void(*)(void*)> deleters;
        std::vector<void*> ptrs;
        
        void push(void* ptr, void(*deleter)(void*)) {
            std::lock_guard<std::mutex> lock(mtx);
            ptrs.push_back(ptr);
            deleters.push_back(deleter);
        }
        
        void clear() {
            std::lock_guard<std::mutex> lock(mtx);
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
        ~ThreadLocalData() {
            if (id != -1) EBRManager::get().deregister_thread(id);
        }
    };

    static ThreadLocalData& get_tls() {
        static thread_local ThreadLocalData tls;
        return tls;
    }

public:
    static EBRManager& get() {
        static EBRManager instance;
        return instance;
    }

    // DEBUG: Get Current Epoch
    uint64_t get_global_epoch() {
        return global_epoch.load(std::memory_order_relaxed);
    }

    int register_thread() {
        for (size_t i = 0; i < MAX_THREADS; ++i) {
            bool expected = false;
            if (thread_states[i].active.compare_exchange_strong(expected, true)) {
                thread_states[i].local_epoch.store(global_epoch.load(), std::memory_order_relaxed);
                return static_cast<int>(i);
            }
        }
        std::cerr << "[EBR] Failed to register thread: Max threads exceeded!" << std::endl;
        return -1;
    }

    void deregister_thread(int id) {
        if (id >= 0) thread_states[id].active.store(false, std::memory_order_release);
    }

    void enter_critical() {
        auto& tls = get_tls();
        if (tls.id == -1) tls.id = register_thread();
        uint64_t g_epoch = global_epoch.load(std::memory_order_seq_cst);
        thread_states[tls.id].local_epoch.store(g_epoch, std::memory_order_seq_cst);
    }

    void exit_critical() {
        // No-op for performance
    }

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
                std::lock_guard<std::mutex> lock(bag.mtx);
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

// ==========================================
// Part 2: Utils
// ==========================================

template<typename T>
struct TaggedPtr {
    static const uintptr_t MARK_MASK = 1;
    static T* get_ptr(T* ptr) { return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(ptr) & ~MARK_MASK); }
    static T* get_ptr(std::atomic<T*>& atomic_ptr) { return get_ptr(atomic_ptr.load(std::memory_order_acquire)); }
    static bool is_marked(T* ptr) { return (reinterpret_cast<uintptr_t>(ptr) & MARK_MASK) != 0; }
    static T* mark(T* ptr) { return reinterpret_cast<T*>(reinterpret_cast<uintptr_t>(ptr) | MARK_MASK); }
};

// ==========================================
// Part 3: Leader Level (Global Ordered List)
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
        uint64_t loops = 0; // DEBUG LOOP COUNTER

        while (true) {
            // DEBUG WATCHDOG
            if (++loops > 10000000) {
                pipq_debug::report_hang("LeaderList::insert", tid, loops, "Infinite retry in CAS loop");
            }

            EBRGuard guard;
            Node* pred = head.load(std::memory_order_acquire);
            Node* curr = nullptr;

            uint64_t inner_loops = 0;
            while (true) {
                // DEBUG WATCHDOG
                if (++inner_loops > 10000000) {
                     pipq_debug::report_hang("LeaderList::insert_traversal", tid, inner_loops, "Stuck traversing list");
                }

                Node* next_ptr = pred->next.load(std::memory_order_acquire);
                if (TaggedPtr<Node>::is_marked(next_ptr)) {
                    pred = head.load(std::memory_order_acquire);
                    continue; 
                }
                
                curr = TaggedPtr<Node>::get_ptr(next_ptr);
                
                // Helping Logic
                if (curr) {
                    Node* succ_ptr = curr->next.load(std::memory_order_acquire);
                    if (TaggedPtr<Node>::is_marked(succ_ptr)) {
                        Node* succ = TaggedPtr<Node>::get_ptr(succ_ptr);
                        // Try to physically unlink 'curr'
                        pred->next.compare_exchange_strong(curr, succ, std::memory_order_release);
                        // Optimization: Don't restart from head, restart check from pred
                        continue; 
                    }
                }

                if (!curr || curr->priority > priority) {
                    break;
                }
                pred = curr;
            }

            new_node->next.store(curr, std::memory_order_release);
            if (pred->next.compare_exchange_strong(curr, new_node, std::memory_order_release)) {
                return;
            }
            // CAS failed, retry
        }
    }

    std::optional<std::tuple<T, int, int>> try_pop_min() {
        uint64_t loops = 0;
        while (true) {
            if (++loops > 10000000) pipq_debug::report_hang("LeaderList::try_pop_min", -1, loops);

            EBRGuard guard;
            Node* dummy = head.load(std::memory_order_acquire);
            Node* next_ptr = dummy->next.load(std::memory_order_acquire);
            Node* first_real = TaggedPtr<Node>::get_ptr(next_ptr);

            if (!first_real) return std::nullopt;

            if (TaggedPtr<Node>::is_marked(next_ptr)) {
                std::this_thread::yield(); 
                continue; 
            }

            Node* marked_next = TaggedPtr<Node>::mark(first_real);
            if (dummy->next.compare_exchange_strong(first_real, marked_next, std::memory_order_release)) {
                Node* succ = TaggedPtr<Node>::get_ptr(first_real->next.load(std::memory_order_acquire));
                if (head.load() == dummy) {
                     dummy->next.compare_exchange_strong(marked_next, succ, std::memory_order_release);
                }
                auto res = std::make_tuple(std::move(first_real->data), first_real->priority, first_real->tid);
                EBRManager::get().retire(first_real);
                return res;
            }
        }
    }
    
    std::optional<std::pair<T, int>> remove_max_of_thread(int target_tid) {
        uint64_t loops = 0;
        while (true) {
            // DEBUG: This loop is the most likely culprit for livelock in Insert Heavy scenarios
            if (++loops > 10000000) pipq_debug::report_hang("LeaderList::remove_max_of_thread", target_tid, loops);

            EBRGuard guard;
            Node* pred = head.load(std::memory_order_acquire);
            
            Node* target_pred = nullptr;
            Node* target_node = nullptr;

            uint64_t traversal_loops = 0;
            // 1. Scan Phase
            while (true) {
                if (++traversal_loops > 10000000) pipq_debug::report_hang("LeaderList::remove_max_scan", target_tid, traversal_loops);

                Node* next_ptr = pred->next.load(std::memory_order_acquire);
                if (TaggedPtr<Node>::is_marked(next_ptr)) {
                     // Should restart from head to be safe
                     pred = head.load(std::memory_order_acquire);
                     target_pred = nullptr; target_node = nullptr;
                     continue;
                }
                
                Node* curr = TaggedPtr<Node>::get_ptr(next_ptr);
                if (!curr) break; // End of list

                // Helping
                Node* succ_ptr = curr->next.load(std::memory_order_acquire);
                if (TaggedPtr<Node>::is_marked(succ_ptr)) {
                    Node* succ = TaggedPtr<Node>::get_ptr(succ_ptr);
                    pred->next.compare_exchange_strong(curr, succ, std::memory_order_release);
                    continue; // Retry with new next
                }

                if (curr->tid == target_tid) {
                    target_pred = pred;
                    target_node = curr;
                }

                pred = curr;
            }

            if (!target_node) return std::nullopt;

            // 2. Delete Phase
            Node* node_next_ptr = target_node->next.load(std::memory_order_acquire);
            if (TaggedPtr<Node>::is_marked(node_next_ptr)) {
                continue; // Already deleted, retry scan
            }
            
            Node* marked_node_next = TaggedPtr<Node>::mark(TaggedPtr<Node>::get_ptr(node_next_ptr));
            
            // Logical Delete
            if (target_node->next.compare_exchange_strong(node_next_ptr, marked_node_next)) {
                // Physical Delete Attempt
                // Note: 'target_pred' might have been deleted or changed, but 'target_pred->next' CAS check protects us
                // If this fails, next traversal will clean it up via Helping logic
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
// Part 4: Worker Level 
// ==========================================

template<typename T>
class WorkerHeap {
    struct Item {
        T data;
        int priority;
        bool operator>(const Item& other) const { return priority > other.priority; }
    };

    std::priority_queue<Item, std::vector<Item>, std::greater<Item>> pq;
    mutable std::mutex mtx; 

public:
    void push(T val, int priority) {
        std::lock_guard<std::mutex> lock(mtx);
        pq.push({std::move(val), priority});
    }

    std::optional<std::pair<T, int>> pop() {
        std::lock_guard<std::mutex> lock(mtx);
        if (pq.empty()) return std::nullopt;
        Item top = pq.top();
        pq.pop();
        return std::make_pair(std::move(top.data), top.priority);
    }

    std::optional<int> min_priority() const {
        std::lock_guard<std::mutex> lock(mtx);
        if (pq.empty()) return std::nullopt;
        return pq.top().priority;
    }
};

// ==========================================
// Part 5: PIPQ Main Class
// ==========================================

template<typename T>
class PIPQ {
    static constexpr int CNTR_MIN = 10;
    static constexpr int CNTR_MAX = 100;
    
    LeaderList<T> leader_list;
    std::vector<std::unique_ptr<WorkerHeap<T>>> worker_heaps;
    std::mutex workers_mtx; 
    std::mutex coordinator_mtx;

    struct ThreadContext {
        int tid;
        int l_count; 
        WorkerHeap<T>* local_heap;
        ThreadContext() : tid(-1), l_count(0), local_heap(nullptr) {}
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
            ctx.local_heap = heap.get();
            
            std::lock_guard<std::mutex> lock(workers_mtx);
            if (worker_heaps.size() <= (size_t)ctx.tid) {
                worker_heaps.resize(ctx.tid + 1);
            }
            worker_heaps[ctx.tid] = std::move(heap);
        }
    }

public:
    PIPQ() = default;

    void push(T val, int priority) {
        ensure_registered();
        auto& ctx = get_ctx();

        std::optional<int> local_min = ctx.local_heap->min_priority();
        bool fast_path = false;
        
        if (local_min.has_value()) {
            if (priority >= local_min.value()) fast_path = true;
        }

        if (fast_path) {
            ctx.local_heap->push(std::move(val), priority);
        } else {
            if (ctx.l_count < CNTR_MAX) {
                leader_list.insert(std::move(val), priority, ctx.tid);
                ctx.l_count++;
            } else {
                leader_list.insert(std::move(val), priority, ctx.tid);
                auto max_node = leader_list.remove_max_of_thread(ctx.tid);
                if (max_node) {
                    ctx.local_heap->push(std::move(max_node->first), max_node->second);
                } else {
                    ctx.l_count++;
                }
            }
        }
    }

    std::optional<T> try_pop() {
        ensure_registered();
        auto& ctx = get_ctx();
        std::lock_guard<std::mutex> lock(coordinator_mtx);

        auto res_tuple = leader_list.try_pop_min();
        
        if (res_tuple) {
            auto [data, prio, origin_tid] = *res_tuple;
            if (origin_tid == ctx.tid) ctx.l_count--;
            return data;
        } 
        
        int best_tid = -1;
        int best_prio = std::numeric_limits<int>::max();

        {
            std::lock_guard<std::mutex> w_lock(workers_mtx); 
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
                if (best_tid == ctx.tid) ctx.l_count++;
                auto retry = leader_list.try_pop_min();
                if (retry) return std::get<0>(*retry);
            }
        }

        return std::nullopt;
    }
};

} // namespace pipq

#endif