#include <vector>
#include <new>
#include <iostream>
#include <string>
#include <algorithm>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <fstream>
#include <future>
#include <list>
#include <functional>
#include <memory>
#include <cstring>
#include <cassert>

using namespace std;

struct Task {
    string name;
    function<void()> void_task;
    short importance;
};

// ========================
// Epoch-Based Reclamation (EBR)
// ========================
class EBR {
public:
    static constexpr size_t MAX_THREADS = 64; // 支持最多64个工作线程
    static constexpr size_t NUM_EPOCHS = 3;

private:
    struct ThreadRecord {
        atomic<bool> active{false};
        atomic<size_t> epoch{0};
        thread::id tid{};
    };

    static ThreadRecord records[MAX_THREADS];
    static atomic<size_t> global_epoch;
    static atomic<size_t> active_threads;

    struct RetiredNode {
        void* ptr;
        size_t epoch;
        function<void()> deleter;
    };
    static vector<RetiredNode> retired_list;
    static mutex retire_mutex;

public:
    class Guard {
        size_t local_epoch;
        size_t index;

    public:
        Guard() {
            // 注册当前线程
            thread::id self = this_thread::get_id();
            for (size_t i = 0; i < MAX_THREADS; ++i) {
                if (!records[i].active.exchange(true)) {
                    records[i].tid = self;
                    records[i].epoch.store(global_epoch.load(memory_order_relaxed), memory_order_relaxed);
                    index = i;
                    break;
                }
            }
            local_epoch = records[index].epoch.load(memory_order_relaxed);
        }

        ~Guard() {
            records[index].active.store(false, memory_order_relaxed);
        }

        void enter() {
            local_epoch = global_epoch.load(memory_order_acquire);
            records[index].epoch.store(local_epoch, memory_order_release);
        }

        void leave() {
            records[index].epoch.store(0, memory_order_relaxed);
        }

        template<typename T>
        void retire(T* ptr, function<void()> deleter = [ptr]() { delete ptr; }) {
            retire_node({ptr, local_epoch, deleter});
        }

    private:
        void retire_node(RetiredNode node) {
            {
                lock_guard<mutex> lk(retire_mutex);
                retired_list.push_back(node);
            }
            try_quiesce();
        }

        void try_quiesce() {
            size_t current_global = global_epoch.load(memory_order_relaxed);
            size_t stable_epoch = current_global;

            // 检查是否有线程仍处于 old 或 older epoch
            bool all_advanced = true;
            for (size_t i = 0; i < MAX_THREADS; ++i) {
                if (!records[i].active.load(memory_order_relaxed)) continue;
                size_t e = records[i].epoch.load(memory_order_relaxed);
                if (e != 0 && e != current_global && e != ((current_global + NUM_EPOCHS - 1) % NUM_EPOCHS)) {
                    all_advanced = false;
                    break;
                }
            }

            if (all_advanced) {
                // 安全回收比 stable_epoch 更旧的节点
                size_t target_epoch = (current_global + NUM_E epochs - 2) % NUM_EPOCHS;
                vector<function<void()>> to_delete;
                {
                    lock_guard<mutex> lk(retire_mutex);
                    auto it = retired_list.begin();
                    while (it != retired_list.end()) {
                        if (it->epoch != target_epoch) {
                            ++it;
                            continue;
                        }
                        to_delete.push_back(it->deleter);
                        it = retired_list.erase(it);
                    }
                }
                for (auto& del : to_delete) del();
            }
        }
    };

    static void advance_epoch() {
        size_t old_epoch = global_epoch.load(memory_order_relaxed);
        size_t new_epoch = (old_epoch + 1) % NUM_EPOCHS;
        global_epoch.store(new_epoch, memory_order_release);
    }

    friend class Guard;
};

// Static definitions
EBR::ThreadRecord EBR::records[EBR::MAX_THREADS];
atomic<size_t> EBR::global_epoch{0};
atomic<size_t> EBR::active_threads{0};
vector<EBR::RetiredNode> EBR::retired_list;
mutex EBR::retire_mutex;

// ========================
// Michael-Scott Queue with EBR
// ========================
template<typename T>
struct MSQueueNode {
    atomic<MSQueueNode*> next{nullptr};
    T data;

    MSQueueNode() = default;
    explicit MSQueueNode(const T& d) : data(d) {}
    explicit MSQueueNode(T&& d) : data(std::move(d)) {}
};

template<typename T>
class MichaelScottQueue {
private:
    alignas(128) atomic<MSQueueNode<T>*> head;
    alignas(128) atomic<MSQueueNode<T>*> tail;

public:
    MichaelScottQueue() {
        MSQueueNode<T>* dummy = new MSQueueNode<T>();
        head.store(dummy, memory_order_relaxed);
        tail.store(dummy, memory_order_relaxed);
    }

    ~MichaelScottQueue() {
        while (auto* p = head.load(memory_order_relaxed)) {
            head.store(p->next.load(memory_order_relaxed), memory_order_relaxed);
            delete p;
        }
    }

    void push(const T& data) {
        MSQueueNode<T>* new_node = new MSQueueNode<T>(data);
        MSQueueNode<T>* prev_tail = tail.load(memory_order_relaxed);

        while (true) {
            MSQueueNode<T>* next = prev_tail->next.load(memory_order_acquire);
            if (next == nullptr) {
                if (prev_tail->next.compare_exchange_weak(next, new_node, memory_order_release)) {
                    break;
                }
            } else {
                // Help update tail
                tail.compare_exchange_weak(prev_tail, next, memory_order_release);
                prev_tail = tail.load(memory_order_relaxed);
            }
        }
        tail.compare_exchange_weak(prev_tail, new_node, memory_order_release);
    }

    bool try_pop(T& result) {
        EBR::Guard guard;
        guard.enter();

        MSQueueNode<T>* old_head = head.load(memory_order_acquire);
        while (true) {
            MSQueueNode<T>* next = old_head->next.load(memory_order_acquire);
            if (next == nullptr) {
                guard.leave();
                return false;
            }
            if (head.compare_exchange_weak(old_head, next, memory_order_acquire)) {
                result = std::move(next->data);
                // Safe to retire old_head after quiescent state
                guard.retire(old_head);
                guard.leave();
                return true;
            }
            // Retry with new old_head
        }
    }

    bool empty() const {
        auto* h = head.load(memory_order_acquire);
        return h->next.load(memory_order_acquire) == nullptr;
    }

    size_t approximate_size() const {
        size_t count = 0;
        auto* curr = head.load(memory_order_acquire)->next.load(memory_order_acquire);
        while (curr && count < 10000) {
            ++count;
            curr = curr->next.load(memory_order_acquire);
        }
        return count;
    }
};

// ========================
// ThreadPool
// ========================
class ThreadPool {
private:
    static const int NUM_PRIORITY_LEVELS = 10;
    alignas(128) array<MichaelScottQueue<typename ThreadPool::PriorityTask>, NUM_PRIORITY_LEVELS> priority_queues;

    struct PriorityTask {
        Task task;
        int priority;
        PriorityTask(Task t, int p) : task(std::move(t)), priority(p) {}
    };

    vector<unique_ptr<atomic<bool>>> Worker_activate;
    atomic<bool> Monitor_active{true};

    mutex pool_mtx;
    condition_variable task_cv;
    vector<thread> worker_thread;
    thread monitor_thread;

    bool lock_threads = false;

    struct alignas(128) InfoTask {
        mutex task_mtx;
        bool is_read = false;
        short Importance = 0;
        chrono::steady_clock::time_point start_time;
        char task_name[72]{0};
    };

    vector<unique_ptr<InfoTask>> Worker_Task_Info;

    int get_priority_queue_index(short importance) const {
        int idx = (importance >= 0) ? min(importance / 10, 9) : 0;
        return 9 - idx; // 高重要性 → 低索引（高优先级）
    }

    void worker_loop(int worker_id) {
        while (*Worker_activate[worker_id]) {
            PriorityTask task_data;
            bool found = false;
            for (int i = 0; i < NUM_PRIORITY_LEVELS; ++i) {
                if (priority_queues[i].try_pop(task_data)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                this_thread::sleep_for(chrono::microseconds(100));
                continue;
            }

            if (!(*Worker_activate[worker_id])) return;

            {
                unique_lock<mutex> lk(Worker_Task_Info[worker_id]->task_mtx);
                strncpy(Worker_Task_Info[worker_id]->task_name, task_data.task.name.c_str(), 71);
                Worker_Task_Info[worker_id]->task_name[71] = '\0';
                Worker_Task_Info[worker_id]->Importance = task_data.priority;
                Worker_Task_Info[worker_id]->start_time = chrono::steady_clock::now();
                Worker_Task_Info[worker_id]->is_read = false;
            }

            try {
                task_data.task.void_task();
            } catch (const exception& e) {
                // 避免线程崩溃
                cerr << "Task '" << task_data.task.name << "' threw: " << e.what() << endl;
            }
        }
    }

    void monitor_loop() {
        while (Monitor_active) {
            this_thread::sleep_for(chrono::milliseconds(100));
            if (!lock_threads) Adjust_threads();
            Save_log();
        }
    }

public:
    ThreadPool(int initial_worker_number, bool lock_thread = false)
        : lock_threads(lock_thread) {
        Worker_Task_Info.reserve(50);
        for (int i = 0; i < initial_worker_number; ++i) {
            add_work_thread();
        }
        monitor_thread = thread(&ThreadPool::monitor_loop, this);
    }

    ~ThreadPool() { clear(); }

    void Adjust_threads() {
        size_t total = 0;
        for (const auto& q : priority_queues) total += q.approximate_size();

        size_t hw_conc = thread::hardware_concurrency();
        size_t max_workers = max(hw_conc * 3 / 2, hw_conc + 4ULL);
        max_workers = min(max_workers, 50ULL);

        if (total > worker_thread.size() * 3 && worker_thread.size() < max_workers) {
            add_work_thread();
        } else if (total < worker_thread.size() && worker_thread.size() > 4) {
            remove_work_thread();
        }
    }

    virtual void Save_log() {
        ofstream ofs("log.txt", ios::app);
        for (size_t i = 0; i < Worker_Task_Info.size(); ++i) {
            unique_lock<mutex> lk(Worker_Task_Info[i]->task_mtx);
            if (!Worker_Task_Info[i]->is_read) {
                auto tp = Worker_Task_Info[i]->start_time;
                auto ms = chrono::duration_cast<chrono::milliseconds>(tp.time_since_epoch()).count();
                ofs << "Thread " << i << " running function "
                    << Worker_Task_Info[i]->task_name
                    << " (importance:" << Worker_Task_Info[i]->Importance
                    << ") at " << ms << "ms\n";

                Worker_Task_Info[i]->is_read = true;

                auto now = chrono::steady_clock::now();
                if (now - Worker_Task_Info[i]->start_time > 10s) {
                    auto elapsed = chrono::duration_cast<chrono::seconds>(now - Worker_Task_Info[i]->start_time);
                    ofs << "Attention! Function " << Worker_Task_Info[i]->task_name
                        << " has run " << elapsed.count() << " seconds\n";
                }
            }
        }
    }

    bool add_work_thread() {
        unique_lock<mutex> lk(pool_mtx);
        Worker_activate.emplace_back(make_unique<atomic<bool>>(true));
        Worker_Task_Info.emplace_back(make_unique<InfoTask>());
        int id = static_cast<int>(worker_thread.size());
        worker_thread.emplace_back(&ThreadPool::worker_loop, this, id);
        return true;
    }

    bool remove_work_thread() {
        if (worker_thread.empty()) return false;
        *Worker_activate.back() = false;
        task_cv.notify_all();
        if (worker_thread.back().joinable()) {
            worker_thread.back().join();
            worker_thread.pop_back();
            Worker_activate.pop_back();
            Worker_Task_Info.pop_back();
        }
        return true;
    }

    void clear() {
        Monitor_active = false;
        if (monitor_thread.joinable()) monitor_thread.join();

        unique_lock<mutex> lk(pool_mtx);
        while (!worker_thread.empty()) remove_work_thread();
    }

    bool change_lock_thread_quantity(bool set) {
        lock_threads = set;
        return lock_threads;
    }

    template<typename F>
    auto submit(F&& f, string name, short importance) -> future<decltype(f())> {
        using R = decltype(f());
        auto task_ptr = make_shared<packaged_task<R()>>(forward<F>(f));
        auto fut = task_ptr->get_future();
        Task t{move(name), [task_ptr]() { (*task_ptr)(); }, importance};
        PriorityTask pt{move(t), importance};
        int idx = get_priority_queue_index(importance);
        priority_queues[idx].push(move(pt));
        return fut;
    }

    size_t get_total_tasks() const {
        size_t total = 0;
        for (const auto& q : priority_queues) total += q.approximate_size();
        return total;
    }
};