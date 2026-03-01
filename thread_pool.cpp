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
#include <iostream>
#include <future>
#include <list>
#include <functional>
#include <memory>
#include <cstring>

using namespace std;

struct Task {
    string name;
    function<void()> void_task;
    short importance;
};

// Hazard Pointer 结构体
struct HazardPointer {
    atomic<void*> ptr{nullptr};
    atomic<bool> active{false};
    thread::id owner_tid{};
};

// Michael Scott 无锁队列节点
template<typename T>
struct MSQueueNode {
    atomic<MSQueueNode*> next;
    T data;
    
    MSQueueNode() : next(nullptr), data{} {}
    explicit MSQueueNode(T const& data_) : next(nullptr), data(data_) {}
    explicit MSQueueNode(T&& data_) : next(nullptr), data(std::move(data_)) {}
};

// Michael Scott 无锁队列实现
template<typename T>
class MichaelScottQueue {
private:
    atomic<MSQueueNode<T>*> head;
    atomic<MSQueueNode<T>*> tail;
    
    // Hazard Pointer 管理
    static const int MAX_HAZARD_PTRS = 100;
    static HazardPointer hazard_ptrs[MAX_HAZARD_PTRS];
    static atomic<int> free_hazard_ptr_index;
    
    // 获取空闲的hazard pointer索引
    static int acquire_hazard_pointer() {
        int index;
        do {
            index = free_hazard_ptr_index.load();
            if (index >= MAX_HAZARD_PTRS) return -1;
        } while (!free_hazard_ptr_index.compare_exchange_weak(index, index + 1));
        
        hazard_ptrs[index].owner_tid = this_thread::get_id();
        hazard_ptrs[index].active.store(true);
        return index;
    }
    
    // 设置hazard pointer指向某个指针
    static void set_hazard_pointer(int idx, MSQueueNode<T>* ptr) {
        hazard_ptrs[idx].ptr.store(ptr);
    }
    
    // 释放hazard pointer
    static void release_hazard_pointer(int idx) {
        hazard_ptrs[idx].active.store(false);
        hazard_ptrs[idx].ptr.store(nullptr);
    }
    
    // 检查指针是否被其他线程用作hazard pointer
    static bool outstanding_hazard_ptr(MSQueueNode<T>* p) {
        for (int i = 0; i < MAX_HAZARD_PTRS; ++i) {
            if (hazard_ptrs[i].active.load() && 
                hazard_ptrs[i].ptr.load() == p &&
                hazard_ptrs[i].owner_tid != this_thread::get_id()) {
                return true;
            }
        }
        return false;
    }
    
    // 垃圾回收列表
    struct ReclaimList {
        atomic<MSQueueNode<T>*> head{nullptr};
        
        void add(MSQueueNode<T>* node) {
            MSQueueNode<T>* old_head = head.load();
            do {
                node->next.store(old_head);
            } while (!head.compare_exchange_weak(old_head, node));
        }
        
        void reclaim_not_hazardous() {
            MSQueueNode<T>* current = head.load();
            MSQueueNode<T>* last_safe = nullptr;
            
            while (current) {
                if (!outstanding_hazard_ptr(current)) {
                    MSQueueNode<T>* next = current->next.load();
                    if (last_safe) {
                        last_safe->next.store(next);
                    } else {
                        head.store(next);
                    }
                    delete current;
                    current = next;
                } else {
                    last_safe = current;
                    current = current->next.load();
                }
            }
        }
    };
    
    static ReclaimList reclaim_list;

public:
    MichaelScottQueue() {
        MSQueueNode<T>* dummy = new MSQueueNode<T>();
        head.store(dummy);
        tail.store(dummy);
    }
    
    ~MichaelScottQueue() {
        // 清理所有节点
        while (MSQueueNode<T>* const old_head = head.load()) {
            head.store(old_head->next.load());
            delete old_head;
        }
    }
    
    void push(T const& data) {
        MSQueueNode<T>* new_node = new MSQueueNode<T>(data);
        MSQueueNode<T>* prev_tail = tail.load();
        MSQueueNode<T>* null_node = nullptr;
        
        // 循环直到成功更新tail指针
        while (!tail.compare_exchange_weak(prev_tail, new_node)) {
            // 如果tail指针被其他线程修改，继续尝试
        }
        
        // 将新节点链接到旧的tail节点后面
        prev_tail->next.store(new_node);
    }
    
    bool try_pop(T& result) {
        int my_hp = acquire_hazard_pointer();
        if (my_hp < 0) return false;
        
        MSQueueNode<T>* old_head = head.load();
        MSQueueNode<T>* temp;
        
        do {
            // 设置hazard pointer保护head节点
            set_hazard_pointer(my_hp, old_head);
            old_head = head.load();
            temp = old_head->next.load();
            
            // 检查head节点是否被其他线程修改
            if (temp == nullptr) {
                release_hazard_pointer(my_hp);
                return false; // 队列为空
            }
        } while (!head.compare_exchange_strong(old_head, temp));
        
        // 成功移除节点，获取数据
        result = temp->data;
        
        // 检查是否有其他线程正在使用old_head节点
        if (!outstanding_hazard_ptr(old_head)) {
            delete old_head;
        } else {
            // 将节点添加到垃圾回收列表
            reclaim_list.add(old_head);
            // 尝试回收非危险节点
            reclaim_list.reclaim_not_hazardous();
        }
        
        release_hazard_pointer(my_hp);
        return true;
    }
    
    bool empty() const {
        MSQueueNode<T>* current_head = head.load();
        MSQueueNode<T>* current_next = current_head->next.load();
        return current_next == nullptr;
    }
    
    // 获取队列大小（近似值，因为是无锁的）
    size_t approximate_size() const {
        size_t count = 0;
        MSQueueNode<T>* current = head.load()->next.load();
        while (current != nullptr) {
            ++count;
            current = current->next.load();
        }
        return count;
    }
};

// 为静态成员变量定义空间
template<typename T>
HazardPointer MichaelScottQueue<T>::hazard_ptrs[MichaelScottQueue<T>::MAX_HAZARD_PTRS];

template<typename T>
atomic<int> MichaelScottQueue<T>::free_hazard_ptr_index{0};

template<typename T>
typename MichaelScottQueue<T>::ReclaimList MichaelScottQueue<T>::reclaim_list;

class ThreadPool {
private:
    // 使用无锁队列存储任务，按优先级排序
    struct PriorityTask {
        Task task;
        int priority;
        
        PriorityTask(Task t, int p) : task(std::move(t)), priority(p) {}
    };
    
    // 优先级队列实现（使用多个无锁队列）
    static const int NUM_PRIORITY_LEVELS = 10;
    MichaelScottQueue<PriorityTask> priority_queues[NUM_PRIORITY_LEVELS];
    
    vector<unique_ptr<atomic<bool>>> Worker_activate;
    atomic<bool> Monitor_active{true};
    
    mutex pool_mtx;
    condition_variable task_cv;
    vector<thread> worker_thread;	
    thread monitor_thread;
    
    bool lock_threads = false;
    
    struct alignas(128) alignas(atomic<size_t>) alignas(atomic<bool>) InfoTask {
        mutex task_mtx;
        bool is_read = false;
        short Importance = 0;
        chrono::steady_clock::time_point start_time;   // epoch ms
        char task_name[72]{0};               // 0-terminated
    };
    
    vector<unique_ptr<InfoTask>> Worker_Task_Info;

    // 从优先级获取队列索引
    int get_priority_queue_index(short importance) const {
        // 映射重要性到队列索引 (0-9)，重要性高的放在前面
        int index = (importance >= 0) ? min(importance / 10, NUM_PRIORITY_LEVELS - 1) : 0;
        return NUM_PRIORITY_LEVELS - 1 - index; // 高优先级对应小索引
    }

    virtual void worker_loop(int worker_id) {
        while(*Worker_activate[worker_id]) {
            PriorityTask task_data;
            bool found_task = false;
            
            // 尝试从各个优先级队列中获取任务（从高优先级开始）
            for (int priority_idx = 0; priority_idx < NUM_PRIORITY_LEVELS; ++priority_idx) {
                if (priority_queues[priority_idx].try_pop(task_data)) {
                    found_task = true;
                    break;
                }
            }
            
            if (!found_task) {
                // 没有任务时短暂等待
                this_thread::sleep_for(chrono::microseconds(100));
                continue;
            }
            
            if (!(*Worker_activate[worker_id])) {
                return;
            }
            
            // 存储任务信息
            {
                unique_lock<mutex> lock_info(Worker_Task_Info[worker_id]->task_mtx);
                strncpy(Worker_Task_Info[worker_id]->task_name, task_data.task.name.c_str(), 71);
                Worker_Task_Info[worker_id]->task_name[71] = '\0';
                Worker_Task_Info[worker_id]->Importance = task_data.priority;
                Worker_Task_Info[worker_id]->start_time = chrono::steady_clock::now();
                Worker_Task_Info[worker_id]->is_read = false;
            }
            
            // 执行任务
            task_data.task.void_task();
        }
    }

    virtual void monitor_loop() {
        while(Monitor_active) {
            this_thread::sleep_for(chrono::milliseconds(100)); // 每100ms检查一次
            
            if(!lock_threads) {
                Adjust_threads();
            }
            
            Save_log();
        }
    }

public:
    ThreadPool(int initial_worker_number, bool lock_thread = false) {
        Worker_Task_Info.reserve(50);
        for(int i = 0; i < initial_worker_number; ++i) {
            add_work_thread();
        }
        change_lock_thread_quantity(lock_thread);
        monitor_thread = thread([this]{ monitor_loop(); });
    }
    
    ~ThreadPool() {
        clear();
    }

    virtual void Adjust_threads() {
        size_t total_tasks = 0;
        for (int i = 0; i < NUM_PRIORITY_LEVELS; ++i) {
            total_tasks += priority_queues[i].approximate_size();
        }
        
        if(total_tasks > worker_thread.size() * 3 && 
           worker_thread.size() < thread::hardware_concurrency() * 1.5 && 
           worker_thread.size() < 50) {
            add_work_thread();
        }
        else if(total_tasks < worker_thread.size() && worker_thread.size() > 4) {
            remove_work_thread();
        }
    }

    virtual void Save_log() {
        ofstream ofs;
        ofs.open("log.txt", ios::app);
        
        for(size_t i = 0; i < Worker_Task_Info.size(); ++i) {
            if (i >= Worker_Task_Info.size()) break;
            
            unique_lock<mutex> lock(Worker_Task_Info[i]->task_mtx);
            if(!Worker_Task_Info[i]->is_read) {
                chrono::steady_clock::time_point tp = Worker_Task_Info[i]->start_time;
                auto rel_ms = chrono::duration_cast<chrono::milliseconds>(tp.time_since_epoch()).count();
                
                ofs << "Thread " << i << " running function " 
                    << Worker_Task_Info[i]->task_name 
                    << "(importance:" << Worker_Task_Info[i]->Importance 
                    << ") at " << rel_ms << "ms" << endl;
                
                Worker_Task_Info[i]->is_read = true;
                
                if(chrono::steady_clock::now() - Worker_Task_Info[i]->start_time > 10s) {
                    auto elapsed = chrono::steady_clock::now() - Worker_Task_Info[i]->start_time;
                    ofs << "Attention! Function " << Worker_Task_Info[i]->task_name 
                        << " has run " << chrono::duration_cast<chrono::seconds>(elapsed).count() 
                        << " seconds" << endl;
                }
            }
        }
        ofs.close();
    }

    int the_thread_size() {
        return static_cast<int>(worker_thread.size());
    }

    bool add_work_thread() {
        unique_lock<mutex> lock(pool_mtx);
        Worker_activate.emplace_back(make_unique<atomic<bool>>(true));
        Worker_Task_Info.emplace_back(make_unique<InfoTask>());
        auto worker_id = static_cast<int>(worker_thread.size());
        worker_thread.emplace_back(thread(&ThreadPool::worker_loop, this, worker_id));
        return true;
    }

    bool remove_work_thread() {
        if (worker_thread.empty()) return false;
        
        *Worker_activate.back() = false;
        task_cv.notify_all();
        
        if(worker_thread.back().joinable()) {
            worker_thread.back().join();
            worker_thread.pop_back();
            Worker_activate.pop_back();
            Worker_Task_Info.pop_back();
        }
        return true;
    }

    void clear() {
        Monitor_active = false;
        if(monitor_thread.joinable()) {
            monitor_thread.join();
        }
        
        unique_lock<mutex> lock(pool_mtx);
        while(!worker_thread.empty()) {
            remove_work_thread();
        }
    }

    bool change_lock_thread_quantity(bool set) {
        lock_threads = set;
        return lock_threads;
    }

    template<typename F>
    auto submit(F&& f, std::string function_name, short Importance) -> future<decltype(f())> {
        using ReturnType = decltype(f()); 
        
        auto user_task = make_shared<packaged_task<ReturnType()>>(forward<F>(f));
        future<ReturnType> task_future = user_task->get_future();
        
        function<void()> wrapper = [user_task]() { 
            (*user_task)(); 
        };
        
        Task task_obj{move(function_name), wrapper, Importance};
        PriorityTask priority_task(move(task_obj), Importance);
        
        // 根据重要性选择队列
        int queue_idx = get_priority_queue_index(Importance);
        priority_queues[queue_idx].push(move(priority_task));
        
        return task_future; 	
    }
    
    // 获取队列中的总任务数
    size_t get_total_tasks() const {
        size_t total = 0;
        for (int i = 0; i < NUM_PRIORITY_LEVELS; ++i) {
            total += priority_queues[i].approximate_size();
        }
        return total;
    }
};



