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

using namespace std;

struct Task {
    string name;
    function<void()> void_task;
    short importance;
};
//Here is the difinition of task.Users have to input:
//The name of Task(Also can be the discription).However, it shouldn't longer than 72 byte.

//Following is the difinition of linked_list which used to store the tasks.
//In fact, it is the same as List. I use the list in order to make sure the tasks are sorted by importance
//Also, the code can be changed to use priority_queue to save task which can achieve the same function
template<typename Object>
class chain_list
{
	private:
		struct Node
		{
			Object data;
			Node* prev;
			Node* next;
			
			Node(const Object& d=Object{},Node* p=nullptr,Node* n=nullptr):data{d},prev{p},next{n} {}
			Node(Object&& d,Node* p=nullptr,Node* n=nullptr):data{std::move(d)},prev{p},next{n} {}
		};
		
	public:
		//Following is the difinition of iterator and const_iterator.
		//In fact,most of them are the same. I think it can be also acheived by using const_cast???
		class const_iterator
		{
			public:
				const_iterator():current{nullptr} {}
					
				const Object& operator*() const
				{	return retrieve();}
					
				const_iterator& operator++()
				{
					current=current->next;
					return *this;
				}
					
				const_iterator operator++(int)
				{
					const_iterator old=*this;
					++(*this);
					return old;
				}
				
				bool operator==(const const_iterator& rhs) const
				{
					return current==rhs.current;
				}
					
				bool operator!=(const const_iterator& rhs) const
				{
						return !(*this==rhs);
				}
					
			protected:
				Node* current;
					
				Object& retrieve() const
				{
					return current->data;
				}
					
				const_iterator(Node* p):current{p} {}
					
				//Make sure the list can visit the iterator
				friend class chain_list<Object>;
		};
		
		//Most of them are the same
		class iterator:public const_iterator
		{
			public:
				iterator() {}
					
				Object& operator*()
				{
					return const_iterator::retrieve();
				}
					
				const Object& operator*() const
				{
					return const_iterator::operator*();
				}
					
				iterator& operator++()
				{
					this->current=this->current->next;
					return *this;
				}
					
				iterator operator++(int)
				{
					iterator old=*this;
					++(*this);
					return old;
				}
				
			protected:
				iterator(Node* p):const_iterator{p} {}
					
				friend class chain_list<Object>;
		};
			
	public:
		chain_list()
		{
			init();
		}
		
		~chain_list()
		{
			clear();
			delete head;
			delete tail;
		}
		
		//Copy constructor
		//Receive a list and push_back every element of it
		chain_list(const chain_list& rhs)
		{
			init();
			for(auto& x:rhs)
				push_back(x);
		}
		
		//Copy-And-Swap
		chain_list& operator=(const chain_list& rhs)
		{
			chain_list copy=rhs;
			std::swap(*this,copy);
			return *this;
		}
		
		//Move constructor
		//Attention:The move constructor must be subject to the ownership of the other party.
		//So it's faster than Copy constructor
		//Once used move constructor, the old one will be useless and CAN'T be visited anymore.
		chain_list(chain_list&& rhs):theSize{rhs.theSize},head{rhs.head},tail{rhs.tail}
		{
			rhs.theSize=0;
			rhs.head=nullptr;
			rhs.tail=nullptr;
		}
		
		chain_list& operator=(chain_list&& rhs)
		{
			std::swap(theSize,rhs.theSize);
			std::swap(head,rhs.head);
			std::swap(tail,rhs.tail);
			
			return *this;
		}
		
		iterator begin()
		{
			return {head->next};
		}
		
		const_iterator begin() const
		{
			return {head->next};
		}
		
		iterator end()
		{
			return {tail};
		}
		
		const_iterator end() const
		{
			return {tail};
		}
		
		int size() const
		{
			return theSize;
		}
		
		bool empty() const
		{
			return size()==0;
		}
		
		void clear()
		{
			while(!empty())
				pop_front();
		}
		
		Object& front()
		{
			return *begin();
		}
		
		const Object& front() const
		{
			return *begin();
		}
		
		Object& back()
		{
			return *--end();
		}
		
		const Object& back() const
		{
			return *--end();
		}
		
		void push_front(const Object& x)
		{
			insert(begin(),x);
		}
		
		void push_front(Object&& x)
		{
			insert(begin(),std::move(x));
		}
		
		void push_back(const Object& x)
		{
			insert(end(),x);
		}
		
		void push_back(Object&& x)
		{
			insert(end(),std::move(x));
		}
		
		void pop_front()
		{
			erase(begin());
		}
		
		void pop_back()
		{
			erase(--end());
		}
		
		iterator insert(iterator itr, const Object& x)
		{
			Node* p=itr.current;
			theSize++;
			return {p->prev = p->prev->next = new Node{ x,p->prev,p}};
		}
		
		iterator insert(iterator itr, Object&& x)
		{
			Node* p=itr.current;
			theSize++;
			return {p->prev = p->prev->next = new Node{std::move(x),p->prev,p}};
		}
		
		iterator erase(iterator itr)
		{
			Node* p=itr.current;
			iterator retVal { p->next};
			p->prev->next=p->next;
			p->next->prev=p->prev;
			
			delete p;
			theSize--;
			
			return retVal;
		}
		
		iterator erase(iterator from,iterator to)
		{
			for(iterator itr=from;itr!=to;)
				itr=erase(itr);
				
			return to;
		}
	
	private:
		int theSize;
		Node* head;
		Node* tail;
				
		void init()
		{
			theSize=0;
			head=new Node;
			tail=new Node;
			head->next=tail;
			tail->prev=head;
		}
};

class ThreadPool {
    private:
        chain_list<Task> tasks;
		//Used to store tasks

        vector<unique_ptr<atomic<bool>>> Worker_activate;
		//The signal of working thread.
		//If it need to stop,set it false.
		//However,the atomic can't be moved or copied.And the worker_activate is dynamic resizing
		//So I used pointer to avoid moving or copy the atomic

        atomic<bool> Monitor_active=true;
		//The signal of monitor thread.Only set false when the threadpool closed.

        mutex task_mtx,pool_mtx;
		//task_mtx used to lock the task
		//pool_mtx used to lock the pool when adjust the quantity of threads.

        condition_variable task_cv;
		//Used to block the thread when task is empty.

        vector<thread> worker_thread;	
        thread monitor_thread;
		//The worker_threads and monitor_thread

        bool lock_threads=false;
		//To lock the quantity of threads
		//Once set true,the monitor thread won't adjust the threads.

        struct alignas(128) alignas(atomic<size_t>) alignas(atomic<bool>) InfoTask 
        {
        	mutex task_mtx;
            bool is_read=false;
            short Importance;
            chrono::steady_clock::time_point start_time;;   // epoch ms
            char task_name[72]{0};               // 0-terminated
        };
        //static_assert(sizeof(InfoTask) == 128);

		//To avoid Pseudo alignment, use alignas to make sure each InfoTask place the front of 128byte.
		//If wanna more strictness, use static_assert(sizeof(InfoTask))==2^m

        vector<unique_ptr<InfoTask>> Worker_Task_Info;
        //store the information of running task

        virtual void worker_loop(int worker_id)
        {
            while(*Worker_activate[worker_id])
            {
				//1.Get task. If empty,blocked.(Attention,also check the worker_activate 
				//otherwise it can't receive closing signals while waiting for tasks)
				//2.Get task and save its Infomation.Use std::move and unlock immediately
				//When saving the Infomation,remember to lock.
				//3.Running the task.

                unique_lock<mutex> lock(task_mtx);
                task_cv.wait(lock,[&]{return !tasks.empty()||!(*Worker_activate[worker_id]);});
                //Hang up while task empty;
                if(!(*Worker_activate[worker_id]))
                    return; //Check whether get task or need to stop
				if(tasks.empty())
					continue;

				//Get the task
                auto Work_Task=move(tasks.front());
                tasks.pop_front();
                lock.unlock();
   
                //store the task information
				unique_lock<mutex> lock_info(Worker_Task_Info[worker_id]->task_mtx);
				strcpy_s(Worker_Task_Info[worker_id]->task_name, 64, Work_Task.name.c_str());
				Worker_Task_Info[worker_id]->Importance = Work_Task.importance;
				Worker_Task_Info[worker_id]->start_time = chrono::steady_clock::now();
				Worker_Task_Info[worker_id]->is_read = false;
				lock_info.unlock();

                //running the task;
                (Work_Task.void_task)();
            }
        }

        virtual void monitor_loop()
        {
            while(Monitor_active) 
            {
				//save the log of running tasks.
				//Users can overwrite monitor loop or Save_log or adjust threads
				Save_log();
				//dynamic adjust the quantity of work_thread.
                if(!lock_threads)
					Adjust_threads();
            }
        }

    public:
        ThreadPool(int initial_worker_number,bool lock_thread=false)
        {
			this->Worker_Task_Info.reserve(50);
            for(int i=0;i<initial_worker_number;++i)
                add_work_thread();
			change_lock_thread_quantity(lock_thread);
			monitor_thread = thread([this]{ monitor_loop(); });
        }
        ~ThreadPool()
        {
        	clear();
		}

		virtual void Adjust_threads()
		{
			//Adjust the quantity of threads.
			//Overwrite it if you want.
			if(tasks.size()>worker_thread.size()*3&&worker_thread.size()<thread::hardware_concurrency() * 1.5&&worker_thread.size()<50)
            {
                add_work_thread();
            }
            else if(tasks.size()<worker_thread.size()&&worker_thread.size()>4)
            {
                remove_work_thread();
            }
		}

		virtual void Save_log()
		{
			//Output the log
			//All the information are from worker_threads
            ofstream ofs;
            ofs.open("log.txt", ios::app);
                
			//Check every threads' work_information
            for(int i=0;i<Worker_Task_Info.size();++i)
            {
                unique_lock<mutex> lock(Worker_Task_Info[i]->task_mtx);
                if(!Worker_Task_Info[i]->is_read)
                {
                    chrono::steady_clock::time_point tp = Worker_Task_Info[i]->start_time;
					auto rel_ms = chrono::duration_cast<chrono::milliseconds>(tp.time_since_epoch()).count();
						
                    ofs<<"Thread "<<i<<" running function "<<Worker_Task_Info[i]->task_name<<"(importance:"<<Worker_Task_Info[i]->Importance<<")"<<" at "<< rel_ms <<"ms"<<endl;
                        
                    Worker_Task_Info[i]->is_read=true;
					//Once it have running too long, warning
                    if(chrono::steady_clock::now()-Worker_Task_Info[i]->start_time>10s)
                    {
                    	auto elapsed = chrono::steady_clock::now() - Worker_Task_Info[i]->start_time;
                        ofs<<"Attention!Function "<<Worker_Task_Info[i]->task_name<<" has run"<<chrono::duration_cast<chrono::seconds>(elapsed).count()<<endl;
                    }
                }
                    
                lock.unlock();
            }
            ofs.close();
		}

		int the_thread_size()
		{
			return worker_threads.size();
		}

        bool add_work_thread()
        {
			//Lock first.
			//Not only add the thread,also add the worker_task_info,worker_activate signal.
			unique_lock<mutex> lock(pool_mtx);
			Worker_activate.emplace_back(make_unique<atomic<bool>>(true));
			this->Worker_Task_Info.emplace_back(make_unique<InfoTask>());
			auto worker_id = worker_thread.size();
			worker_thread.emplace_back([this, worker_id] {worker_loop(worker_id); });
			lock.unlock();
			return true;
        }

        bool remove_work_thread()
        {
            *Worker_activate.back()=false;
            task_cv.notify_all();
			//notify it to avoid blocking.
            if(worker_thread.back().joinable())
            {
                worker_thread.back().join();
                worker_thread.pop_back();
                Worker_activate.pop_back();
                Worker_Task_Info.pop_back();
            }
            return true;
        }

        void clear()
        {
			Monitor_active=false;
        	if(monitor_thread.joinable())
        		monitor_thread.join();
        	unique_lock<mutex> lock(pool_mtx);
            while(!worker_thread.empty())
                remove_work_thread();
        }

        bool change_lock_thread_quantity(bool set)
        {
            return lock_threads=set;
        }

        //package the submit
        template<typename F>
        auto package(F&& f)
        {
            auto task=make_shared<packaged_task<decltype(f())>>(forward<F>(f()));
            return task;
        }

		//Attention
		//In order to cheive returning value from thread_pool,Must use packaged_task to package the task!!!
		//However,the task use function<void> to save the function, which means the return value will lose when saving
		//So we have to package the packaged task again into function<void()>!!!

		//function<?>->packaged_task<?>->function<void()>(running in the thread_pool)
		//					|                 |(a pointer inside,point to the packaged_task)
		//                  |                 |(Once finish,send the answer to the future.)
		//                  ----------------------------> future<?>(return value)
        template<typename F>
        auto submit(F&& f,std::string function_name,short Importance)->future<decltype(f())>
        {
            using ReturnType = decltype(f()); 
            
			//Get the packaged_task
            auto user_task = make_shared<packaged_task<ReturnType()>>(forward<F>(f));
            
            future<ReturnType> task_future = user_task->get_future();
            
			//packaged it into function<void>
            function<void()> wrapper = [user_task]() { (*user_task)(); };
            
            Task task_obj{move(function_name),wrapper,Importance};

			//Insert the task.
			//The more important it is, the earlier it should be placed.
            unique_lock<mutex> lock(task_mtx);
                
            auto it = tasks.begin();
            while (it != tasks.end() && (*it).importance > Importance) {
                ++it;
            }
            tasks.insert(it, move(task_obj));
                
            lock.unlock();
                
            task_cv.notify_one();

            //the return value is a future!!!
			//use return_value.get() to get the answer.
            return task_future; 	
        }
};