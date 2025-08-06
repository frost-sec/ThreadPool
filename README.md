This is a thread pool which use muti-threads to solve abundant tasks.

It is defined as a class,use
ThreadPool threadpool(int initial_worker_number,bool lock_thread=false);
to initial it.
the thread will create initial_worker_number threads.If lock_thread is true, the thread pool willnot change the quantity of worker_threads.

Use threadpool.submit(F&& Function,string Discription,short Importance) to submit tasks to thread_pool.
The Function must be packaged by lambda.The Discription must shorter than 72bytes.The higher importance it is,the faster it will run in the pool.
For example, if you wanna run a function1(),you can submit as following.
threadpool.submit([=](){function1();},"Function 1",4);
If the function has a return value, the threadpool will return a future<ReturnType()>,use following command to get it.
auto result=threadpool.submit([=](){return function1();},"Fcuntion 1",4);
result.get();
Attention,once use result.get(),and the task didn't finish,the thread will be blocked until the task finish.
submit_task--->thread_pool_running--->get_answer
    |--->other_tasks--->answer.future----|---->other_tasks
                                 (blocked)
The threadpool will dynamic change the working threads.
If tasks'quantity is bigger than worker_threads'quantity,and the worker_threads'quantity is smaller than limit,the thread_pool will add worker_threads.
If tasks'quantity is smaller than worker_threads'quantity,and the worker_thread'quantity is bigger than limit,the thread_pool will reduce worker_threads.
However,you can use ThreadPool.add(remove)_work_thread() to change the threads manually.It will return a bool if successful.
Also the thread pool provide a log_save function.It will save the tasks the worker run.(However,some tasks might be loss.Because the worker runs too fast and didn't save)
If you wanna create the threadpool which run in your want,you can create aderived class from ThreadPool.
The worker_loop(for running thread),monitor_loop(for monitor_thread),Adjust_threads(for adjusting the quantity of threads dynamicly),Save_log(for saving the running logs) are all virtual function.

You can visit Demo to check how to use the threadpool
Really sorry that it can't run in VS-Studio which always cast errors while running.However,it works perfectly on Dev-C++ which use gcc.