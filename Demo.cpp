#include"thread_pool.cpp"
#include<iostream>
using namespace std;

int func1(int a)
{
	cout<<"Here func2 receive "<<a<<endl;
	int sum=0;
	for(int i=1;i<=a;++i)
		sum+=i;
	cout<<"Calculating sum "<<sum<<endl;
	return sum;
}

int main()
{
    ThreadPool threadpool(4);
    vector<future<int>> results;
    for (int i = 0; i < 5000; ++i) {
        results.push_back(
            threadpool.submit([=]() { return func1(100000); }, "Function 1 for calculating"s, i % 4)
        );
    }
    for (auto& f : results) f.get();
    return 0;
}