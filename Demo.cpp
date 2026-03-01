#include"thread_pool.cpp"
#include<iostream>
#include <vector>
#include <random>
using namespace std;

bool func1(vector<int> a)
{
	sort(a.begin(),a.end());
	return true;
}

int main()
{
	const std::size_t N = 100000;
    std::vector<int> v(N);

    std::random_device rd;           
    std::mt19937_64 gen(rd());       
    std::uniform_int_distribution<int> dis; 

    for (auto& x : v) x = dis(gen);
    
    ThreadPool threadpool(4);
    vector<future<bool>> results;
    for (int i = 0; i < 5000; ++i) {
        results.push_back(
            threadpool.submit([=]() {return func1(v);}, "Function 1 for sorting"s, i % 4)
        );
    }
    return 0;
}
