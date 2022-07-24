#include <iostream>
#include <chrono>
#include <vector>
#include <cstdint>
#include <cstring>
using namespace std;

int main(int, char**) {
  const size_t SIZE = 0x40000000; //1GiB
  vector<uint8_t> src(SIZE);
  vector<uint8_t> dest(SIZE);
  auto start = chrono::steady_clock::now();
  memcpy(&dest[0], &src[0], src.size());
  auto end = chrono::steady_clock::now();
  chrono::duration<double> d = end - start;
  cout << d.count() << endl;
  return 0;
}
