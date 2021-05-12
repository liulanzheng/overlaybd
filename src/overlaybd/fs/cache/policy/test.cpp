#include "lru.h"
#include <gtest/gtest.h>
#include "../../../alog.h"
using namespace std;


class LRU : public ::FileSystem::LRU<int>
{
public:
    vector<int> get()
    {
        vector<int> r;
        if (size() < 1 || empty())
            return r;

        auto i = m_head;
        do
        {
            r.push_back(PTR(i)->val);
            i = PTR(i)->next;
        }while(i != PTR(m_head)->prev);
        return r;
    }
    void print()
    {
        auto r = get();
        for (auto x: r)
            printf("%d ", x);
        printf("\n");
    }
};


TEST(LRU, test)
{
    LRU lru;
    auto i1 = lru.push_front(1);
    auto i3 = lru.push_front(3);
    auto i5 = lru.push_front(5);
    auto i7 = lru.push_front(7);
    EXPECT_EQ(lru.get(), (vector<int>{7,5,3,1}));
    lru.print();

    lru.use(i5);
    EXPECT_EQ(lru.get(), (vector<int>{5,7,3,1}));
    lru.print();

    lru.use(i1);
    EXPECT_EQ(lru.get(), (vector<int>{1,5,7,3}));
    lru.print();

    lru.remove(i7);
    EXPECT_EQ(lru.get(), (vector<int>{1,5,3}));
    lru.print();

    EXPECT_EQ(lru.size(), 3u);

    EXPECT_EQ(lru.front(), 1);
    EXPECT_EQ(lru.back(), 3);

    lru.pop_back();
    EXPECT_EQ(lru.get(), (vector<int>{1, 5}));
    lru.print();

    EXPECT_EQ(lru.size(), 2u);

    auto i9 = lru.push_front(9);
    EXPECT_EQ(lru.get(), (vector<int>{9,1,5}));

    auto i11 = lru.push_front(11);
    EXPECT_EQ(lru.get(), (vector<int>{11,9,1,5}));
    (void)i3;
    (void)i9;
    (void)i11;

    lru.mark_key_cleared(i11);
    lru.mark_key_cleared(i9);
    lru.mark_key_cleared(i1);
    EXPECT_FALSE(lru.empty());
    lru.mark_key_cleared(i5);
    EXPECT_TRUE(lru.empty());
    EXPECT_EQ(4u, lru.size());
}


 int main(int argc, char** argv)
 {
   ::testing::InitGoogleTest(&argc, argv);
   log_output_level = 0;

   auto ret = RUN_ALL_TESTS();
   return ret;
 }
