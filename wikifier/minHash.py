from datasketch import MinHash
from collections import defaultdict
import os, pickle


# input IDs, return MinHash Object
def minhash_IDs(IDs):
    m = MinHash()
    for ID in IDs:
        m.update(ID.encode('utf-8'))
    return m


if __name__ == "__main__":
    # iterate through this array, do minhash
    filenames = os.listdir("./results_2.0")
    minhash_list = []
    rank_list = []
    for idx,filename in enumerate(filenames):
        cur_file = open('./results_2.0/' + filename,'r')
        try:
            cur_ids = cur_file.read().split('\n')
            cur_file.close()
            minhash_list.append((minhash_IDs(cur_ids),filename))
        except:
            print(filename)
        cur_file.close()
        # if(idx>300):
        #     break

    # provide a filename, then compare jaccard similarity with all of the files
    #filename = 'P5942_Protected objects Ostbelgien ID.txt'
    #cur_file = open('./results_2.0/' + filename,'r')
    #cur_file = open('/Users/tong/Desktop/missing_results/P846_Global Biodiversity Information Facility ID.txt','r')
    cur_file = open('./test.txt','r')
    
    cur_ids = cur_file.read().split('\n')
    cur_m = minhash_IDs(cur_ids)
    cur_file.close()

    cur_file = open('./test.txt','r')
    cur_id = cur_file.read().split('\n')
    cur_n = minhash_IDs(cur_id)
    print(cur_m.jaccard(cur_n))
    for mHash, name in minhash_list:
        similarity = cur_m.jaccard(mHash)
        rank_list.append((similarity, name))
    rank_list.sort(reverse = True)
    for sim, fname in rank_list[:5]:
        print('similarity : '+str(sim)+' name: '+fname)



''' references

https://ekzhu.github.io/datasketch/minhash.html
https://blog.csdn.net/lsq2902101015/article/details/51305825
https://python3-cookbook.readthedocs.io/zh_CN/latest/c05/p21_serializing_python_objects.html
https://blog.csdn.net/Lycoridiata/article/details/78536768
'''
'''store_minhash(minhash_list,'minhash_list.pkl')
f = open('minhash_list.pkl', 'rb')
minhash_list2 = pickle.loads(f)
print(minhash_list2)
f.close()'''
# store minhash list into pickle
# def store_minhash(data, file_name):
#     f = open(file_name, "wb")
#     pickle.dump(data, f)
