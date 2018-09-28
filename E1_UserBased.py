import json
import math
from operator import itemgetter
import DataLoad
import multiprocessing
import time
from prettytable import PrettyTable

Analys_dic = {}
table = PrettyTable(['N#','K#','precision','recall','F1','popularity','coverage'])

def UserSimilarity_IIF(train):
    print 'UserSimilarity_IIF:' +str(time.time())
    startTime = time.time()
    # make item_users inverse table
    item_users = dict()
    for u,items in train.items():
        for i in items:
            if i not in item_users:
                item_users[i] = set()
            item_users[i].add(u)

    C = dict()
    N = dict()

    # compute co-rated items between users
    for i, users in item_users.items():
        for u in users:
            if u not in N:
                N[u] = 0
            N[u] += 1
            for v in users:
                if u == v:
                    continue
                if u not in C:
                    C[u] = dict()
                if v not in C[u]:
                    val = 1 / math.log(1 + len(users))
                    C[u].update({v: val})
                else:
                    val = C[u][v] + 1 / math.log(1 + len(users))
                    C[u].update({v: val})

    # similarity matrix
    W = dict()
    for u, related_users in C.items():
        if u not in W:
            W[u] = dict()
        for v, cuv in related_users.items():
            if v not in W[u]:
                val = cuv / math.sqrt(N[u] * N[v])
                W[u].update({v: val})
    print 'UserSimilarity_IIF :' + str(time.time())
    print 'UserSimilarity_IIF duration:' + str(time.time()-startTime)
    return W

def Recommend(user, train, W, K):
    rank = dict()
    interacted_items = train[user]
    li = W[user].items()
    for v, wuv in sorted(W[user].items(), key=itemgetter(1), reverse=True)[0:K]:
        for i in train[v]:
            if i not in interacted_items:
                if(i in rank):
                    rank[i] += wuv
                else:
                    rank[i] = wuv
    return rank

def Recall(train, test, N, K,W):
    hit = 0
    all = 0
    # W = UserSimilarity_IIF(train)

    for user, items in train.items():
        if user in test:
            tu = test[user]
            rank = Recommend(user, train, W, K)
            rk = sorted(rank.items(), key=itemgetter(1), reverse=True)[0:N]
            for item, pui in rk:
                if item in tu:
                    hit += 1
            all += len(tu)
    return hit / (all * 1.0)

def Precision(train, test, N, K,W):
    hit = 0
    all = 0
    # W = UserSimilarity_IIF(train)

    for user, items in test.items():
        tu = test[user]
        rank = Recommend(user, train, W, K)
        rk = sorted(rank.items(), key=itemgetter(1), reverse=True)[0:N]
        for item, pui in rk:
            if item in tu:
                hit += 1
        all += N
    return hit / (all * 1.0)

def Coverage(train, test, N, K,W):
    recommend_items = set()
    all_items = set()
    # W = UserSimilarity_IIF(train)
    for user, items in train.items():
        for item in items:
            all_items.add(item)
        rank = Recommend(user, train, W, K)
        rk = sorted(rank.items(), key=itemgetter(1), reverse=True)[0:N]
        for item, pui in rk:
            recommend_items.add(item)
    return len(recommend_items) / (len(all_items)*1.0)

def Popularity(train, test, N, K,W):
    item_popularity = dict()
    for user, items in train.items():
        for item in items:
            if item not in item_popularity:
                item_popularity[item] = 0
            item_popularity[item] += 1
    ret = 0
    n = 0
    # W = UserSimilarity_IIF(train)
    for user in train.keys():
        rank = Recommend(user, train, W, K)
        rk = sorted(rank.items(), key=itemgetter(1), reverse=True)[0:N]
        for item, pui in rk:
            ret += math.log(1 + item_popularity[item])
            n += 1
    ret /= n * 1.0
    return ret

def get_Analys(train,test,N,K,similarity_matrix):
    print 'start process with N:' + str(N) + ' and K:' + str(K)
    coverage = Coverage(train, test, N, K,similarity_matrix)
    precision = Precision(train, test, N, K,similarity_matrix)
    recall = Recall(train,test,N,K,similarity_matrix)
    popularity = Popularity(train,test,N,K,similarity_matrix)

    F1 = 2*(precision*recall)/float(precision+recall)
    return json.dumps([N,K,round(precision*100,5),round(recall*100,5),F1,round(coverage*100,5),round(popularity,5)])

if __name__ == '__main__':

    (users, ratings, movies) = DataLoad.LoadDataML_1M()
    print ratings.head(5)
    (train, test) = DataLoad.getUserItemTrainList(ratings,100,10)
    print('**********Train Data**********')
    print len(train)
    print('**********Test Data**********')
    print len(test)

    similarity_matrix = UserSimilarity_IIF(train)
    result = []

    pool = multiprocessing.Pool(processes=5)
    result.append(pool.apply_async(get_Analys, (train,test,5,10, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train,test,5,30, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train,test,5,50, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train,test,5,80, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train,test,5,100, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train,test,5,120, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train,test,5,150, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 10, 10, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 10, 20, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 10, 30, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 10, 50, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 10, 70, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 10, 100, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 10, 120, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 10, 150, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 10, 3, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 15, 20, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 15, 30, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 15, 50, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 15, 80, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 15, 100, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 15, 120, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 15, 150, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 20, 10, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 20, 30, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 20, 50, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 20, 80, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 20, 100, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 20, 150, similarity_matrix,)))
    pool.close()
    pool.join()
    for items in result:
        print items.get()
        table.add_row(json.loads(items.get()))
    print "Sub-process(es) done."
    print table
    # # print(Recommend(1, train, similarity_matrix, 3))
    #
