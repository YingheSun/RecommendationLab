import json
import math
from operator import itemgetter
import DataLoad
import multiprocessing
import time
from prettytable import PrettyTable

Analys_dic = {}
table = PrettyTable(['N#','K#','precision','recall','F1','popularity','coverage'])

def ItemSimilarity(train):
    C = dict()
    N = dict()
    for u, items in train.items():
        for i in items:
            if i not in N:
                N[i] = 0
            N[i] += 1
            for j in items:
                if i == j:
                    continue
                if i not in C:
                    C[i] = dict()
                if j not in C[i]:
                    val = 1 / math.log(1 + len(items)*1.0)
                    C[i].update({j: val})
                else:
                    val = C[i][j] + 1 / math.log(1 + len(items)*1.0)
                    C[i].update({j: val})
    W = dict()
    for i, related_items in C.items():
        for j, cij in related_items.items():
            if i not in W:
                W[i] = dict()

            val = cij / math.sqrt(N[i] * N[j])
            W[i].update({j: val})

    return W

def Recommend(train, user_id, W, K):
    rank = dict()
    ru = train[user_id]
    for i in ru:
        for j, wj in sorted(W[i].items(), key=itemgetter(1), reverse=True)[0:K]:
            if j in ru:
                continue
            if j not in rank:
                rank[j] = wj
            else:
                rank[j] +=  wj
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

    similarity_matrix = ItemSimilarity(train)
    result = []

    pool = multiprocessing.Pool(processes=5)
    result.append(pool.apply_async(get_Analys, (train, test, 5, 10, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 5, 30, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 5, 50, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 5, 80, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 5, 100, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 5, 120, similarity_matrix,)))
    result.append(pool.apply_async(get_Analys, (train, test, 5, 150, similarity_matrix,)))
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
        print items
        # table.add_row(json.loads(items.get()))
    print "Sub-process(es) done."
    print table