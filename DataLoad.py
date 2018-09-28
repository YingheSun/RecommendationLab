import random
import pandas as pd
import numpy as np

Path = "/data/hadoop_data/hdfs/"

def LoadDataML_1M():
    users_Name=['user_id','gender','age','work','zip']
    ratings_Name=['user_id','movies_id','ratings','timeStamp']
    movie_Name=['movie_id','title','calss']
    print('Loading............ ml-1m/users.dat')
    users = pd.read_table(Path + 'ml-1m/users.dat', sep='::', header=None, names=users_Name,engine='python')
    print('**********User Record**********')
    print(users.head(5))
    print('Loading............ ml-1m/ratings.dat')
    ratings = pd.read_table(Path + 'ml-1m/ratings.dat', sep='::', header=None, names=ratings_Name,engine='python')
    print('**********Rating Record**********')
    print(ratings.head(5))
    print('Loading............ ml-1m/movies.dat')
    movies = pd.read_table(Path + 'ml-1m/movies.dat', sep='::', header=None, names=movie_Name,engine='python')
    print('**********Movie Record**********')
    print(movies.head(5))
    print('User Record:',len(users),'Rating Record:',len(ratings),'Movie Record:',len(movies))
    print('All Loaded Complete')
    return (users,ratings,movies)

def DataSplit(data, M, k, seed = 1526):
    test = dict()
    train = dict()
    random.seed(seed)
    for user, item in data:
        rdm = random.randint(0, M)
        if rdm == k:
            if user not in test:
                test[user] = set()
            test[user].add(item)
            # test.append([user, item])
        else:
            if user not in train:
                train[user] = set()
            train[user].add(item)
            # train.append([user, item])
    return train, test

def getUserItemTrainList(rating, M=10, k=1):
    col = ['user_id', 'movies_id']
    use_col_list = pd.DataFrame(rating, columns = col)
    print(use_col_list.head(5))
    arr_data = np.array(use_col_list)
    list_data = arr_data.tolist()

    return DataSplit(list_data[:10000], M, k)

if __name__ == '__main__':
    (users, ratings, movies) = LoadDataML_1M()
    print(getUserItemTrainList(ratings))