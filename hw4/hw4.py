import numpy as np
import pandas as pd

users_df = pd.read_csv('../hw4/ml-1m/users.dat', sep='::', header=None)
# print(users_df)
users_df.columns = ['UserID', 'Gender', 'Age', 'Occupation', 'Zip-code']

movies_df = pd.read_csv('../hw4/ml-1m/movies.dat', sep='::', header=None)
movies_df.columns = ['MovieID', 'Title', 'Genres']
# print(movies_df)

ratings_df = pd.read_csv('../hw4/ml-1m/ratings.dat', sep='::', header=None)
# print(ratings_df)
ratings_df.columns = ['UserID', 'MovieID', 'Rating', 'Timestamp']


# 1
sorted_all_movie_ratings_df = ratings_df.groupby('MovieID').mean().sort_values('Rating', ascending=False)['Rating']
print(sorted_all_movie_ratings_df)
sorted_all_movie_ratings_df.to_csv('output/top-rated_movies_by_all_users.csv')
##########################################################################
# 2
def get_column_unique_value_list(df, column):
    return list(set(df[column].to_numpy()))


def get_result_sorted_all_group_ratings_df(users_df, ratings_df, by_column):
    column_unique_value_list = get_column_unique_value_list(users_df, by_column)
    # print(column_unique_value_list)

    result_sorted_all_group_ratings_df = pd.DataFrame()
    for i in range(len(column_unique_value_list)):
        print(column_unique_value_list[i])

        # print(users_df[users_df[by_column] == column_unique_value_list[0]])
        one_group_users_id_np = users_df[users_df[by_column] == column_unique_value_list[i]]['UserID'].to_numpy()
        # print(one_group_users_id_np)

        one_group_ratings_df = pd.DataFrame()
        for k in range(len(one_group_users_id_np)):
            one_group_ratings_df = one_group_ratings_df.append(
                ratings_df[ratings_df['UserID'] == one_group_users_id_np[k]])

        # print(one_group_ratings_df)

        sorted_one_group_ratings_df = one_group_ratings_df.groupby('MovieID').mean()
        sorted_one_group_ratings_df[by_column] = [column_unique_value_list[i] for _ in
                                                  range(len(sorted_one_group_ratings_df['Rating']))]
        sorted_one_group_ratings_df = sorted_one_group_ratings_df.sort_values('Rating', ascending=False)
        result_sorted_all_group_ratings_df = result_sorted_all_group_ratings_df.append(sorted_one_group_ratings_df)

    result_sorted_all_group_ratings_df[[by_column, 'Rating']].to_csv('output/top-rated_movies_by_' + by_column + '.csv')


get_result_sorted_all_group_ratings_df(users_df, ratings_df, 'Gender')
get_result_sorted_all_group_ratings_df(users_df, ratings_df, 'Age')
get_result_sorted_all_group_ratings_df(users_df, ratings_df, 'Occupation')
##########################################################################
# 3
rating_score_of_each_user_df = ratings_df.groupby('UserID').mean().sort_values('Rating', ascending=False)['Rating']
rating_score_of_each_user_df.to_csv('output/rating_score_of_each_user.csv')
######################################################
# add_movie_genre_to_rating_df
movie_id_np = ratings_df['MovieID'].to_numpy()
ratings_df['Genres'] = [movies_df[movies_df['MovieID'] == movie_id]['Genres'].to_numpy()[0] for movie_id in movie_id_np]

########################remove################
# ratings_df.to_csv('ratings.csv', index=False)
# ratings_df = pd.read_csv('ratings.csv')
########################remove################

rating_score_of_each_user_by_genre_df = pd.DataFrame()
all_user_np = users_df['UserID'].to_numpy()
# print(len(all_user_np))
for user_id in all_user_np:
    genres_np = ratings_df[ratings_df['UserID'] == user_id]['Genres'].to_numpy()
    all_genres_list = list()
    for genres in genres_np:
        all_genres_list += genres.split('|')
    all_genres_list = list(set(all_genres_list))
    # print(all_genres_list)

    one_user_rating_df = ratings_df[ratings_df['UserID'] == user_id]
    one_user_rating_df_Genres_np = one_user_rating_df['Genres'].to_numpy()
    one_user_rating_df_Rating_np = one_user_rating_df['Rating'].to_numpy()

    average_rating_list = list()
    for genre in all_genres_list:
        all_rating_of_one_Genre = list()
        for i in range(len(one_user_rating_df_Genres_np)):
            if genre in one_user_rating_df_Genres_np[i]:
                all_rating_of_one_Genre.append(one_user_rating_df_Rating_np[i])
        average_rating_list.append(np.mean(all_rating_of_one_Genre))

    one_user_df = pd.DataFrame({
        'UserID': [user_id for i in range(len(all_genres_list))],
        'Genres': all_genres_list,
        'Rating': average_rating_list,
    })
    rating_score_of_each_user_by_genre_df = rating_score_of_each_user_by_genre_df.append(
        one_user_df.sort_values('Rating', ascending=False))

rating_score_of_each_user_by_genre_df.to_csv('output/rating_score_of_each_user_by_genre.csv', index=False)


##########################################################################
# 4
def get_cosine_similarity(a_np, b_np):
    return np.sum(a_np * b_np) / (pow(np.sum(np.square(a_np)), 1 / 2) * pow(np.sum(np.square(b_np)), 1 / 2))


# user_id = int(input('Please input UserID: '))
user_id = 50

user_cosine_similarity_list = list()
all_user_np = users_df['UserID'].to_numpy()
print(all_user_np)

for user_id2 in all_user_np:
    two_user_df = pd.DataFrame()
    two_user_df['MovieID'] = movies_df['MovieID']
    two_user_df = two_user_df.set_index('MovieID')

    user1_df = ratings_df[ratings_df['UserID'] == user_id].set_index('MovieID')
    user2_df = ratings_df[ratings_df['UserID'] == user_id2].set_index('MovieID')

    two_user_df = two_user_df.merge(user1_df, how='outer', left_index=True, right_index=True)
    two_user_df = two_user_df.merge(user2_df, how='outer', left_index=True, right_index=True)
    two_user_df = two_user_df.fillna(0)
    # print(two_user_df)

    a_np = two_user_df['Rating_x'].to_numpy()
    b_np = two_user_df['Rating_y'].to_numpy()

    user_cosine_similarity_list.append(get_cosine_similarity(a_np, b_np))

# print(cosine_similarity_list)

user_cosine_similarity_df = pd.DataFrame({
    'UserID': all_user_np,
    'score': user_cosine_similarity_list,
}).sort_values('score', ascending=False)

user_cosine_similarity_df.to_csv('output/user_cosine_similarity.csv', index=False)
##########################################################################
# 5
# movie_id = int(input('Please input MovieID: '))
movie_id = 50
all_movie_np = movies_df['MovieID'].to_numpy()
movie_cosine_similarity_list = list()
print(all_movie_np)
for movie_id2 in all_movie_np:
    two_movie_df = pd.DataFrame()
    two_movie_df['UserID'] = users_df['UserID']
    two_movie_df = two_movie_df.set_index('UserID')

    movie1_df = ratings_df[ratings_df['MovieID'] == movie_id].set_index('UserID')
    movie2_df = ratings_df[ratings_df['MovieID'] == movie_id2].set_index('UserID')

    two_movie_df = two_movie_df.merge(movie1_df, how='outer', left_index=True, right_index=True)
    two_movie_df = two_movie_df.merge(movie2_df, how='outer', left_index=True, right_index=True)
    two_movie_df = two_movie_df.fillna(0)

    a_np = two_movie_df['Rating_x'].to_numpy()
    b_np = two_movie_df['Rating_y'].to_numpy()

    movie_cosine_similarity_list.append(get_cosine_similarity(a_np, b_np))

movie_cosine_similarity_df = pd.DataFrame({
    'UserID': all_movie_np,
    'score': movie_cosine_similarity_list,
}).sort_values('score', ascending=False)

movie_cosine_similarity_df.to_csv('output/movie_cosine_similarity.csv', index=False)
