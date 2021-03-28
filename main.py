from src import util

data_path = './data'
#data_path = '.\data'
sentiment_scores = util.get_sentiment_socres(data_path)
melb_grid = util.get_melb_grid(data_path)
cell_name = util.get_cell([144.97505587, -37.87538373], melb_grid)  # C2
tiny_twitter = util.load_twitter_data(data_path, 'tiny')
small_twitter = util.load_twitter_data(data_path, 'small')

#temp = tiny_twitter['rows'][2]
#print(temp.keys())
#print(temp['value']['geometry']['coordinates'])
#print(temp['doc'].keys())
#print(temp['doc']['coordinates'])
#print(temp['doc']['geo'])
#print(temp['doc']['text'])
#print(temp['value']['properties']['text'])

temp = util.get_cell_textlist(small_twitter,melb_grid)
result = util.get_cell_score(temp,sentiment_scores)
print(result)


temp2 = util.get_coordinate_score_dic(small_twitter,sentiment_scores)
result2 = util.get_cell_score_dic(temp2,melb_grid)
print(result2)



