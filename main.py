from src import util

data_path = './data'
sentiment_scores = util.get_sentiment_socres(data_path)
melb_grid = util.get_melb_grid(data_path)
cell_name = util.get_cell([144.97505587, -37.87538373], melb_grid)  # C2
tiny_twwiter = util.load_twitter_data(data_path, 'tiny')