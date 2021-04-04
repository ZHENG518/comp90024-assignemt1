import os
import json
import re
from pathlib import Path
import copy

def get_sentiment_socres(data_path):
    """get the corresponding dict between sentiment and score"""
    with open(os.path.join(data_path, 'AFINN.txt')) as f:
        lines = f.readlines()
    sentiment_scores = {}
    for line in lines:
        sentiment, score = line.strip().split('\t')
        sentiment_scores[sentiment] = int(score)
    return sentiment_scores

def get_melb_grid(data_path):
    """get the corresponding dict between cell and location(x, y)"""
    with open(os.path.join(data_path, 'melbGrid.json')) as f:
        data = json.load(f)
    melb_grid = {'x':{}, 'y':{}}
    for line in data['features']:
        cell = line['properties']['id']
        melb_grid['x'][cell[1]] = (line['properties']['xmin'],line['properties']['xmax'])
        melb_grid['y'][cell[0]] = (line['properties']['ymin'],line['properties']['ymax'])
    return melb_grid

def get_cell(coordinate, grid):
    """get the cell name according to the location(x,y)"""
    x,y = None,None
    for x_cell, x_range in grid['x'].items():
        if x_range[0] < coordinate[0] <= x_range[1]:
            x = x_cell
            break
    for y_cell, y_range in grid['y'].items():
        if y_range[0] < coordinate[1] <= y_range[1]:
            y = y_cell
            break
    if x and y:
        return y+x
    else:
        return None

def get_phrases(sentiment_scores):
    phrases = {}
    words = {}
    for key in sentiment_scores:
        temp = key.split(' ')
        if (len(temp) > 1):
            phrases[key] = sentiment_scores[key]
        else:
            words[key] = sentiment_scores[key]
    return (phrases,words) #return a pair of dict, phrases is phrases sentiment score, words is word sentiment score

# def load_twitter_data(data_path, size):
#     """load data file according to size"""
#     assert size in ['tiny', 'small', 'big'], 'size should be in tiny, small or big'
#     with open(os.path.join(data_path,f'{size}Twitter.json')) as f:
#         data = json.load(f)
#     return data

def load_twitter_data(data_path, size):
    """load data file according to size"""
    assert size in ['tiny', 'small', 'big'], 'size should be in tiny, small or big'
    path = Path(data_path)
    file_to_open = path / f'{size}Twitter.json'

    with open(file_to_open,'r',encoding='UTF-8') as f:
        data = json.load(f)
    return data




