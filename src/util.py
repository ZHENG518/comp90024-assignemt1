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
    for x_cell, x_range in grid['x'].items():
        if x_range[0] < coordinate[0] <= x_range[1]:
            x = x_cell
            break
    for y_cell, y_range in grid['y'].items():
        if y_range[0] < coordinate[1] <= y_range[1]:
            y = y_cell
            break
    return y+x
'''
def load_twitter_data(data_path, size):
    """load data file according to size"""
    assert size in ['tiny', 'small', 'big'], 'size should be in tiny, small or big'
    with open(os.path.join(data_path,f'{size}Twitter.json')) as f:
        data = json.load(f)
    return data
'''
def load_twitter_data(data_path, size):
    """load data file according to size"""
    assert size in ['tiny', 'small', 'big'], 'size should be in tiny, small or big'
    path = Path(data_path)
    file_to_open = path / f'{size}Twitter.json'

    with open(file_to_open,'r',encoding='UTF-8') as f:
        data = json.load(f)
    return data

def preprocess_text(text):
    text_lower = text.lower()
    pattern = re.compile(r"[!?',.…]")
    pattern2 = re.compile(r'["]')
    sub_text = re.sub(pattern,'',text_lower)
    sub_text2 = re.sub(pattern2,'',sub_text)
    return sub_text2

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

def get_score(text,sentiment_scores):
    phrases,words = get_phrases(sentiment_scores)
    score = 0
    #consider phrases first, remove phrases from text once the score has benn added
    for key in phrases:
        while(key in text):
            score += phrases[key]
            text = text.replace(key,'',1)
    #split text and consider words
    text_list = text.split(' ')
    for text in text_list:
        for key in words:
            if(key == text):
                score += words[key]
    return score

def get_coordinate_score_dic(twitter_dic,sentiment_scores):
    tweets = twitter_dic['rows']
    dic = {}
    for tweet in tweets:
        coordinates = tweet['value']['geometry']['coordinates']
        x, y = coordinates
        text = preprocess_text(tweet['doc']['text'])
        #tweets may be posed in the same coordinate
        if((x,y) in dic.keys()):
            dic[(x,y)] += get_score(text,sentiment_scores)
        else:
            dic[(x,y)] = get_score(text,sentiment_scores)
    return dic

def get_cell_score_dic(coordinate_score,grid):
    dic = {}
    for key in coordinate_score:
        cell = get_cell(key,grid)
        score = coordinate_score[key]
        if(cell in dic.keys()):
            dic[cell] += score
        else:
            dic[cell] = score
    return dic

def get_cell_textlist(twitter_dic,grid):
    tweets = twitter_dic['rows']
    dic = {}
    for tweet in tweets:
        coordinates = tweet['value']['geometry']['coordinates']
        x, y = coordinates
        cell = get_cell((x,y),grid)
        text = preprocess_text(tweet['doc']['text'])
        if(cell in dic.keys()):
            dic[cell].append(text)
        else:
            dic[cell] = [text]
    return dic

def get_cell_score(cell_textlist,sentiment_scores):
    dic = cell_textlist
    for key in cell_textlist:
        score = 0
        textlist = cell_textlist[key]
        num = len(textlist)
        for text in textlist:
            score += get_score(text,sentiment_scores)
        dic[key] = (num,score)
    return dic




