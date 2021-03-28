from src import util
import re

def get_sentiment_pattern(sentiment_scores):
    phrases,words = util.get_phrases(sentiment_scores)
    phrases = list(phrases.keys())
    words = list(words.keys())
    sentiment_pattern = '|'.join(phrases+words)
    sentiment_pattern = r'(?<=\s)('+sentiment_pattern+ r')(?=\s)'
    return sentiment_pattern

def get_score(text, sentiment_pattern, sentiment_scores):
    keywords = re.findall(sentiment_pattern, ' '+text+' ')
    score = sum([sentiment_scores[keyword] for keyword in keywords])
    return score

def preprocess_text(text):
    text_lower = text.lower()
    pattern = re.compile(r"[!?',.…]")
    pattern2 = re.compile(r'["]')
    sub_text = re.sub(pattern,'',text_lower)
    sub_text2 = re.sub(pattern2,'',sub_text)
    return sub_text2

def get_coordinate_score_dic(twitter_dic,sentiment_scores):
    sentiment_pattern = get_sentiment_pattern(sentiment_scores)
    tweets = twitter_dic['rows']
    dic = {}
    for tweet in tweets:
        coordinates = tweet['value']['geometry']['coordinates']
        x, y = coordinates
        text = preprocess_text(tweet['doc']['text'])
        #tweets may be posed in the same coordinate
        dic[(x,y)] = dic.get((x,y), 0) + get_score(text, sentiment_pattern, sentiment_scores)
    return dic

def get_cell_score_dic(coordinate_score,grid):
    dic = {}
    for key, score in coordinate_score.items():
        cell = util.get_cell(key,grid)
        dic[cell] = dic.get(cell, 0) + score
    return dic

def get_cell_textlist(twitter_dic,grid):
    tweets = twitter_dic['rows']
    dic = {}
    for tweet in tweets:
        coordinates = tweet['value']['geometry']['coordinates']
        x, y = coordinates
        cell = util.get_cell((x,y),grid)
        text = preprocess_text(tweet['doc']['text'])
        if(cell in dic.keys()):
            dic[cell].append(text)
        else:
            dic[cell] = [text]

    return dic

def get_cell_score(cell_textlist,sentiment_scores):
    sentiment_pattern = get_sentiment_pattern(sentiment_scores)
    dic = cell_textlist
    for key, textlist in cell_textlist.items():
        score = 0
        num = len(textlist)
        for text in textlist:
            score += get_score(text, sentiment_pattern, sentiment_scores)
        dic[key] = (num,score)
    return dic

if __name__ == '__main__':
    # data_path = './data'
    data_path = '.\data'
    sentiment_scores = util.get_sentiment_socres(data_path)
    melb_grid = util.get_melb_grid(data_path)
    # cell_name = util.get_cell([144.97505587, -37.87538373], melb_grid)  # C2
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

    temp = get_cell_textlist(small_twitter,melb_grid)
    result = get_cell_score(temp,sentiment_scores)
    print(result)

    temp2 = get_coordinate_score_dic(small_twitter,sentiment_scores)
    result2 = get_cell_score_dic(temp2,melb_grid)
    print(result2)



