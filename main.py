from src import util
import re
import time
from mpi4py import MPI
import queue

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

'''def get_coordinate_score_dic(twitter_dic,sentiment_scores):
    sentiment_pattern = get_sentiment_pattern(sentiment_scores)
    tweets = twitter_dic['rows']
    dic = {}
    for tweet in tweets:
        coordinates = tweet['value']['geometry']['coordinates']
        x, y = coordinates
        text = preprocess_text(tweet['doc']['text'])
        #tweets may be posed in the same coordinate
        dic[(x,y)] = dic.get((x,y), 0) + get_score(text, sentiment_pattern, sentiment_scores)
    return dic'''

'''def get_cell_score_dic(coordinate_score,grid):
    dic = {}
    for key, score in coordinate_score.items():
        cell = util.get_cell(key,grid)
        dic[cell] = dic.get(cell, 0) + score
    return dic'''

'''def get_cell_textlist(twitter_dic,grid):
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

    return dic'''

'''def get_cell_score(cell_textlist,sentiment_scores):
    sentiment_pattern = get_sentiment_pattern(sentiment_scores)
    dic = cell_textlist
    for key, textlist in cell_textlist.items():
        score = 0
        num = len(textlist)
        for text in textlist:
            score += get_score(text, sentiment_pattern, sentiment_scores)
        dic[key] = (num,score)
    return dic'''

'''def get_cell_text(twitter_dic,grid):
    tweets = twitter_dic['rows']
    dic = {}
    for tweet in tweets:
        coordinates = tweet['value']['geometry']['coordinates']
        x, y = coordinates
        cell = util.get_cell((x,y),grid)
        text = tweet['doc']['text']
        if(cell in dic.keys()):
            dic[cell].append(text)
        else:
            dic[cell] = [text]
    return dic'''

# given a text and its coordinates, return the score and cell
def get_cell_score2(coordinates,text,sentiment_scores,grid):
    sentiment_pattern = get_sentiment_pattern(sentiment_scores)
    score = get_score(text,sentiment_pattern,sentiment_scores)
    x,y=coordinates
    cell = util.get_cell((x,y),grid)
    return(cell,score)

###
### MPI functions
### master node to allocate jobs and integrate results from slave nodes
### slave nodes to preprocess text, compute score, get cell and count tweets
### grid and sentiment score is loaded in each slave nodes due to limitation of sended package size
###

def master(data_path,twitter_size):
    status = MPI.Status()

    small_twitter = util.load_twitter_data(data_path, twitter_size)
    tweets = small_twitter['rows']

    i = 1+size # slave node index starts from 1
    result = {}

    for tweet in tweets:# send slave nodes one tweet info once a time
        coordinates = tweet['value']['geometry']['coordinates']
        text = tweet['doc']['text']
        job = (coordinates,text)
        if(i%size==0):
            i+=1 #avoid sending job to master node,
                # slave node 1 can have more jobs because of this sentence
                # all jobs expected to send to master node is sended to slave node 1
            send = comm.send(job, dest=i%size, tag=1)
        else:
            send = comm.send(job, dest=i%size, tag=1)
            i+=1

    #collect message from slave nodes
    i=1
    for i in range(1,size):
        send = comm.send(None,dest=i,tag = 0)#send finish tag to all nodes
        rec = comm.recv(source=i, tag=MPI.ANY_TAG, status=status)#get result from slave node
        #add results from salve nodes
        for key in rec:#rec = {'cell',[score,number of tweets],'cell',[score,number of tweets]...}
            if(key in result.keys()):
                result[key][0] += rec[key][0]
                result[key][1] += rec[key][1]
            else:
                result[key] = rec[key]

    return result

def slave(data_path):
    result_dic = {}#key is cell, value is list[score,total_tweets]
    sentiment_scores = util.get_sentiment_socres(data_path)
    melb_grid = util.get_melb_grid(data_path)

    status = MPI.Status()
    while True:#receive message from master until receive a message with tag 0, which means stop working
        rec = comm.recv(source = 0, tag = MPI.ANY_TAG,status = status)
        if(status.Get_tag()==0): break #stop if stop instruction sended from master node
        (x,y),text = rec
        cell,score = get_cell_score2((x,y),preprocess_text(text),sentiment_scores,melb_grid)
        if (cell in result_dic.keys()):
            result_dic[cell][0] += score
            result_dic[cell][1] += 1
        else:#initialize if key,value pair doesn't exist
            result_dic[cell] = [score,1]
    #print(result_dic)

    #return result to master node
    #result_dic = {'cell',[score,number of tweets],'cell',[score,number of tweets]...}
    send = comm.send(result_dic,dest = 0,tag=1)

if __name__ == '__main__':
    start = time.time()
    data_path = './data'
    #data_path = '.\data'
    #sentiment_scores = util.get_sentiment_socres(data_path)
    #melb_grid = util.get_melb_grid(data_path)
    # cell_name = util.get_cell([144.97505587, -37.87538373], melb_grid)  # C2
    #tiny_twitter = util.load_twitter_data(data_path, 'tiny')
    #small_twitter = util.load_twitter_data(data_path, 'small')

    #temp = get_cell_textlist(small_twitter,melb_grid)
    #result = get_cell_score(temp,sentiment_scores)
    #print('?????????',result)

    #temp2 = get_coordinate_score_dic(small_twitter,sentiment_scores)
    #result2 = get_cell_score_dic(temp2,melb_grid)
    #print('!>!>!>',result2)


    #end = time.time()
    #print('run time: ', end-start)



    ###
    ###MPI
    ###
    # run by command below in terminal
    # mpiexec -n 8 python main.py

    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    result = {}
    if rank == 0:
        result = master(data_path,'small')
    else:
        slave(data_path)

    end = time.time()
    print(result)
    print('node %r runtime' % rank,end-start)



