from pyspark import SparkConf, SparkContext
import random

####################
# Input Parameters #
####################
sentenceLength = 20
windowLength = 2
wordsSelectNum = 5
source = "textLibrary/*.txt"
filterWords = ['whichWords', 'toFilter']

def prepareText(text):
    preText = text.replace(',','').replace('?','.').replace('!','.').replace('"','.')#.split('.')
    return preText

def parseText(text):
    postText = text.split('.')
    return postText

def createPairs(line):
    results = []
    input = line.split(' ')
    input = [item.lower() for item in input]
    for word in input:
        if filterWords.count(word) != 0:
            input.remove(word)
    #print(input)
    #running these three next rows to include last pair in the sentence
    for i in range(windowLength):
        input.append(())
    for i in range(len(input)-windowLength-1):
        for ii in range(windowLength):
            if input[i] != input[i+ii] and input[i]  != '' and input[i+ii]  != '' and input[i+ii]  != ():# and input[i] != 'a' and input[i] != 'an' and input[i] != 'and' and input[i] != 'the' and input[i+ii] != 'and' and input[i+ii] != 'the' and input[i+ii] != 'a' and input[i+ii] != 'an':
                results.append(((input[i],input[i+ii]),1))
    return(results)

conf = SparkConf().setMaster("local").setAppName("Little Red Riding Hood")
sc = SparkContext(conf = conf)

results = sc.textFile(source).map(prepareText).flatMap(parseText).filter(lambda x: len(x) >= windowLength).flatMap(createPairs)
results = results.reduceByKey(lambda x, y: x + y).map(lambda x : (x[1], x[0])).sortByKey().collect()#.filter(lambda x: x[0]<15).collect()

for result in results:
    print(result)

print('Welcome to LRRH and other fairy tales code!')
inputWord = ''
while(inputWord != 'exit()'):
    inputWord = input('Write word to predict: ').lower()
    sentence = []
    maxWord = 'is not in the text :('
    num = 0
    kill = False
    sentence.append(inputWord)

    while(num < sentenceLength):
        inputMatch = []
        for result in results:
            if inputWord == result[1][0]:
                inputMatch.append((result[1][1],result[0]))
        #print(inputMatch)
        maxCount = 0
        for i in range(len(inputMatch)):
            if int(inputMatch[i][1]) >= maxCount and sentence.count(inputMatch[i][0]) == 0:
                maxCount = inputMatch[i][1]
                maxWord = inputMatch[i][0]
                maxI = i
        if len(inputMatch)<wordsSelectNum:
            wordRange = len(inputMatch)
        else:
            wordRange = wordsSelectNum
        wordOptions = []
        for i in range(wordRange):
            print(inputMatch[maxI-i][0])
            wordOptions.append((inputMatch[maxI-i][0]))
        #inputWord = input('Select new input: ')
        inputWord = random.choice(wordOptions)
        print('Word selected: ', inputWord)
        if inputWord != 'exit()': # and maxWord != inputWord
            #inputWord = maxWord
            sentence.append(inputWord)
        else:
            break
        num=num+1
    print('Whole sentence: ', str(sentence))
print("End of code")
