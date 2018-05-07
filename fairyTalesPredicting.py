from pyspark import SparkConf, SparkContext
import random

####################
# Input Parameters #
####################
maxSentenceLength = 30
windowLength = 2
wordsSelectNum = 20
autoPick = True
source = "textLibrary/*.txt"
#source = "textLibrary/grimm.txt"
filterWords = ['whichWords', 'toFilter']
commonWords = ['a' ,'an', 'the' , 'and', 'as', 'but', 'no' , 'not', 'if', 'she', 'he', 'i', 'his' , 'my', 'we' , 'they', 'who']
commonWordsMaybeEnd = ['to', 'of', 'it', 'so', 'with', 'at']
prepositions = []

def prepareText(text):
    preText = text.replace(',','.').replace('?','.').replace('!','.').replace('"','.').replace(';','.').replace(':','.').replace('(','.').replace(')','.').replace('-','')#.split('.')
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
    #put frequent words together
    for i in reversed(range(len(input)-1)):
        #delete if last words of sentence
        if commonWords.count(input[len(input)-1]) == 1 or commonWordsMaybeEnd.count(input[len(input)-1]) == 1:
            input.pop(len(input)-1)
            i = i-1
        if commonWords.count(input[i]) == 1 or commonWordsMaybeEnd.count(input[i]) == 1:
            input[i] = input[i] + ' ' + input[i+1]
            input.pop(i+1)
    #print(input)
    #running these three next rows to include last pair in the sentence
    for i in range(windowLength):
        input.append(())
    for i in range(len(input)-windowLength-1):
        for ii in range(windowLength):
            if input[i] != input[i+ii] and input[i]  != '' and input[i+ii]  != '' and input[i+ii]  != ():
                results.append(((input[i],input[i+ii]),1))
    return(results)

conf = SparkConf().setMaster("local").setAppName("Little Red Riding Hood")
sc = SparkContext(conf = conf)

results = sc.textFile(source).map(prepareText).flatMap(parseText).filter(lambda x: len(x) >= windowLength).flatMap(createPairs)#.collect()#
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

    while(num < maxSentenceLength):
        inputMatch = []
        #select all possible pairs with input word
        for result in results:
            if inputWord == result[1][0]:
                inputMatch.append((result[1][1],result[0]))
        maxCount = 0
        maxI = 0
        for i in range(len(inputMatch)):
            if int(inputMatch[i][1]) >= maxCount and sentence.count(inputMatch[i][0]) == 0:
                maxCount = inputMatch[i][1]
                maxWord = inputMatch[i][0]
                maxI = i
        #select only top wordsSelectNum or at least what is available
        if len(inputMatch)<wordsSelectNum:
            wordRange = len(inputMatch)
        else:
            wordRange = wordsSelectNum
        wordOptions = []
        wordOptionsCount = []
        #create pairs to select from
        for i in range(wordRange):
            #print(inputMatch[maxI-i][0], ' ', str(inputMatch[maxI-i][1]))
            wordOptions.append((inputMatch[maxI-i][0]))
            wordOptionsCount.append(inputMatch[maxI-i][1])
        if len(wordOptions) > 0:
            #if there are better performing pairs, select those
            if max(wordOptionsCount) != 1:
                for i in reversed(range(len(wordOptions))):
                    if wordOptionsCount[i] == 1:
                        wordOptionsCount.pop(i)
                        wordOptions.pop(i)
            #print possible next words out
            for i in range(len(wordOptions)):
                print(wordOptions[i], ' ', wordOptionsCount[i])
            if autoPick:
                inputWord = random.choice(wordOptions)
            else:
                inputWord = input('Select new input: ')
            print('Word selected: ', inputWord)
            if inputWord != 'exit()': # and maxWord != inputWord
                sentence.append(inputWord)
            else:
                break
            num=num+1
        else:
            print('No matches for last word to select')
            break
    print('Whole sentence: ', ' '.join(sentence))
print("End of code")
