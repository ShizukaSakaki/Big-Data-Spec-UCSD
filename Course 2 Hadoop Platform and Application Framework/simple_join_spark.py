PYSPARK_DRIVER_PYTHON=ipython pyspark

fileA = sc.textFile("input/join1_FileA.txt")
fileA.collect()

fileB = sc.textFile("input/join1_FileB.txt")
fileB.collect()

def split_fileA(line):
    # split the input line in word and count on the comma
    word, count=line.split(",")
    # turn the count to an integer  
    count=int(count)
    return (word, count)

test_line = "able,991"
split_fileA(test_line) 


#map transformation to the fileA RDD
fileA_data = fileA.map(split_fileA)
fileA_data.collect()

def split_fileB(line):
    # split the input line into word, date and count_string
    word_date, count_string = line.split(",")
    date, word = word_date.split(" ")
    return (word, date + " " + count_string) 

fileB_data = fileB.map(split_fileB)
#gathering the output back to the pyspark Driver console
fileB_data.collect()

#run join
fileB_joined_fileA = fileB_data.join(fileA_data)
#Verify the result
fileB_joined_fileA.collect()