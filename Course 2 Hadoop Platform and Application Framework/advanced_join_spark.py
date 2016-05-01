PYSPARK_DRIVER_PYTHON=ipython pyspark

show_views_file = sc.textFile("input/join2_gennum?.txt")

# first 2 elements of the dataset
show_views_file.take(2)

#splits and parses each line of the dataset
def split_show_views(line):
    show, views = line.split(",")
    views=int(views)
    return (show, views)

#transform the input RDD
show_views = show_views_file.map(split_show_views)

#view the result
show_views.collect()

#Read channel files
show_channel_file = sc.textFile("input/join2_genchan?.txt")

# first 2 elements of the dataset
show_channel_file.take(2)

# parse each line of the dataset
def split_show_channel(line):
    show, channel=line.split(",")
    return (show, channel)

#parse the channel files
show_channel = show_channel_file.map(split_show_channel)  

#view the result
show_channel.collect()

#Join the 2 datasets
#order does not matter as long as consistent
joined_dataset = show_channel.join(show_views)

#view the result
joined_dataset.take(2)

#create an RDD with the channel as key and all the viewer counts
def extract_channel_views(show_views_channel): 
    channel, views = show_views_channel[1]
    return (channel, views)

#create an RDD of channel and views
channel_views = joined_dataset.map(extract_channel_views)

#view the result
channel_views.take(2)

#sum all of the viewers for each channel
def sum_channel_viewers(a,b):
    return a + b

channel_views.reduceByKey(sum_channel_viewers).collect()
