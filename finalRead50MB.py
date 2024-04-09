import ijson
import multiprocessing
import os
file_name = 'twitter-50mb.json'


def process_partition(partition):
    MMDDTweets = {}
    hourTweets = {}
    MMDDSenTweets = {}
    hourSenTweets = {}

    for item in partition:
        sentimentData = item["data"].get("sentiment")
        if isinstance(sentimentData, dict):
            sentiment = sentimentData.get("score", None)
        else:
            sentiment = sentimentData
        created_at = item["data"].get("created_at")

        # Get only the Month-Day with the corresponding sentiment
        monthDay = (created_at.split("T")[0].split("-")[1], created_at.split("T")[0].split("-")[2])
        # Make a key for the dictionary to remember how many tweets were tweeted in given day of a given month
        MMDDKey = ''.join(monthDay)

        # Get only the hour since we're only concerned with the hour. Both dateData and timeData are the same size so
        # the hours should line up with the day. We get Month-Day-Hour
        hour = (created_at.split("T")[0].split("-")[1], created_at.split("T")[0].split("-")[2],
                created_at.split("T")[1].split(".")[0].split(":")[0])
        hourKey = ''.join(hour)

        # Increment number of tweets in that given month-day combination
        MMDDTweets[MMDDKey] = MMDDTweets.get(MMDDKey, 0) + 1
        # Sum up sentiment for a given MMDD Combination
        MMDDSenTweets[MMDDKey] = MMDDSenTweets.get(MMDDKey, 0) + (sentiment if sentiment is not None else 0)

        # Increment number of tweets in that given month-day-hour combination
        hourTweets[hourKey] = hourTweets.get(hourKey, 0) + 1
        # Sum up sentiment for a given MMDDHH combination
        hourSenTweets[hourKey] = hourSenTweets.get(hourKey, 0) + (sentiment if sentiment is not None else 0)

    return MMDDTweets, hourTweets, MMDDSenTweets, hourSenTweets


def merge_results(results2):
    merged_MMDDTweets = {}
    merged_hourTweets = {}
    merged_MMDDSenTweets = {}
    merged_hourSenTweets = {}

    for MMDDTweets, hourTweets, MMDDSenTweets, hourSenTweets in results2:
        merge_dictionaries(merged_MMDDTweets, MMDDTweets)
        merge_dictionaries(merged_hourTweets, hourTweets)
        merge_dictionaries(merged_MMDDSenTweets, MMDDSenTweets)
        merge_dictionaries(merged_hourSenTweets, hourSenTweets)

    return merged_MMDDTweets, merged_hourTweets, merged_MMDDSenTweets, merged_hourSenTweets


def merge_dictionaries(toMergeWith, mergeItem):
    for key, value in mergeItem.items():
        toMergeWith[key] = toMergeWith.get(key, 0) + value


processes = multiprocessing.cpu_count()
file_size = os.path.getsize(file_name)
chunk_size = file_size // processes

pool = multiprocessing.Pool(processes)

with open(file_name, "r", encoding="utf-8") as file:
    chunks = [list(chunk) for chunk in ijson.items(file, "rows.item.doc", multiple_values=True)]

results = pool.map(process_partition, chunks)
merged_results = merge_results(results)

pool.close()
pool.join()

print("Most active Month-Day Tweets:", merged_results[0])
print("Most active Hourly Tweets:", merged_results[1])
print("Most happy Month-Day Sentiment Tweets:", merged_results[2])
print("Most happy Hourly Sentiment Tweets:", merged_results[3])