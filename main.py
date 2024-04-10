from mpi4py import MPI
import sys
import os
import json

comm = MPI.COMM_WORLD

def main():
 
    start = MPI.Wtime()
    f = open(sys.argv[1], "r", encoding="utf8")
    chunk_size = os.path.getsize(sys.argv[1]) // comm.size
    
    # Setup partitions, ensuring they start at beginning of new record
    f.seek(chunk_size*comm.rank)
    next_record(f)

    # Setup large dictionaries
    if comm.rank == 0:

        # Keep track of how many tweets are made on a given month-day
        LargeMMDDTweets = {}

        # Keep track of total sentiment of tweets of a given day in a given month-day
        LargeMMDDSenTweets = {}

        # Keep track of how many tweets are made on a given month-day-hour
        LargehourTweets = {}

        # Keep track of total sentiment of tweets of a given hour of a given day in a given month-day-hour
        LargehourSenTweets = {}

    result = process(f, chunk_size)
    results = comm.gather(result, root=0)
    
    # Combine results into larger dictionaries
    if comm.rank == 0:
        LargeMMDDTweets = combine_dicts([LargeMMDDTweets] + [i[0] for i in results])
        LargeMMDDSenTweets = combine_dicts([LargeMMDDSenTweets] + [i[1] for i in results])
        LargehourTweets = combine_dicts([LargehourTweets] + [i[2] for i in results])
        LargehourSenTweets = combine_dicts([LargehourSenTweets] + [i[3] for i in results])

    # Find and print results
    if comm.rank == 0:
        mostTweetsInDay = max(LargeMMDDTweets, key=LargeMMDDTweets.get)
        happiestDay = max(LargeMMDDSenTweets, key=LargeMMDDSenTweets.get)
        mostTweetsInHour = max(LargehourTweets, key=LargehourTweets.get)
        happiestHour = max(LargehourSenTweets, key=LargehourSenTweets.get)
        print("Month-Day with the most tweets: ", mostTweetsInDay, "with a total of: ", LargeMMDDTweets[mostTweetsInDay])
        print("Month-Day with the happiest tweets: ", happiestDay, "with a sentiment score of: ", LargeMMDDSenTweets[happiestDay])
        print("Month-Day-Hour with the most tweets", mostTweetsInHour, "with a total of: ", LargehourTweets[mostTweetsInHour])
        print("Month-Day-Hour with the happiest tweets: ", happiestHour, "with a sentiment score of: ", LargehourSenTweets[happiestHour])
        
        print("Total time in seconds taken: ", MPI.Wtime() - start)

def process(f, chunk_size):
    """
    Helper function which takes a file object, and integer indicating the chunk
    size in bytes as input. Gathers all required data and return a list of 
    dictionaries storing this data.
    """

    # Keep track of how many tweets are made on a given month-day
    MMDDTweets = {}
    # Keep track of total sentiment of tweets of a given day in a given month-day
    MMDDSenTweets = {}
    # Keep track of how many tweets are made on a given month-day-hour
    hourTweets = {}
    # Keep track of total sentiment of tweets of a given hour of a given day in a given month-day-hour
    hourSenTweets = {}

    # Progress until we go past the starting point of next partition
    while (f.tell() < chunk_size * (comm.rank + 1)):
        record = get_next_record(f)

        if record is not None:

            recordDict = json.loads(record)

            item = recordDict["doc"]

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

            # Increment number of tweets in that given month-day combination
            MMDDTweets[MMDDKey] = MMDDTweets.get(MMDDKey, 0) + 1

            # Sum up sentiment for a given MMDD Combination
            MMDDSenTweets[MMDDKey] = MMDDSenTweets.get(MMDDKey, 0) + (sentiment if sentiment is not None else 0)

            # Get only the hour since we're only concerned with the hour. Both dateData and timeData are the same size so
            # the hours should line up with the day. We get Month-Day-Hour
            hour = (created_at.split("T")[0].split("-")[1], created_at.split("T")[0].split("-")[2], created_at.split("T")[1].split(".")[0].split(":")[0])
            hourKey = ''.join(hour)

            # Increment number of tweets in that given month-day-hour combination
            hourTweets[hourKey] = hourTweets.get(hourKey, 0) + 1

            # Sum up sentiment for a given MMDDHH combination
            hourSenTweets[hourKey] = hourSenTweets.get(hourKey, 0) + (sentiment if sentiment is not None else 0)

    return [MMDDTweets, MMDDSenTweets, hourTweets, hourSenTweets]
    
def next_record(f):
    """
    Helper function which progresses the input file object to the next record
    """
    while True:

        # Get to next line
        f.readline()

        # Ensure we see id section for next record
        position = f.tell()
        if f.readline()[:6] == '{"id":':
            f.seek(position)
            break

def get_next_record(f):
    """
    Helper function which retrieves and returns the next record in the input
    file.
    """
    result = ""
    while True:

        # Get to next line
        result += f.readline()

        # Ignore empty objects and end of file line
        if (result[:2] == "{}") or (result == ""):
            return None
        
        # Ensure we see id section for next record
        position = f.tell()
        f.seek(position)
        next_prefix = f.readline()[:6]
        if (next_prefix == '{"id":') or (len(next_prefix) < 6):
            
            # Strip off final new line characters to make complete json object
            result = result[:-2]
            break

    f.seek(position)

    return result

def combine_dicts(dicts):
    """
    Helper functions which takes a list of dictionaries as input, and combines 
    them into a single dictioanry where any common keys have their respective 
    values summed.
    """
    result = dicts[0]
    for dict in dicts[1:]:
        for key, value in dict.items():
            result[key] = result.get(key, 0) + value
    return result

if __name__ == "__main__":
    main()
