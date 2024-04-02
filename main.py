from mpi4py import MPI
import ijson

MAX_RECORDS = 1000
comm = MPI.COMM_WORLD

def main():

    # Setup data file
    if comm.rank == 0:
        f = open("twitter-50mb.json", "r", encoding="utf-8")
        items = ijson.items(f, "rows.item.doc")

        # Keep track of how many tweets are made on a given month-day
        LargeMMDDTweets = {}

        # Keep track of total sentiment of tweets of a given day in a given month-day
        LargeMMDDSenTweets = {}

        # Keep track of how many tweets are made on a given month-day-hour
        LargehourTweets = {}

        # Keep track of total sentiment of tweets of a given hour of a given day in a given month-day-hour
        LargehourSenTweets = {}

    is_file = True
    while (is_file):

        # Scatter the data
        if comm.rank == 0:
            records = read_data_chunk(items)
            tasks = divide_records(records)

            # Check if reached end of file
            if len(tasks[0]) != MAX_RECORDS:
                is_file = False
        
        # No other node has 'tasks' to scatter
        else:
            tasks = None

        # Ensure all tasks know is file has ended
        is_file = comm.bcast(is_file, root=0)

        # Scatter the tasks to each node and work
        task = comm.scatter(tasks, root=0)
        result = process(task)
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
        print("Month-Day with the most tweets: ", mostTweetsInDay)
        print("Month-Day-Hour with the most tweets", mostTweetsInHour)
        print("Month-Day with the happiest tweets: ", happiestDay, "with a sentiment score of: ", LargeMMDDSenTweets[happiestDay])
        print("Month-Day-Hour with the happiest tweets: ", happiestHour, "with a sentiment score of: ", LargehourSenTweets[happiestHour])

def read_data_chunk(items):
    """
    Helper function used to read fixed size data chunk into a list of records.
    For ease of implementation we don't completely preprocess data.
    """

    records = []
    for i in range(comm.size * MAX_RECORDS):

        # Ensure a next item exists, otherwise stop early
        try:
            item = next(items)
        except StopIteration: 
            break

        records.append(item)
    return records

def divide_records(records):
    """
    Helper function used to divide a larger list of json records into a list
    of comm.size smaller sized list of json records, or 'tasks'. 
    Simply wraps records into another list if comm.size = 1 for consistency.
    """

    tasks = []
    task_size = len(records) // comm.size
    for i in range(comm.size - 1):
        tasks.append(records[0:task_size])
        records = records[task_size:]

    # Ensure any remainders due to integer rounding are added to last 'task'
    tasks.append(records)

    return tasks
        


def process(task):
    """
    Helper function which calculates relevant information for subset of data task.
    Returns a list of dictionaries for all 4 desired statistics
    """

    # Keep track of how many tweets are made on a given month-day
    MMDDTweets = {}
    # Keep track of total sentiment of tweets of a given day in a given month-day
    MMDDSenTweets = {}
    # Keep track of how many tweets are made on a given month-day-hour
    hourTweets = {}
    # Keep track of total sentiment of tweets of a given hour of a given day in a given month-day-hour
    hourSenTweets = {}

    for item in task:
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



def combine_dicts(dicts):
    result = dicts[0]
    for dict in dicts[1:]:
        result = {k: result.get(k, 0) + dict.get(k, 0) for k in set(result) | set(dict)}
    return result


if __name__ == "__main__":
    main()