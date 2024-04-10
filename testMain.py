import ijson
import os
import multiprocessing


def process_partition(partition, total_MMDDTweets, total_hourTweets, total_MMDDSenTweets, total_hourSenTweets):
    try:
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

            # Get only the hour since we're only concerned with the hour. Both dateData and timeData are the same size
            # so the hours should line up with the day. We get Month-Day-Hour
            hour = (created_at.split("T")[0].split("-")[1], created_at.split("T")[0].split("-")[2],
                    created_at.split("T")[1].split(".")[0].split(":")[0])
            hourKey = ''.join(hour)

            # Increment number of tweets in that given month-day combination
            total_MMDDTweets[MMDDKey] = total_MMDDTweets.get(MMDDKey, 0) + 1
            # Sum up sentiment for a given MMDD Combination
            total_MMDDSenTweets[MMDDKey] = total_MMDDSenTweets.get(MMDDKey, 0) + (
                sentiment if sentiment is not None else 0)

            # Increment number of tweets in that given month-day-hour combination
            total_hourTweets[hourKey] = total_hourTweets.get(hourKey, 0) + 1
            # Sum up sentiment for a given MMDDHH combination
            total_hourSenTweets[hourKey] = total_hourSenTweets.get(hourKey, 0) + (
                sentiment if sentiment is not None else 0)
    except Exception as e:
        return None


if __name__ == '__main__':
    file_name = 'twitter-50mb.json'

    # Get number of cores I can use
    core_count = multiprocessing.cpu_count()
    # How big is the file
    file_size = os.path.getsize(file_name)
    # Size of each partition for each core
    partition_size = file_size // core_count

    # Global dictionaries to aggregate data after sending out the partitions to the cores
    manager = multiprocessing.Manager()
    total_MMDDTweets = manager.dict()
    total_hourTweets = manager.dict()
    total_MMDDSenTweets = manager.dict()
    total_hourSenTweets = manager.dict()

    with open(file_name, 'r', encoding='utf-8') as file:
        start = 0
        end = partition_size

        # Store all the different process objects that we'll later send out to all the cores
        processes = []

        while start < file_size:
            # Start reading at where the last partition ended
            file.seek(start)
            # Read the specific parts of the code per partition
            partitions = list(ijson.items(file, "rows.item.doc", multiple_values=True))

            # Creating a new process object that will later be sent out to the cores
            new_process = multiprocessing.Process(target=process_partition, args=(partitions, total_MMDDTweets,
                                                                                  total_hourTweets, total_MMDDSenTweets,
                                                                                  total_hourSenTweets))
            new_process.start()
            # Add the object to the list of processes
            processes.append(new_process)

            # Update to the next partition
            start = end
            # Make sure we don't go over the file size
            end = min(start + partition_size, file_size)

        for process in processes:
            process.join()
        # Convert the shared dictionaries into normal ones to grab the key value pairs
        merge_MMDDTweets = dict(total_MMDDTweets)
        merge_hourTweets = dict(total_hourTweets)
        merge_MMDDSenTweets = dict(total_MMDDSenTweets)
        merge_hourSenTweets = dict(total_hourSenTweets)

        # Grab the information we need
        mostTweetsInDay = max(merge_MMDDTweets, key=merge_MMDDTweets.get)
        mostTweetsInHour = max(merge_hourTweets, key=merge_hourTweets.get)
        happiestDay = max(merge_MMDDSenTweets, key=merge_MMDDSenTweets.get)
        happiestHour = max(merge_hourSenTweets, key=merge_hourSenTweets.get)

        # Print the answers
        print("Month-Day with the most tweets: ", mostTweetsInDay)
        print("Month-Day with the happiest tweets: ", happiestDay, "with a sentiment score of: ",
              merge_MMDDTweets[happiestDay])
        print("Month-Day-Hour with the most tweets", mostTweetsInHour)
        print("Month-Day-Hour with the happiest tweets: ", happiestHour, "with a sentiment score of: ",
              merge_hourSenTweets[happiestHour])
