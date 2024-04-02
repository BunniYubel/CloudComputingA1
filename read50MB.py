import ijson
import json

# Read data using ijson (Attempting to get both sentiment value and date
with open("twitter-50mb.json", "r", encoding="utf-8") as f:
    items = ijson.items(f, "rows.item.doc")
    # Keep track of how many tweets are made on a given month-day
    MMDDTweets = {}
    # Keep track of how many tweets are made on a given month-day-hour
    hourTweets = {}
    # Keep track of total sentiment of tweets of a given day in a given month-day
    MMDDSenTweets = {}
    # Keep track of total sentiment of tweets of a given hour of a given day in a given month-day-hour
    hourSenTweets = {}

    for item in items:
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


mostTweetsInDay = max(MMDDTweets, key=MMDDTweets.get)
mostTweetsInHour = max(hourTweets, key=hourTweets.get)
happiestDay = max(MMDDSenTweets, key=MMDDSenTweets.get)
happiestHour = max(hourSenTweets, key=hourSenTweets.get)
print("Month-Day with the most tweets: ", mostTweetsInDay)
print("Month-Day with the happiest tweets: ", happiestDay, "with a sentiment score of: ", MMDDSenTweets[happiestDay])
print("Month-Day-Hour with the most tweets", mostTweetsInHour)
print("Month-Day-Hour with the happiest tweets: ", happiestHour, "with a sentiment score of: ", hourSenTweets[happiestHour])
