def time__seconds_to_string(seconds):
    seconds = int(seconds)
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    
    res = ""
    if days > 0:
        res += f"{days} days, "
    res += f"{hours} hours, {minutes} minutes, {seconds} seconds"
    return res


def classify_rfm(recency, frequency, monetary):
    if recency <= 10 and frequency > 20 and monetary > 25000:
        return "High-Value Loyal Customer"
    elif recency > 30:
        return "Lost Customer"
    elif frequency <= 5 and monetary < 2000:
        return "Low-Value Customer"
    else:
        return "Regular Customer"
    

def categorize_hour(hour):
    if hour is None or hour < 0 or hour > 24:
        return "Unknown"
    if 5 <= hour < 11:
        return "Early Morning (5 AM - 11 AM)"
    elif 11 <= hour < 13:
        return "Noon (11 AM - 1 PM)"
    elif 13 <= hour < 18:
        return "Afternoon (1 PM - 6 PM)"
    elif 18 <= hour < 22:
        return "Evening (6 PM - 10 PM)"
    else:
        return "Night (10 PM - 5 AM)"
