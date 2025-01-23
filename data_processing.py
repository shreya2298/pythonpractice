import datetime

def proc_data(message):
    message["tempF"] = (message["temperature"]*9/5)+32
    message["date"] = datetime.datetime.fromtimestamp(message["timestamp"], datetime.UTC).strftime('%d-%m-%Y %H:%M:%S')
    return message