import urllib2
import json
import datetime
import csv
import time

app_id = "<FILL IN>"
app_secret = "<FILL IN>" # DO NOT SHARE WITH ANYONE!
file_id = "cnn"

page_access_token = "<FILL IN>"

def request_until_succeed(url):
    req = urllib2.Request(url)
    success = False
    while success is False:
        try:
            response = urllib2.urlopen(req)
            if response.getcode() == 200:
                success = True
        except Exception, e:
            print e
            time.sleep(5)

            print "Error for URL %s: %s" % (url, datetime.datetime.now())
            print "Retrying."

            if '400' in str(e):
                return None;

    return response.read()

# Needed to write tricky unicode correctly to csv
def unicode_normalize(text):
    return text.translate({ 0x2018:0x27, 0x2019:0x27, 0x201C:0x22,
                            0x201D:0x22, 0xa0:0x20 }).encode('utf-8')

def getFacebookMessegsData(conv_id, access_token, limit):

    # Construct the URL string
        base = "https://graph.facebook.com/v2.6"
        node = "/%s/messages" % conv_id
        fields = "?fields=id,message,created_time,from"
        parameters = "&limit=%s&access_token=%s" % \
                     (limit, access_token)
        url = base + node + fields + parameters

        # retrieve data
        data = request_until_succeed(url)
        if data is None:
            return None
        else:
            return json.loads(data)

def processFacebookMessage(message, conv_id, parent_id =''):

    # The message is now a Python dictionary, so for top-level items,
    # we can simply call the key.

    # Additionally, some items may not always exist,
    # so must check for existence first

    message_id = message['id']
    message_text = '' if 'message' not in message else \
            unicode_normalize(message['message'])
    message_author = unicode_normalize(message['from']['name'])

    if 'attachment' in message:
        attach_tag = "[[%s]]" % message['attachment']['type'].upper()
        message_text = attach_tag if message_text is '' else \
                (message_text.decode("utf-8") + " " + \
                        attach_tag).encode("utf-8")

    # Time needs special care since a) it's in UTC and
    # b) it's not easy to use in statistical programs.

    created_time = datetime.datetime.strptime(
            message['created_time'], '%Y-%m-%dT%H:%M:%S+0000')
    created_time = created_time.strftime(
            '%Y-%m-%d %H:%M:%S') # best time format for spreadsheet programs

    # Return a tuple of all processed data

    return (message_id, conv_id, message_text, message_author,
            created_time)

def scrapeFacebookPageMessages(page_id, access_token):
    with open('%s_facebook_messages.csv' % file_id, 'wb') as file:
        w = csv.writer(file)
        w.writerow(["message_id", "conv_id", "message_text",
            "message_author", "created_time"])

        num_processed = 0   # keep a count on how many we've processed
        scrape_starttime = datetime.datetime.now()

        print "Scraping %s Messages From Conversation: %s\n" % \
                (file_id, scrape_starttime)

        with open('%s_facebook_conversations.csv' % file_id, 'rb') as csvfile:
            reader = csv.DictReader(csvfile)

            #reader = [dict(status_id='759985267390294_1158001970921953')]

            for conv in reader:
                has_next_page = True

                messages = getFacebookMessegsData(conv['id'],
                        access_token, 100)

                while has_next_page and messages is not None:
                    for message in messages['data']:
                        w.writerow(processFacebookMessage(message,
                                                          conv['id']))

                        # output progress occasionally to make sure code is not
                        # stalling
                        num_processed += 1
                        if num_processed % 1000 == 0:
                            print "%s Messages Processed: %s" % \
                                    (num_processed, datetime.datetime.now())

                    if 'paging' in messages:
                        if 'next' in messages['paging']:
                            messages = json.loads(request_until_succeed(
                                        messages['paging']['next']))
                        else:
                            has_next_page = False
                    else:
                        has_next_page = False


        print "\nDone!\n%s Messages Processed in %s" % \
                (num_processed, datetime.datetime.now() - scrape_starttime)


if __name__ == '__main__':
    scrapeFacebookPageMessages(file_id, page_access_token)


# The CSV can be opened in all major statistical programs. Have fun! :)
