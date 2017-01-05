import urllib2
import json
import datetime
import csv
import time

page_id = "cnn"

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

    return response.read()

# Needed to write tricky unicode correctly to csv
def unicode_normalize(text):
    return text.translate({ 0x2018:0x27, 0x2019:0x27, 0x201C:0x22, 0x201D:0x22,
                            0xa0:0x20 }).encode('utf-8')

def getFacebookPageConversationsData(page_id, access_token, limit):

    # Construct the URL string; see http://stackoverflow.com/a/37239851 for
    # Reactions parameters
    base = "https://graph.facebook.com/v2.6"
    node = "/%s/conversations" % page_id
    fields = "/?fields=message_count,updated_time,id"
    parameters = "&limit=%s&access_token=%s" % (limit, access_token)
    url = base + node + fields + parameters

    # retrieve data
    data = json.loads(request_until_succeed(url))

    return data


def processFacebookConversations(conv, access_token):

    # The Conversations now is a Python dictionary, so for top-level items,
    # we can simply call the key.

    # Additionally, some items may not always exist,
    # so must check for existence first

    conv_id = conv['id']
    message_count = 0 if 'message_count' not in conv.keys() else \
            conv['message_count']

    # Time needs special care since a) it's in UTC and
    # b) it's not easy to use in statistical programs.

    updated_time = datetime.datetime.strptime(
        conv['updated_time'],'%Y-%m-%dT%H:%M:%S+0000')
    updated_time = updated_time.strftime(
            '%Y-%m-%d %H:%M:%S') # best time format for spreadsheet programs

    return (conv_id, message_count, updated_time)


def scrapeFacebookConvs(page_id, access_token):
    with open('%s_facebook_conversations.csv' % page_id, 'wb') as file:
        w = csv.writer(file)
        w.writerow(["id", "message_count", "updated_time"])

        has_next_page = True
        num_processed = 0   # keep a count on how many we've processed
        scrape_starttime = datetime.datetime.now()

        print "Scraping %s Facebook Page: %s\n" % (page_id, scrape_starttime)

        convs = getFacebookPageConversationsData(page_id, access_token, 100)

        while has_next_page:
            for conv in convs['data']:

                w.writerow(processFacebookConversations(conv,
                    access_token))

                # output progress occasionally to make sure code is not
                # stalling
                num_processed += 1
                if num_processed % 100 == 0:
                    print "%s Conversations Processed: %s" % \
                        (num_processed, datetime.datetime.now())

            # if there is no next page, we're done.
            if 'paging' in convs.keys():
                convs = json.loads(request_until_succeed(
                    convs['paging']['next']))
            else:
                has_next_page = False


        print "\nDone!\n%s Conversations Processed in %s" % \
                (num_processed, datetime.datetime.now() - scrape_starttime)


if __name__ == '__main__':
    scrapeFacebookConvs(page_id, page_access_token)


# The CSV can be opened in all major statistical programs. Have fun! :)
