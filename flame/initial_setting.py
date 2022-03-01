#!/usr/bin/env python
import re
from datetime import datetime, timedelta, date
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
import time, sys, traceback
import mysql.connector
from tweet_getter import TweetGetter
from requests_oauthlib import OAuth1Session



'''
crontab -eの場合は以下のimport
'''
DB_USER = ***
DB_PASSWORD = ***
DB_HOST = ***
DB_NAME = ***
CHARSET = ***

TARGET_WORD = ***
CONSUMER_KEY = ***
CONSUMER_SECRET = ***
ACCESS_TOKEN = ***
ACCESS_TOKEN_SECRET = ***

twitter = OAuth1Session(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
tw = TweetGetter(twitter)
# TwitterAPIのアクセスキートークン

exception_list = []
# 学研の文字を含む関係の無さそうな文字列のリスト
noise_list = ['考古学研', '獣医学研', '数学研', '理科学研', '物理学研', '化学研', '生物学研', '地学研',
              '情報学研', '日本文学研', '外国文学研', '詩学研', '史学研', '地理学研', '哲学研', '倫理学研',
              '宗教学研', '心理学研', '行動学研', '文化学研', '教養学研', '人間学研', '美術学研', 'デザイン学研',
              '芸術学研', '日本語学研', '外国語学研', '教育学研', '法学研', '政治学研', '国際関係学研', '経済学研',
              '経営学研', '商学研', '経営情報学研', 'メディア学研', '社会学研', '社会福祉学研', '環境学研', '観光学研',
              'マスコミ学研', '地球科学研', '機械工学研', 'システム工学研', '航空学研', '宇宙工学研', '電子工学研',
              '通信学研', '情報工学研', '建築学研', '環境工学研', '材料工学研', '応用物理学研', '応用化学研', '生物工学研',
              'エネルギー工学研', '経営工学研', '海洋工学研', '農学研', '農芸化学研', '農業工学研', '林学研', '農業経済学研',
              '畜産学研', '酪農学研', '水産学研', '医学研', '看護学研', '医療技術学研', 'スポーツ健康学研', '歯学研', '保健福祉学研',
              '薬学研', '栄養学研', '被服学研', '児童学研', '住居学研', '生活学研', '学研都市', '学研奈良', '関西学研', '医科学研', '学研・東西・宝塚線',
              '学研北生駒', '学研奈良登美ヶ丘', '学研通り', '学研降り口']



def now_unix_time():
    return time.mktime(datetime.now().timetuple())


def execute_sql(sql, db_info, is_commit=False):
    '''
    SQL文の実行
    '''
    connector = mysql.connector.connect(
        host=db_info["host"],
        port=3306,
        user=db_info["user"],
        password=db_info["password"],
        db=db_info["db_name"],
        charset="utf8"
    )
    cursor = connector.cursor()
    cursor.execute(sql)
    if is_commit:
        connector.commit()
    cursor.close()
    connector.close()
    return True


def create_hashtag_serch_table(db_info):
    '''
    database内にtableを作成
    '''
    sql = """
    CREATE TABLE IF NOT EXISTS
        initial_day(
            tweet_id BIGINT,
            day_id DATETIME,
            created_at DATETIME,
            user_id BIGINT,
            user_name VARCHAR(50),
            user_friends MEDIUMINT,
            user_followers MEDIUMINT,
            retweet_count MEDIUMINT,
            favorite_count MEDIUMINT,
            text VARCHAR(255)
        )
    ;
    """
    execute_sql(sql, db_info, is_commit=True)
    return True


def insert_into_hashtag_search(db_info, hashtag_search_dict):
    '''
    作成したテーブル内にデータを挿入
    '''
    sql = """
    INSERT INTO
        initial_day
    VALUES(
        '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s' , '%s'
        )
    ;
    """ % (
        hashtag_search_dict["tweet_id"],
        hashtag_search_dict['day_id'],
        hashtag_search_dict["created_at"],
        hashtag_search_dict["user_id"],
        hashtag_search_dict["user_name"],
        hashtag_search_dict["user_friends"],
        hashtag_search_dict["user_followers"],
        hashtag_search_dict["retweet_count"],
        hashtag_search_dict["favorite_count"],
        hashtag_search_dict["text"]
    )
    execute_sql(sql, db_info, is_commit=True)
    return True


def tweet_main():
    sid = -1
    mid = -1
    count = 0
    week_ago = date.today() - timedelta(days=1)
    local_db = {
        "host": DB_HOST,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "db_name": DB_NAME
    }
    # テーブル作成
    create_hashtag_serch_table(local_db)
    while True:
        try:
            count += 1
            sys.stdout.write('%d, ' % count)
            # ここにデータベースがもし存在していたら，そのマックスの時間を入れて，その時間以上のものからログデータを取らせる様にする
            tweet_data = tw.get_zeroday_tweet_data(u'学研', max_id=mid, since_id=sid, start_date=week_ago)
            if tweet_data['result'] == False:
                print("status_code{}".format(tweet_data['status_code']))
                break

            if int(tweet_data['limit']) == 0:
                print('Adding created_at field')
                diff_sec = int(tweet_data['reset_time_unix']) - now_unix_time()
                print("sleep %d sec." % (diff_sec + 5))
                break
            else:
                # metadata処理
                if len(tweet_data['statuses']) == 0:
                    sys.stdout.write("statuses is none.")
                    break
                elif 'next_results' in tweet_data['metadata']:

                    # 結果をMySQLに格納する
                    tweet_data_st = tweet_data['statuses']
                    for tweet in tweet_data_st:
                        if (tweet['user']['screen_name'] not in exception_list) & (len(re.findall(r'【*】', tweet['text'])) == 0):
                            if len([s for s in noise_list if s in tweet['text']]) == 0:
                                # データのストア
                                tweet['text'] = tw.tweet_cleaner(tweet['text'])
                                hashtag_search_dict = {
                                    "tweet_id": u"{}".format(tweet['id']),
                                    "day_id": u"{}".format(date.today()),
                                    "created_at": u"{}".format(tweet['created_at']),
                                    "user_id": u"{}".format(tweet['user']['id']),
                                    "user_name": u"{}".format(tweet['user']['screen_name']),
                                    "user_friends": u"{}".format(tweet['user']['friends_count']),
                                    "user_followers": u"{}".format(tweet['user']['followers_count']),
                                    'retweet_count': u'{}'.format(tweet['retweet_count']),
                                    'favorite_count': u'{}'.format(tweet['favorite_count']),
                                    "text": u"{}".format(tweet['text'])
                                }
                                insert_into_hashtag_search(local_db, hashtag_search_dict)

                    next_url = tweet_data['metadata']['next_results']
                    pattern = r".*max_id=([0-9]*)\&.*"
                    ite = re.finditer(pattern, next_url)
                    for i in ite:
                        mid = i.group(1)
                        break
                else:
                    sys.stdout.write("next is none. finished.")
                    break
        except SSLError:
            print("SSLError")
            print("waiting 5mins")
            time.sleep(5 * 60)
        except ConnectionError:
            print("ConnectionError")
            print("waiting 5mins")
            time.sleep(5 * 60)
        except ReadTimeout:
            print("ReadTimeout")
            print("waiting 5mins")
            time.sleep(5 * 60)
        except:
            print("Unexpected error:{}".format(sys.exc_info()[0]))
            traceback.format_exc(sys.exc_info()[2])
            raise
        finally:
            info = sys.exc_info()
    return True

tweet_main()
