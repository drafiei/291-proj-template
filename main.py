import sqlite3
import time
import hashlib

connection = sqlite3.connect(path)
cursor = connection.cursor()


def connect(path):
    global connection, cursor

    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    cursor.execute(' PRAGMA foreign_keys=ON; ')
    connection.commit()
    return


def drop_tables():
    global connection, cursor
    drop_includes = "drop table if exists includes; "
    drop_lists = "drop table if exists lists; "
    drop_retweets = "drop table if exists retweets; "
    drop_mentions = "drop table if exists mentions; "
    drop_hashtags = "drop table if exists hashtags; "
    drop_tweets = "drop table if exists tweets; "
    drop_follows = "drop table if exists follows; "
    drop_users = "drop table if exists users"
    cursor.execute(drop_includes)
    cursor.execute(drop_lists)
    cursor.execute(drop_retweets)
    cursor.execute(drop_mentions)
    cursor.execute(drop_hashtags)
    cursor.execute(drop_tweets)
    cursor.execute(drop_follows)
    cursor.execute(drop_users)


def define_tables():
    global connection, cursor

    users_query = '''
                          CREATE TABLE users (
                                      usr INTEGER,
                                      pwd TEXT,
                                      name TEXT,
                                      email TEXT,
                                      city TEXT,
                                      timezone FLOAT,
                                      PRIMARY KEY (usr)
                                      );
                      '''
    follows_query = '''
                        CREATE TABLE follows (
                                    flwer INTEGER,
                                    flwee INTEGER,
                                    start_date DATE,
                                    PRIMARY KEY (flwer,flwee),
                                    FOREIGN KEY (flwer) REFERENCES users(usr),
                                    FOREIGN KEY (flwee) REFERENCES users(usr)
                                    );
                      '''

    tweets_query = '''
                        CREATE TABLE tweets (
                                    tid INTEGER,
                                    writer INTEGER,
                                    tdate DATE,
                                    text TEXT,
                                    replyto INTEGER,
                                    PRIMARY KEY (tid),
                                    FOREIGN KEY(writer) REFERENCES users(usr),
                                    FOREIGN KEY(replyto) REFERENCES tweets(writer)
                                    );
                    '''
    hashtags_query = '''
                            CREATE TABLE hashtags (
                                       term TEXT,
                                       PRIMARY KEY (term)
                                        );
                        '''
    mentions_query = '''
                            CREATE TABLE mentions (
                                        tid INTEGER,
                                        term TEXT,
                                        PRIMARY KEY (tid,term),
                                        FOREIGN KEY(tid) REFERENCES tweets(tid),
                                        FOREIGN KEY(term) REFERENCES hashtags(term)
                                        );
                        '''
    retweets_query = '''
                            CREATE TABLE retweets (
                                        usr INTEGER,
                                        tid INTEGER,
                                        rdate DATE,
                                        PRIMARY KEY (usr,tid),
                                        FOREIGN KEY(usr) REFERENCES users(usr),
                                        FOREIGN KEY(tid) REFERENCES tweets(tid)
                                        );
                        '''
    lists_query = '''
                                CREATE TABLE lists (
                                            lname TEXT,
                                            owner INTEGER,
                                            PRIMARY KEY (lname),
                                            FOREIGN KEY(owner) REFERENCES users(usr)
                                            );
                            '''
    includes_query = '''
                                CREATE TABLE includes (
                                            lname TEXT,
                                            member INTEGER
                                            PRIMARY KEY (lname,member),
                                            FOREIGN KEY(lname) REFERENCES lists(lname),
                                            FOREIGN KEY(member) REFERENCES users(usr)
                                            );
                            '''
    cursor.execute(users_query)
    cursor.execute(follows_query)
    cursor.execute(tweets_query)
    cursor.execute(hashtags_query)
    cursor.execute(mentions_query)
    cursor.execute(retweets_query)
    cursor.execute(lists_query)
    cursor.execute(includes_query)
    connection.commit()
    return


def question_1():
    Input = input("Enter a keyword or hashtag: ")
    offset = 0
    offsets = -1
    batch_size = 5
    j = 0
    array = []
    while True:
        # Display the current batch of tweets
        keywords = [word[1:] if word.startswith('#') else word for word in Input.split()]
        for word in keywords:
            # search_query = f"%{word}%"
            search_sql = '''
            SELECT distinct(*) FROM tweets t
            WHERE t.text LIKE ?
            UNION
            SELECT t.* 
            FROM tweets t
            JOIN mentions m ON t.tid = m.tid
            WHERE m.term = ?                         
            ORDER BY t.tdate DESC
            LIMIT 5 OFFSET ?;  
            '''
            cursor.execute(search_sql, (word, word, offset))  # last three are input for injection
            result = cursor.fetchall()
            if result is not None:
                array[j].append(result)
                j = j + 1
        for i, array in enumerate(array, start=offsets + 1):  # 0-4,total 5
            tweet = array[i]
            tid, writer, tdate, text, replyto = tweet
            print(f"{i}. {text}")
        user_input = input("which one do you want to see more specific(1,2,3,4,5)")

        if 1 <= int(user_input) <= 5:
            print("here is the tweet static number of retweets and replies")
            index = int(user_input) - 1
            tweets = array[index]  # tweets[0]is tid
            retweetnumber = '''
                    SELECT COUNT(DISTINCT usr) AS retweetcount
                    FROM retweets r
                    JOIN tweets t ON r.tid = t.tid
                    WHERE t.tid = ?
                    ;
                    '''
            cursor.execute(retweetnumber, tweets[i])
            retweet = cursor.fetchall()
            replynumber = '''
                    SELECT COUNT(DISTINCT t1.usr) AS reply
                    FROM tweets t, tweets t1
                    WHERE t.tid = t1.replyto
                    AND t.tid = ?
                    ;
                    '''
            cursor.execute(replynumber, tweets[i])
            reply = cursor.fetchall()
            print(f"{int(user_input)},{retweet},{reply}")

        # Ask the user if they want to see more tweets
        use_input = input("Do you want to see more (yes/no)? ")
        if use_input.lower() != "yes":
            break  # Exit the loop if the user's input is not "yes"

        offset += batch_size
    return


def question3(tid, writer, tdate):
    global connection
    global cursor
    Input = input("write for your tweet")
    hashtags = [word[1:] for word in Input.split() if word.startswith('#')]
    tweet = '''
    INSERT INTO tweets (tid,writer, tdate, text) VALUES (?, ?, ?,?)
    '''
    cursor.execute(tweet, (tid, writer, tdate, Input))
    for i in hashtags:
        check_hashtag = '''
        INSERT OR IGNORE INTO hashtags (term) VALUES (?)
        '''
        cursor.execute(check_hashtag, i)
        mention = '''
        INSERT INTO mentions (tid, term) VALUES (?, ?)
        '''
        cursor.execute(mention, (tid, i))
    return


def main():
    global connection, cursor
    path = "./register.db"
    connect(path)
    drop_tables()
    define_tables()

    connection.commit()
    connection.close()
    return


if __name__ == "__main__":
    main()
