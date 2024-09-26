import sys
import json

from collections import Counter
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QListWidget,
    QListWidgetItem,
    QLabel,
    QSplitter,
)
from PySide6.QtGui import QPixmap
from PySide6.QtCore import *
from PySide6.QtGui import *
from PySide6.QtWidgets import *
from PySide6 import QtCore


import findspark

findspark.find()
findspark.init()

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HW1").master("local[*]").getOrCreate()

sc = spark.sparkContext


# Define Parameters
owner_s = 1

# following parameters should not be equal to owner_s
rep_to_s = 0.2
rep_from_s = 0.9

ret_to_s = 0.5
ret_from_s = 1.5

qut_to_s = 0.6
qut_from_s = 1.5


global gflag1

class SerialReader(QThread):
    dataset = {}
    s = Signal(tuple)
    q_uid = None

    def init(self):
        super().init()

    def run(self):
        print(f"extracting favorite tweets ids for (user_id={self.q_uid})...")
        if self.q_uid in self.dataset:
            data = self.dataset[self.q_uid]
            self.s.emit(data)
            print("number of favorite tweets extracted:", len(data[1]))
            return
        data = ([], [])
        try:
            data = extract_tweets_id(self.q_uid)
            self.dataset[self.q_uid] = data
            print("number of favorite tweets extracted:", len(data[1]))
        except Exception as e:
            print(e)
        finally:
            self.s.emit(data)


class SerialReader2(QThread):
    s = Signal(list)
    q_uid = None
    user_favorites = None
    mcru_id = None

    def init(self):
        super().init()

    def run(self):
        data = []
        try:
            print(f"extracting recommended users for (user_id={self.q_uid})...")
            mcru = get_recommended_users(self.user_favorites, self.q_uid)
            data.append(mcru)
            print("number of recommended users extracted:", len(mcru))

            self.mcru_id, _ = zip(*mcru)
            print("extracting recommended users' tweets...")
            data.append(extract_rec_users_tweets(self.q_uid, self.mcru_id))
            print("number of recommended users' tweets extracted:", len(data[1]))
        except Exception as e:
            print(e)
        finally:
            self.s.emit(data)


class ContactWidgetItem(QWidget):
    clicked = Signal()

    def __init__(
        self, name, image_url=None, color=None, width=40, height=40, parent=None
    ):
        super().__init__(parent)

        # Create QLabel for the contact image

        # Load image from URL and resize
        if image_url:
            self.image_label = QLabel()
            pixmap = QPixmap(image_url)
            self.image_label.setPixmap(pixmap.scaled(width, height, Qt.KeepAspectRatio))

            # Set the resized pixmap to the QLabel

            # Create QLabel for the contact name
            self.name_label = QLabel(name)
            if color:
                self.name_label.setStyleSheet(f"background-color: {color}")
            # Layout for the custom widget
            layout = QHBoxLayout()

            layout.addWidget(self.image_label, 2)
            layout.addWidget(self.name_label, 5)

            self.setLayout(layout)

        else:
            self.name_label = QLabel(name)

    def mousePressEvent(self, e):
        global gflag1
        if gflag1 == 1:
            self.name_label.setStyleSheet(f"background-color: yellow")
        self.clicked.emit()


class Messages(QWidget):
    def __init__(
        self, name1, name2, name3, message, image_url="user-286.png", parent=None
    ):
        super().__init__(parent)
        # self.width = 500
        # self.height = 100
        # self.setFixedSize(self.size())
        x1 = 60
        y1 = 60
        x2 = 300
        y2 = 15
        x3 = 300
        y3 = 10
        xl = 15
        yl = 1
        main_layout = QVBoxLayout(self)
        top_layout = QHBoxLayout()
        top_right_layout = QVBoxLayout()

        self.image_label = QLabel()
        pixmap = QPixmap(image_url)
        self.image_label.setPixmap(pixmap.scaled(x1, y1, Qt.KeepAspectRatio))

        self.name_label = QLabel(name1)
        self.name_label.setStyleSheet("color: blue")
        self.name_label.setFont(QFont("Times New Roman", 10))
        self.name_label.setAlignment(Qt.AlignTop | Qt.AlignLeft)

        self.name_label2 = QLabel("Name: " + name2)
        self.name_label2.setStyleSheet("color: red")
        self.name_label2.setFont(QFont("Comic Sans MS", 12))
        self.name_label2.setAlignment(Qt.AlignTop | Qt.AlignLeft)

        self.name_label3 = QLabel("username: " + name3)
        self.name_label3.setStyleSheet("color: green")
        self.name_label3.setFont(QFont("Comic Sans MS", 10))
        self.name_label3.setAlignment(Qt.AlignTop | Qt.AlignLeft)

        top_right_layout.addWidget(self.name_label)
        top_right_layout.addWidget(self.name_label2)
        top_right_layout.addWidget(self.name_label3)
        self.name_label.setFixedSize(x2, y2)
        self.name_label2.setFixedSize(x2, 20)
        self.name_label3.setFixedSize(x2, y2)

        wd = QWidget()
        wd.setLayout(top_right_layout)

        top_layout.addWidget(self.image_label, 1)
        top_layout.addWidget(wd, 10)

        self.content = QLabel(message)
        self.content.setAlignment(Qt.AlignTop | Qt.AlignRight)
        self.content.setWordWrap(True)
        self.content.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        main_layout.addLayout(top_layout)
        main_layout.addWidget(self.content)


class TwitterApp(QMainWindow):

    def __init__(self, users):
        super().__init__()
        self.users = users
        self.setWindowTitle("Twitter-Like App")
        self.setGeometry(50, 50, 1000, 700)

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)

        self.setup_ui()

        self.last_opt = None
        self.flag1 = 1
        global gflag1
        gflag1 = 1
        self.flag2 = 0
        self.contact = None
        self.opt = None
        self.q_uid = 0
        self.serial_reader = SerialReader()
        self.serial_reader.s.connect(self.update_flag)

        self.serial_reader2 = SerialReader2()
        self.serial_reader2.s.connect(self.update_flag2)

        # self.serial_reader3 = SerialReader3()
        # self.serial_reader3.s.connect(self.update_flag3)

    def update_flag(self, data):
        self.user_favorites = data[0]
        self.favorites_tweets = data[1]
        self.flag1 = 1
        global gflag1
        gflag1 = 1
        if self.last_opt:
            self.options(opt=self.last_opt)

    def update_flag2(self, data):
        self.users = data[0]
        self.favorites_tweets = data[1]

        self.contacts_list.close()
        self.contacts_list = QListWidget()

        contact_widget = ContactWidgetItem(str(self.q_uid), "user-286.png", color="red")
        contact_widget.clicked.connect(self.click_contact)
        list_item = QListWidgetItem(self.contacts_list)
        list_item.setSizeHint(contact_widget.sizeHint())
        self.contacts_list.setItemWidget(list_item, contact_widget)

        for i in range(len(self.users)):
            contact_widget = ContactWidgetItem(
                str(self.users[i][0]), "user-286.png", color="green"
            )
            contact_widget.clicked.connect(self.click_contact)
            list_item = QListWidgetItem(self.contacts_list)
            list_item.setSizeHint(contact_widget.sizeHint())
            self.contacts_list.setItemWidget(list_item, contact_widget)

        self.left_layout.addWidget(self.contacts_list)
        self.flag1 = 1
        global gflag1
        gflag1 = 1

    def setup_ui(self):
        main_layout = QHBoxLayout(self.central_widget)
        splitter = QSplitter(Qt.Horizontal)

        self.left_layout = QVBoxLayout()
        right_layout = QVBoxLayout()
        topright_layout = QHBoxLayout()
        self.contacts_list = QListWidget()
        self.message_lst = QListWidget()

        for i in range(min(50, len(self.users))):
            contact_widget = ContactWidgetItem(str(self.users[i][0]), "user-286.png")
            contact_widget.clicked.connect(self.click_contact)
            list_item = QListWidgetItem(self.contacts_list)
            list_item.setSizeHint(contact_widget.sizeHint())
            self.contacts_list.setItemWidget(list_item, contact_widget)

        self.left_layout.addWidget(self.contacts_list)

        self.opt_name = [
            "origin",
            "quoted",
            "retweeted",
            "replied",
            "explore",
            "Recommended Users",
        ]
        for i in range(len(self.opt_name)):
            options_label = QPushButton(self.opt_name[i])
            options_label.clicked.connect(self.options)
            topright_layout.addWidget(options_label)
            if i == 4:
                self.explore_button = options_label
            if i == 5:
                self.recommended_button = options_label

        # self.msg_widget = QWidget()
        self.bottomright_layout = QVBoxLayout()
        self.progress_bar = QProgressBar()
        self.bottomright_layout.addWidget(self.message_lst)
        self.bottomright_layout.addWidget(self.progress_bar)

        self.progress_bar.hide()

        right_layout.addLayout(topright_layout)
        right_layout.addLayout(self.bottomright_layout)

        splitter.addWidget(QWidget())
        splitter.addWidget(QWidget())

        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 4)

        main_layout.addWidget(splitter)

        splitter.widget(0).setLayout(self.left_layout)
        splitter.widget(1).setLayout(right_layout)

    def click_contact(self):
        sender_widget = self.sender()
        if isinstance(sender_widget, ContactWidgetItem):
            if self.flag1 == 1:
                self.contact = sender_widget.name_label.text()
                if self.q_uid != self.contact:
                    self.q_uid = self.contact
                    if self.flag2 == 0:
                        self.serial_reader.q_uid = self.q_uid
                        self.serial_reader.start()
                        self.progress_bar.setRange(0, 0)
                        self.progress_bar.show()
                        self.flag1 = 0
                        global gflag1
                        gflag1 = 0
                    elif self.last_opt:
                        if self.opt_name.index(self.last_opt) < 4:
                            self.options(opt=self.last_opt)
                QtCore.QTimer.singleShot(100, lambda: self.hide_progress_bar())

    def hide_progress_bar(self):
        if self.flag1 != 1:
            QtCore.QTimer.singleShot(100, lambda: self.hide_progress_bar())
        else:
            self.progress_bar.hide()

    def options(self, opt=None):

        if self.contact == None or self.flag1 == 0:
            return
            # self.message_lst.close()
            # self.progress_bar.close()
            # self.progress_bar = QProgressBar()
            # self.message_lst = QListWidget()
            # contact_widget = QLabel("Please Choose a User")
            # list_item = QListWidgetItem(self.message_lst)
            # list_item.setSizeHint(contact_widget.sizeHint())
            # self.message_lst.setItemWidget(list_item, contact_widget)
            # self.bottomright_layout.addWidget(self.message_lst)
            # self.bottomright_layout.addWidget(self.progress_bar)
            # self.progress_bar.hide()

        else:
            sender_widget = self.sender()
            if opt:
                self.opt = opt
            else:
                self.opt = sender_widget.text()
            self.last_opt = self.opt

            self.message_lst.close()
            self.message_lst = QListWidget()
            if self.opt == self.opt_name[5] and self.flag1 == 1:
                if self.user_favorites:
                    self.flag1 = 0
                    global gflag1
                    gflag1 = 0
                    self.flag2 = 1
                    self.serial_reader2.q_uid = self.q_uid
                    self.serial_reader2.user_favorites = self.user_favorites
                    self.serial_reader2.start()
                    contact_widget = QLabel("please wait...")
                    list_item = QListWidgetItem(self.message_lst)
                    list_item.setSizeHint(contact_widget.sizeHint())
                    self.message_lst.setItemWidget(list_item, contact_widget)
                    self.progress_bar.setRange(0, 0)
                    self.progress_bar.show()
                    # self.bottomright_layout.addWidget(self.progress_bar)
                    self.explore_button.close()
                    self.recommended_button.close()
            else:
                self.progress_bar.close()
                self.progress_bar = QProgressBar()
                ############################################START##############################################
                if self.opt == self.opt_name[0]:
                    tweets = extract_user_origin_tweets(
                        self.favorites_tweets, self.q_uid
                    )
                    message_number = len(tweets)
                if self.opt == self.opt_name[1]:
                    tweets = extract_user_quoted_tweets(
                        self.favorites_tweets, self.q_uid
                    )
                    message_number = len(tweets)
                if self.opt == self.opt_name[2]:
                    tweets = extract_user_retweeted_tweets(
                        self.favorites_tweets, self.q_uid
                    )
                    message_number = len(tweets)
                if self.opt == self.opt_name[3]:
                    tweets = extract_user_replied_tweets(
                        self.favorites_tweets, self.q_uid
                    )
                    message_number = len(tweets)
                if self.opt == self.opt_name[4]:
                    tweets = extract_explore_tweets(self.favorites_tweets, self.q_uid)
                    message_number = len(tweets)
                ############################################STOP###############################################
                for i in range(message_number):
                    ############################################START##############################################
                    # if self.opt == self.opt_name[0]:
                    name1 = tweets[i]["tweet_type"]
                    name2 = tweets[i]["user"]["name"]
                    name3 = tweets[i]["user"]["screen_name"]
                    message = tweets[i]["text"].replace("\u200c", "")  # .split('\n')

                    ############################################STOP###############################################
                    contact_widget = Messages(name1, name2, name3, message)
                    # self.message_lst.addWidget(contact_widget)
                    list_item = QListWidgetItem(self.message_lst)
                    list_item.setSizeHint(contact_widget.sizeHint())
                    self.message_lst.setItemWidget(list_item, contact_widget)
                if message_number == 0:
                    contact_widget = QLabel("No tweet!")
                    list_item = QListWidgetItem(self.message_lst)
                    list_item.setSizeHint(contact_widget.sizeHint())
                    self.message_lst.setItemWidget(list_item, contact_widget)
                self.progress_bar.hide()
            self.bottomright_layout.addWidget(self.message_lst)
            self.bottomright_layout.addWidget(self.progress_bar)


def to_user_tid_score(tid, uid, type, origin_tid, origin_uid):
    """This function used for calculate primary rating matrix.

    This function get some information of a tweet and Returns
    the score that two users (the user who posted this tweet
    and the user who posted the original tweet and has now
    been replied to, retweeted, or quoted by this tweet)
    give to these two tweets FOR EACH USER.

    The output is a list of tuples, where each tuple contains
    a user ID and a list of tweets that have been rated (along
    with the value of the rating).
    Output format: [((u_i, ((t_j, s_ij),)) ...]
    """

    origin_tid = [i for i in origin_tid if i is not None]
    origin_uid = [i for i in origin_uid if i is not None]

    if not origin_tid or not origin_uid:
        return [(uid, [(tid, owner_s)])]

    to_s = (
        rep_to_s
        if type == "replied"
        else ret_to_s
        if type == "retweeted"
        else qut_to_s
        if type == "quoted"
        else 0
    )
    from_s = (
        rep_from_s
        if type == "replied"
        else ret_from_s
        if type == "retweeted"
        else qut_from_s
        if type == "quoted"
        else 0
    )

    return [
        (uid, [(tid, owner_s), (origin_tid[0], from_s)]),
        (origin_uid[0], [(tid, to_s)]),
    ]


def to_tid_user_score(tid, uid, type, origin_tid, origin_uid):
    """This function used for secondary rating.

    This function get some information of a tweet and Returns
    the score that two users (the user who posted this tweet
    and the user who posted the original tweet and has now
    been replied to, retweeted, or quoted by this tweet)
    give to these two tweets FOR EACH TWEET.

    The output is a list of tuples, where each tuple contains
    a tweet ID and a tuple list of users rate to it (along with
    the value of the rating).
    Output format: [(tid_i, ((u_j, s_ij), ...)), ...]
    """
    if uid != to_tid_user_score.q_uid and origin_uid != to_tid_user_score.q_uid:
        return []

    origin_tid = [i for i in origin_tid if i is not None]
    origin_uid = [i for i in origin_uid if i is not None]

    if not origin_tid or not origin_uid:
        return [(tid, ((uid, owner_s),))]

    to_s = (
        rep_to_s
        if type == "replied"
        else ret_to_s
        if type == "retweeted"
        else qut_to_s
        if type == "quoted"
        else 0
    )
    from_s = (
        rep_from_s
        if type == "replied"
        else ret_from_s
        if type == "retweeted"
        else qut_from_s
        if type == "quoted"
        else 0
    )

    return [
        (
            tid,
            (
                (uid, owner_s),
                (origin_uid[0], to_s),
            ),
        ),
        (origin_tid[0], ((uid, from_s),)),
    ]


def extract_tuples(tweet_info, func):
    """
    This function extract some information of tweet and send it
    to one of the following funcions and return their results:
    - to_user_tid_score
    - to_tid_user_score
    """
    return func(
        tweet_info.get("id"),
        tweet_info["user"].get("id"),
        tweet_info.get("tweet_type"),
        [
            tweet_info.get("in_reply_to_status_id_str", None),
            tweet_info.get("quoted_status", {}).get("id"),
            tweet_info.get("retweeted_status", {}).get("id"),
        ],
        [
            tweet_info.get("in_reply_to_user_id_str", None),
            tweet_info.get("quoted_status", {}).get("user", {}).get("id"),
            tweet_info.get("retweeted_status", {}).get("user", {}).get("id"),
        ],
    )


def extract_relations(x):
    """
    This function extracts relationships between users who interacted
    with this tweet.

    x contains the tweet_id (tid) and a list of tuples. Each tuple
    contains a user_id (u) and its interaction with the tweet (score
    given to the tweet).
    x: (tid, [(u1, s1), ...])

    The output is a list that each element of it, contains the user_id
    (u1) and a list of tuples. Each tuple contains a tweet_id (tid),
    second user_id that has relation to u1 (called u2), and the score
    given by each of these two users to this tweet (s1 and s2 respectively)
    output:
    [(u1, [(tid, u2, s1, s2),
            (tid, u3, s1, s3),
            ...
           ]),
     (u2, [(tid, u1, s2, s1),
            (tid, u3, s2, s3),
            ...
                  ]),
     ...
    ]

    """

    tid, user_score = x

    for (u1, s1) in user_score:
        if u1 == extract_relations.q_uid:
            u1_relations = []
            for (u2, s2) in user_score:
                if u1 != u2 and s1 > rep_to_s:
                    u1_relations.append((tid, u2, s1, s2))
            if u1_relations:
                return [
                    (
                        u1,
                        sorted(u1_relations, key=lambda x: -x[2] * x[3])[
                            : min(100, len(u1_relations))
                        ],
                    )
                ]

    return [(None, [])]


def find_user_favorites(x, user_scores):
    """
    user_scores: {ui: [(t_j, s_ij), ...]}
    x: (u1, ((tid, uj, s1, sj), ...))
    """
    th = 0.05
    u, relations = x
    favorite = {}
    if relations:
        for (tid, uj, s1, s2) in relations:
            favorite[tid] = favorite.get(tid, 0) + s1
            for (t, s) in user_scores.get(uj, []):
                favorite[t] = favorite.get(t, 0) + s1 * s2 * s
    if favorite:
        max_favorite = max(favorite.values())
        th = 0.05 * max_favorite
        while len(favorite) > 100:
            favorite = {t: i for t, i in favorite.items() if i > th}
            th *= 1.1
        return (u, favorite)
    return (u, {})


def calculate_rating_matrix():
    # [((u_i, ((t_j, s_ij),)), ...]
    user_tid_score_rdd = tweets_rdd.flatMap(
        lambda x: extract_tuples(json.loads(x), func=to_user_tid_score)
    )

    # [(u_i, ((t_j, s_ij), ...)), ...]
    user_tids_scores_rdd = user_tid_score_rdd.reduceByKey(
        lambda x, y: x if not x.extend(y) else x
    )

    user_scores_list = user_tids_scores_rdd.collect()

    return dict(user_scores_list)


def extract_user_origin_tweets(tweets, uid):
    origin_tweets = []
    for t in tweets:
        if t["tweet_type"] == "generated":
            if t.get("user", {}).get("id", "0") == uid:
                origin_tweets.append(t)

    return origin_tweets


def extract_user_replied_tweets(tweets, uid):
    replied_tweets = []
    for t in tweets:
        if t["tweet_type"] == "replied":
            if t.get("user", {}).get("id", "0") == uid:
                replied_tweets.append(t)

    return replied_tweets


def extract_user_quoted_tweets(tweets, uid):
    quoted_tweets = []
    for t in tweets:
        if t["tweet_type"] == "quoted":
            if t.get("user", {}).get("id", "0") == uid:
                quoted_tweets.append(t)

    return quoted_tweets


def extract_user_retweeted_tweets(tweets, uid):
    retweeted_tweets = []
    for t in tweets:
        if t["tweet_type"] == "retweeted":
            if t.get("user", {}).get("id", "0") == uid:
                retweeted_tweets.append(t)

    return retweeted_tweets


def extract_replied_to_user_tweets(tweets, uid):
    replied_tweets = []
    for t in tweets:
        if t["tweet_type"] == "replied":
            if t.get("in_reply_to_user_id_str", "0") == uid:
                replied_tweets.append(t)

    return replied_tweets


def extract_quoted_user_tweets(tweets, uid):
    quoted_tweets = []
    for t in tweets:
        if t["tweet_type"] == "quoted":
            if t.get("quoted_status", {}).get("user", {}).get("id", "0") == uid:
                quoted_tweets.append(t)

    return quoted_tweets


def extract_retweeted_user_tweets(tweets, uid):
    retweeted_tweets = []
    for t in tweets:
        if t["tweet_type"] == "retweeted":
            if t.get("retweeted_status", {}).get("user", {}).get("id", "0") == uid:
                retweeted_tweets.append(t)

    return retweeted_tweets


def extract_explore_tweets(tweets, uid):
    explore_tweets = []
    for t in tweets:
        if t.get("user", {}).get("id", "0") != uid:
            if t.get("retweeted_status", {}).get("user", {}).get("id", "0") != uid:
                if t.get("quoted_status", {}).get("user", {}).get("id", "0") != uid:
                    if t.get("in_reply_to_user_id_str", "0") != uid:
                        explore_tweets.append(t)

    return explore_tweets


def show_tweets(tweets, tweets_type):
    print("-" * 100)
    print(f"Number of {tweets_type}:", len(tweets), "\n")
    ts = [t["text"].replace("\u200c", "").split("\n") for t in tweets]
    for t in ts:
        print("-" * 100)
        for line in t:
            print(line)
    print("-" * 100)


def extract_tweets_id(q_uid):
    # [(tid, [(u, s)]), ...]
    to_tid_user_score.q_uid = q_uid
    tid_user_score_rdd = tweets_rdd.flatMap(
        lambda x: extract_tuples(json.loads(x), func=to_tid_user_score)
    )

    # [(tid, [(u, s), ...]), ...]
    tid_users_scores_rdd = tid_user_score_rdd.reduceByKey(lambda x, y: x + y)

    # [(q_uid, [(tid, u2, s1, s2)])]
    extract_relations.q_uid = q_uid
    user_relation_rdd = tid_users_scores_rdd.flatMap(extract_relations).filter(
        lambda x: x[1]
    )

    # [(q_uid, [(tid, u2, s1, s2), ...])]
    user_relations_rdd = user_relation_rdd.reduceByKey(
        lambda x, y: x if not x.extend(y) else x
    )

    # Take query user's relations in ram
    user_relations = user_relations_rdd.take(1)[0]

    # Find the ID of the user's favorite tweets
    user_favorites = find_user_favorites(x=user_relations, user_scores=user_scores_dict)

    necessary_keys = [
        "tweet_type",
        "user",
        "id",
        "text",
        "name",
        "screen_name",
        "in_reply_to_status_id_str",
        "in_reply_to_user_id_str",
        "quoted_status",
        "retweeted_status",
        "profile_image_url",
    ]

    favorites_tweets = (
        tweets_rdd.filter(
            lambda x: json.loads(x)["id"] in user_favorites[1].keys()
            or json.loads(x)["user"]["id"] == q_uid
        )
        .map(
            lambda x: dict(
                filter(lambda y: y[0] in necessary_keys, json.loads(x).items())
            )
        )
        .collect()
    )
    return (user_favorites, favorites_tweets)


def extract_users(x):
    tweet_info = json.loads(x)

    uids = [tweet_info["user"].get("id")]

    origin_uid = [
        tweet_info.get("in_reply_to_user_id_str", None),
        tweet_info.get("quoted_status", {}).get("user", {}).get("id"),
        tweet_info.get("retweeted_status", {}).get("user", {}).get("id"),
    ]

    origin_uid = [i for i in origin_uid if i is not None]

    if origin_uid:
        uids.append(origin_uid[0])

    return uids


def get_recommended_users(user_favorites, q_uid):
    recommended_users = (
        tweets_rdd.filter(
            lambda x: json.loads(x)["id"] in user_favorites[1].keys()
            or json.loads(x)["user"]["id"] == q_uid
        )
        .flatMap(extract_users)
        .collect()
    )

    counts = Counter(recommended_users)
    mcru = counts.most_common(20)  # most_common_recommended_users
    return mcru[1:]


def extract_rec_users_tweets(q_uid, mcru_id):
    necessary_keys = [
        "tweet_type",
        "user",
        "id",
        "text",
        "name",
        "screen_name",
        "in_reply_to_status_id_str",
        "in_reply_to_user_id_str",
        "quoted_status",
        "retweeted_status",
        "profile_image_url",
    ]
    return (
        tweets_rdd.filter(
            lambda x: json.loads(x)["user"]["id"] in mcru_id
            or json.loads(x)["user"]["id"] == q_uid
        )
        .map(
            lambda x: dict(
                filter(lambda y: y[0] in necessary_keys, json.loads(x).items())
            )
        )
        .collect()
    )


if __name__ == "__main__":
    tweets_rdd = sc.textFile("twitter_data_v2.jsonl")
    tweets_rdd = tweets_rdd.sample(False, 0.1, seed=42)

    print("Retriving Users....")
    users = tweets_rdd.map(lambda x: json.loads(x)["user"].get("id")).collect()
    counts = Counter(users)
    users = counts.most_common()

    print("Calculating Rating Matrix (Please Wait)....")
    user_scores_dict = calculate_rating_matrix()

    app = QApplication(sys.argv)
    window = TwitterApp(users)
    window.show()

    sys.exit(app.exec())
