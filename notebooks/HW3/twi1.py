import sys

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTextEdit, QListWidget, QListWidgetItem, QLabel, QSplitter
from PySide6.QtGui import QPixmap
from PySide6.QtCore import *
from PySide6.QtGui import *
from PySide6.QtWidgets import *
from PySide6 import QtCore

import requests


class ContactWidgetItem(QWidget):
    clicked = Signal()

    def __init__(self, name, image_url=None, width=40, height=40, parent=None):
        super().__init__(parent)

        # Create QLabel for the contact image
        
        # Load image from URL and resize
        if image_url:
            self.image_label = QLabel()
            # image_data = requests.get(image_url).content
            # pixmap = QPixmap()
            # pixmap.loadFromData(image_data)
            # pixmap = pixmap.scaled(width, height, Qt.KeepAspectRatio)  # Resize the image while maintaining aspect ratio
            # self.image_label.setPixmap(pixmap)
            pixmap = QPixmap(image_url)
            self.image_label.setPixmap(pixmap.scaled(width, height, Qt.KeepAspectRatio))

            # Set the resized pixmap to the QLabel
            

            # Create QLabel for the contact name
            self.name_label = QLabel(name)

            # Layout for the custom widget
            layout = QHBoxLayout()

            layout.addWidget(self.image_label, 2)
            layout.addWidget(self.name_label, 5)

            # layout.setStretchFactor(0, 2)
            # layout.setStretchFactor(1, 5) 

            self.setLayout(layout)
        
        else:
            self.name_label = QLabel(name)
    
    def mousePressEvent(self, event):
        self.clicked.emit()
            
class Messages(QWidget):

    def __init__(self, name1, name2, name3, message ,image_url='user-286.png', parent=None):
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
        self.name_label.setFont(QFont("Times New Roman", 9)) 

        self.name_label2 = QLabel(name2) 
        self.name_label2.setStyleSheet("color: red")  
        self.name_label2.setFont(QFont("Comic Sans MS", 8))

        self.name_label3 = QLabel(name3)  
        self.name_label3.setStyleSheet("color: green") 
        self.name_label3.setFont(QFont("Comic Sans MS", 8))


        top_right_layout.addWidget(self.name_label)
        top_right_layout.addWidget(self.name_label2)
        top_right_layout.addWidget(self.name_label3)
        self.name_label.setFixedSize(x2, y2)  
        self.name_label2.setFixedSize(x2, y2)
        self.name_label3.setFixedSize(x2, y2)

        wd = QWidget()
        wd.setLayout(top_right_layout)

        top_layout.addWidget(self.image_label,1)
        top_layout.addWidget(wd, 10)

        self.content = QLabel(message)
        self.content.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        self.content.setWordWrap(True)
        self.content.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        main_layout.addLayout(top_layout)
        main_layout.addWidget(self.content)


class TwitterApp(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Twitter-Like App")
        self.setGeometry(50, 50, 1000, 700)

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)

        self.setup_ui()

        self.flag1 = 0  
        self.contact = None
        self.opt = None

    def setup_ui(self):
        main_layout = QHBoxLayout(self.central_widget)
        splitter = QSplitter(Qt.Horizontal)

        left_layout = QVBoxLayout()
        right_layout = QVBoxLayout()
        topright_layout = QHBoxLayout()
        contacts_list = QListWidget()
        self.message_lst = QListWidget()

        for i in range(50):
            contact_widget = ContactWidgetItem(f"Contact {i+1}", 'user-286.png')
            contact_widget.clicked.connect(self.click_contact)
            list_item = QListWidgetItem(contacts_list)
            list_item.setSizeHint(contact_widget.sizeHint())
            contacts_list.setItemWidget(list_item, contact_widget)

        left_layout.addWidget(contacts_list)


        self.opt_name = ['option1', 'option2', 'option3', 'option4', 'option5']
        for i in range(5):
            options_label = QPushButton(self.opt_name[i])
            options_label.clicked.connect(self.options)
            topright_layout.addWidget(options_label)

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

        splitter.widget(0).setLayout(left_layout)
        splitter.widget(1).setLayout(right_layout)

    def click_contact(self):
        sender_widget = self.sender()
        if isinstance(sender_widget, ContactWidgetItem):
            self.contact  = sender_widget.name_label.text()
            self.flag1 = 0
            self.progress_bar.setRange(0, 0) 
            self.progress_bar.show()
            QtCore.QTimer.singleShot(5000, lambda: self.hide_progress_bar())

    def hide_progress_bar(self):
        if self.flag1 != 1:
            QtCore.QTimer.singleShot(100, lambda: self.hide_progress_bar())
            # self.flag1 = 1
        else:
            self.progress_bar.hide()

    def options(self):
        if self.contact == None:
            self.message_lst.close()
            self.progress_bar.close()
            self.progress_bar = QProgressBar()  
            self.message_lst = QListWidget()
            contact_widget = QLabel("Please Choose a User")
            list_item = QListWidgetItem(self.message_lst)
            list_item.setSizeHint(contact_widget.sizeHint())
            self.message_lst.setItemWidget(list_item, contact_widget)
            self.bottomright_layout.addWidget(self.message_lst) 
            self.bottomright_layout.addWidget(self.progress_bar)  
            self.progress_bar.hide()
        else:
            sender_widget = self.sender()
            self.opt = sender_widget.text()
            self.message_lst.close()
            self.progress_bar.close()
            self.progress_bar = QProgressBar()  
            
            self.message_lst = QListWidget()
            ############################################START##############################################
            if self.opt == self.opt_name[0]:
                message_number = 10
            ############################################STOP###############################################
            for i in range(message_number):
                ############################################START##############################################
                if self.opt == self.opt_name[0]:
                    name1 = 'mahdi'
                    name2 = 'javad'
                    name3 = 'reza'
                    message = 'salam mahdi jooon khoobi?' * 50
                ############################################STOP###############################################
                contact_widget = Messages(name1, name2, name3, message)
                # self.message_lst.addWidget(contact_widget)
                list_item = QListWidgetItem(self.message_lst)
                list_item.setSizeHint(contact_widget.sizeHint())
                self.message_lst.setItemWidget(list_item, contact_widget)
            
            self.bottomright_layout.addWidget(self.message_lst) 
            self.bottomright_layout.addWidget(self.progress_bar)  
            self.progress_bar.hide()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = TwitterApp()
    window.show()
    sys.exit(app.exec())
