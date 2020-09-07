from logger_settings import app_logger
import sqlite3

connection = sqlite3.connect('database.db')
c = connection.cursor()




connection.close()
    