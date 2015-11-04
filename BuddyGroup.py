#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb


class BuddyGroup:
    def __init__(self):
        self.xslSocial_connection = MySQLdb.connect(host="192.168.168.131", user="root", passwd="780810", db="XslSocial",
                                                    port=33306, charset="utf8")
        self.xslSocial_cursor = self.xslSocial_connection.cursor(MySQLdb.cursors.DictCursor)
        print("init XslSocial connection")
        self.userCenter_connection = MySQLdb.connect(host="192.168.168.136", user="root", passwd="780810",
                                                     db="UserCenter", port=33306, charset="utf8")
        self.userCenter_cursor = self.userCenter_connection.cursor(MySQLdb.cursors.DictCursor)

    def close(self):
        self.xslSocial_cursor.close()
        self.xslSocial_connection.close()
        print("close XslSocial connection")
        self.userCenter_cursor.close()
        self.userCenter_connection.close()
        print("close UserCenter connection")
        return

    def start_update_group_owner_name(self):
        print("start update group owner name")
        cursor = self.xslSocial_cursor

        select_social_sql = "select UserID,ID from BuddyGroup where UserName is null "
        try:
            cursor.execute(select_social_sql)
            results = cursor.fetchall()
            for row in results:
                userid = row["UserID"]
                id = row["ID"]
                username = self.find_name_by_id(userid)
                update_name_sql = "update BuddyGroup set UserName = %s where ID = %s"
                cursor.execute(update_name_sql, (username, id))
                self.xslSocial_connection.commit()
        except:
            print("update group owner name is error")
            cursor.rollback()
        finally:
            print("update group owner name end")

    def find_name_by_id(self, id):
        select_user_sql = "select TrueName from User where ID = %s " % id

        cursor = self.userCenter_cursor

        username = ""
        try:
            cursor.execute(select_user_sql)
            data = cursor.fetchone()
            if (data != None):
                username = data["TrueName"]
        except:
            print("select user is error")
        finally:
            return username
def main():
    try:
        group = BuddyGroup()
        group.start_update_group_owner_name()
    except Exception, e:
        print e

    finally:
        group.close()

if __name__ == "__main__":
    main()







