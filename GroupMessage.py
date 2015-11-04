#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb


class GroupMessage:
    def __init__(self):
        self.xslSocial_connection = MySQLdb.connect(host="192.168.100.136", user="root", passwd="780810", db="XslSocial",
                                                    port=33306, charset="utf8")
        self.xslSocial_cursor = self.xslSocial_connection.cursor(MySQLdb.cursors.DictCursor)
        print("init XslSocial connection")

    def close(self):
        self.xslSocial_cursor.close()
        self.xslSocial_connection.close()
        print("close XslSocial connection")
        return

    def start_update_group_owner_name(self):
        print("start update group messge groupUID")
        cursor = self.xslSocial_cursor

        select_social_sql = "select ID,ShareUID from GroupMessage where ShareUID=GroupUID"
        try:
            cursor.execute(select_social_sql)
            results = cursor.fetchall()
            for row in results:
                shareUid = row["ShareUID"]
                id = row["ID"]
                username = self.find_name_by_id(shareUid)
                if username != "":
                    update_name_sql = "update GroupMessage set GroupUID = %s where ID = %s"
                    cursor.execute(update_name_sql, (username, id))
                    self.xslSocial_connection.commit()
        except:
            print("update group owner name is error")
            cursor.rollback()
        finally:
            print("update group owner name end")

    def find_name_by_id(self, id):
        select_user_sql = "select GroupUID from ShareInfo where ShareUID = '%s' " % id

        cursor = self.xslSocial_cursor

        username = ""
        try:
            cursor.execute(select_user_sql)
            data = cursor.fetchone()
            if (data != None):
                username = data["GroupUID"]
        except:
            print("select user is error")
        finally:
            return username


def main():
    try:
        group = GroupMessage()
        group.start_update_group_owner_name()
    except Exception, e:
        print e

    finally:
        group.close()


if __name__ == "__main__":
    main()







