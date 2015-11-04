#!/usr/bin/python
# -*- coding: utf-8 -*-

import uuid
import time

import MySQLdb


class CreateGroupFromShareUser:
    GroupType = 2
    JoinType = 1
    OpenType = 1
    Status = 1

    UserCache = {}

    MediacalSpeciality = {}

    list_member_sql = []
    list_group_sql = []
    list_shareuser_sql = []
    list_shareinfo_sql = []

    def __init__(self):
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

        self.xslSocial_connection = MySQLdb.connect(host="192.168.168.131", user="root", passwd="780810",
                                                    db="XslSocial",
                                                    port=33306, charset="utf8")

        self.xslSocial_cursor = self.xslSocial_connection.cursor(MySQLdb.cursors.DictCursor)
        print("init XslSocial connection")
        self.userCenter_connection = MySQLdb.connect(host="192.168.168.136", user="root", passwd="780810",
                                                     db="UserCenter", port=33306, charset="utf8")
        self.userCenter_cursor = self.userCenter_connection.cursor(MySQLdb.cursors.DictCursor)

        self.casefolder_connection = MySQLdb.connect(host="192.168.168.131", user="root", passwd="780810",
                                                     db="NewCaseFolder", port=33306, charset="utf8")
        self.casefolder_cursor = self.casefolder_connection.cursor(MySQLdb.cursors.DictCursor)

    def close(self):
        self.xslSocial_cursor.close()
        self.xslSocial_connection.close()
        print("close social connection")
        self.userCenter_cursor.close()
        self.userCenter_connection.close()
        print("close user center connection")
        self.casefolder_cursor.close()
        self.casefolder_connection.close()
        print("close case folder connection")
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


    def createGroup(self, group_info):
        millis = int(round(time.time() * 1000))
        data = (group_info["GroupUID"], group_info["GroupName"], group_info["UserID"], group_info["UserName"],
                group_info["MemberCount"], self.GroupType, self.JoinType, self.OpenType, self.Status, millis, millis)
        self.list_group_sql.append(data)


    def createGroupMember(self, lt_user, group_uid):
        millis = int(round(time.time() * 1000))
        for id in lt_user:
            userInfo = self.find_user_info_by_id(id)
            user_name = userInfo["TrueName"]
            hosptial = userInfo["Hospital"]
            department = ""
            if userInfo["MediacalSpeciality"] != None and userInfo["MediacalSpeciality"] != "":
                if userInfo["MediacalSpeciality"] in self.MediacalSpeciality:
                    medicalSpeciality = self.MediacalSpeciality[str(userInfo["MediacalSpeciality"])]
                    department = medicalSpeciality["TagName"]

            data = (group_uid, lt_user[0], id, user_name, hosptial, department, millis, millis)
            self.list_member_sql.append(data)

    def updateShareGroupUID(self, share_uid, group_uid):
        data = (group_uid, share_uid)
        self.list_shareinfo_sql.append(data)

    def createNewGroupInfo(self, datas):

        distinct_passive_user_id = [datas[0]["ActiveUserID"]]
        for index in datas:
            if index["PassiveUserID"] not in distinct_passive_user_id:
                distinct_passive_user_id.append(index["PassiveUserID"])
        if len(distinct_passive_user_id) == 0:
            return None

        result = {}
        result["UserName"] = self.find_user_info_by_id(distinct_passive_user_id[0])["TrueName"]
        if len(distinct_passive_user_id) > 2:
            data = self.find_user_info_by_id(distinct_passive_user_id[1])
            result["GroupName"] = result["UserName"] + u"、" + data["TrueName"] + u"等的群"
        elif len(distinct_passive_user_id) > 1:
            data = self.find_user_info_by_id(distinct_passive_user_id[1])
            if data != None and data["TrueName"]!=None:
                result["GroupName"] = result["UserName"] + u"、" + data["TrueName"] + u"的群"
            else:
                result["GroupName"] = result["UserName"] + u"的群"
        else:
            result["GroupName"] = result["UserName"] + u"的群"
        result["MemberCount"] = len(distinct_passive_user_id)
        result["GroupUID"] = str(uuid.uuid1())
        result["UserID"] = distinct_passive_user_id[0]
        result["DistinctUserID"] = distinct_passive_user_id
        return result

    def getAllShareUserByShareUID(self, share_uid):
        cursor = self.xslSocial_cursor
        select_all_share_user = "select ActiveUserID,PassiveUserID,GroupUID,Type from ShareUser where ShareUID = %s order by Type desc"
        cursor.execute(select_all_share_user, share_uid)
        results = cursor.fetchall()

        # #验证 active,passiveUserId 是否可用
        results = self.validShareUser(results)
        if results != None and len(results) != 0:
            # #只分配了一个群
            if self.isShareGroup(results) and not (self.isShareUser(results)) and self.isOnlySameGroup(results):
                if results[0]["GroupUID"] != "":
                    self.updateShareGroupUID(share_uid, results[0]["GroupUID"])
                else:
                    self.updateShareGroupUID(share_uid, results[1]["GroupUID"])
            else:
                dict_group = self.createNewGroupInfo(results)
                self.createGroup(dict_group)
                self.createGroupMember(dict_group["DistinctUserID"], dict_group["GroupUID"])
                self.updateShareGroupUID(share_uid, dict_group["GroupUID"])

        data = (share_uid,)
        self.list_shareuser_sql.append(data)

    nowFetch = 1

    def getAllDistinctShare(self):

        print('now is %s %s ' % (str(self.nowFetch), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        cursor = self.xslSocial_cursor
        #select_distinct_share = "select distinct ShareUID from ShareUser where ShareUID='3CCE9BDA-E631-4D3A-BDF8-2F9F27296E1E' "
        select_distinct_share = "select distinct ShareUID from ShareUser where Status =1 and UploadStatus = 23 limit 100"
        cursor.execute(select_distinct_share)
        results = cursor.fetchall()
        if len(results) == 0:
            return None
        for row in results:
            self.getAllShareUserByShareUID(row["ShareUID"])
        self.batch_createGroup_createMember_updateShareInfo_updateShareUser()
        self.nowFetch = self.nowFetch + 1
        self.getAllDistinctShare()

    def batch_createGroup_createMember_updateShareInfo_updateShareUser(self):
        cursor = self.xslSocial_cursor
        connection = self.xslSocial_connection
        # 批量创建群
        insert_group_sql = "insert into BuddyGroup (GroupUID,GroupName,UserID,UserName,MemberCount,GroupType,JoinType,OpenType,Status,ServerCreateTimestame,ServerUpdateTimestame) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.executemany(insert_group_sql, self.list_group_sql)
        # 批量插入新的群成员
        insert_member_sql = "insert into BuddyGroupMember(GroupUID,InviterUserID,UserID,UserName,Hosptial,Department,ServerCreateTimestame,ServerUpdateTimestame) values (%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.executemany(insert_member_sql, self.list_member_sql)
        # 批量修改ShareInfo groupUID
        update_share_group_uid = "update ShareInfo set GroupUID = %s where ShareUID =%s"
        cursor.executemany(update_share_group_uid, self.list_shareinfo_sql)
        # 批量修改shareuser状态
        update_shareuser_status = "update ShareUser set UploadStatus = 24 where ShareUID =%s"
        cursor.executemany(update_shareuser_status, self.list_shareuser_sql)
        connection.commit()

        self.list_group_sql = []
        self.list_member_sql = []
        self.list_shareinfo_sql = []
        self.list_shareuser_sql = []

    def isShareGroup(self, datas):
        result = False
        for index in datas:
            if index["GroupUID"] == None or index["GroupUID"] == "":
                continue
            else:
                result = True
                break

        return result

    def isShareUser(self, datas):
        result = False
        for index in datas:
            if index["ActiveUserID"] == index["PassiveUserID"]:
                continue
            elif index["GroupUID"] == None or index["GroupUID"] == "":
                result = True
                break
            else:
                continue

        return result

    def isOnlySameGroup(self, datas):
        result = True
        group_uid = ""
        for index in datas:
            if group_uid == "":
                group_uid = index["GroupUID"]

            elif group_uid == index["GroupUID"]:
                continue
            else:
                result = False
                break
        return result


    def select_all_mediacal_speciality(self):
        cursor = self.casefolder_cursor

        select_all_sql = "select ID,TagName,ParentID from MediacalSpeciality"

        cursor.execute(select_all_sql)
        results = cursor.fetchall()

        for row in results:
            self.MediacalSpeciality[str(row["ID"])] = row


    def find_user_info_by_id(self, id):
        if id == None:
            return None
        if not (self.UserCache.has_key(str(id))):
            select_user_sql = "select TrueName ,Hospital,MediacalSpeciality  from User where ID = %s " % id
            cursor = self.userCenter_cursor
            cursor.execute(select_user_sql)
            data = cursor.fetchone()
            if (data != None):
                self.UserCache[str(id)] = data
            else:
                self.UserCache[str(id)] = None
        return self.UserCache[str(id)]


    def validShareUser(self, datas):
        resultLt = []
        if self.find_user_info_by_id(datas[0]["ActiveUserID"]) == None:
            return None
        for row in datas:
            if self.find_user_info_by_id(row["PassiveUserID"]) != None:
                resultLt.append(row)
        return resultLt


    def select_all_group_member(self):
        cursor = self.xslSocial_cursor
        connection = self.xslSocial_connection
        select_all_sql = "select distinct UserID from BuddyGroupMember where UploadStatus =21 and UserID is not null limit  100"

        cursor.execute(select_all_sql)
        results = cursor.fetchall()
        if len(results) == 0:
            return None
        list_updateGroupMember = []
        for row in results:
            userInfo = self.find_user_info_by_id(row["UserID"])
            user_name = ""
            hosptial = ""
            department = ""
            if userInfo != None:
                user_name = userInfo["TrueName"]
                hosptial = userInfo["Hospital"]

                if userInfo["MediacalSpeciality"] != None and userInfo["MediacalSpeciality"] != "":
                    if userInfo["MediacalSpeciality"] in self.MediacalSpeciality:
                        medicalSpeciality = self.MediacalSpeciality[userInfo["MediacalSpeciality"]]
                        department = medicalSpeciality["TagName"]

            data = (user_name, hosptial, department, row["UserID"])
            list_updateGroupMember.append(data)

            if len(list_updateGroupMember) >= 100:
                update_group_member_sql = "update BuddyGroupMember set UserName = %s,Hosptial=%s,Department=%s,UploadStatus =22 where UserID = %s"
                cursor.executemany(update_group_member_sql, list_updateGroupMember)
                connection.commit()

        if len(list_updateGroupMember) > 0:
            update_group_member_sql = "update BuddyGroupMember set UserName = %s,Hosptial=%s,Department=%s,UploadStatus =22 where UserID = %s"
            cursor.executemany(update_group_member_sql, list_updateGroupMember)
            connection.commit()

        self.select_all_group_member()


    # def update_group_member(self, userName, hospital, department, userId):
    # cursor = self.xslSocial_cursor
    # connection = self.xslSocial_connection
    #
    # update_group_member_sql = "update BuddyGroupMember set UserName = %s,Hosptial=%s,Department=%s,UploadStatus =22 where InviterUserID = %s"
    # cursor.execute(update_group_member_sql, (userName, hospital, department, userId))
    # connection.commit()


    def find_all_user_from_share_user(self):
        cursor = self.xslSocial_cursor

        select_shareuserId_sql = "select distinct PassiveUserID from ShareUser"
        cursor.execute(select_shareuserId_sql)
        results = cursor.fetchall()
        list_userId = []
        for row in results:
            list_userId.append(row["PassiveUserID"])
            if len(list_userId) >= 100:
                self.find_user_info_by_ids(list_userId)
                list_userId = []

        if len(list_userId) > 0:
            self.find_user_info_by_ids(list_userId)
            list_userId = []

    def find_all_user_from_buddyGroupMember(self):
        cursor = self.xslSocial_cursor

        select_buddyGroupMember_sql = "select distinct UserID from BuddyGroupMember"
        cursor.execute(select_buddyGroupMember_sql)
        results = cursor.fetchall()
        list_userId = []
        for row in results:
            list_userId.append(row["UserID"])
            if len(list_userId) >= 100:
                self.find_user_info_by_ids(list_userId)
                list_userId = []

        if len(list_userId) > 0:
            self.find_user_info_by_ids(list_userId)
            list_userId = []

    def find_user_info_by_ids(self, list_userId):
        cursor = self.userCenter_cursor
        sql = 'select TrueName ,Hospital,MediacalSpeciality,ID  from User where ID in (%s)'
        in_p = ', '.join(map(lambda x: '%s', list_userId))
        sql = sql % in_p
        cursor.execute(sql, list_userId)
        results = cursor.fetchall()
        for row in results:
            id = row["ID"]
            if not (self.UserCache.has_key(str(id))):
                self.UserCache[str(id)] = row


def main():
    try:
        group = CreateGroupFromShareUser()
        # 初始化科室
        print('init mediacalSpeciality...%s' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        group.select_all_mediacal_speciality()
        # 初始化用户
        print('init user...%s' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        group.find_all_user_from_share_user()
        group.find_all_user_from_buddyGroupMember()
        # 修改已有的群成员信息
        print('start update exist groupMember...%s' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        group.select_all_group_member()
        # 修改shareuser
        print('start update shareuser...%s' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        #group.getAllDistinctShare()
    except Exception, e:
        print e
    finally:
        group.close()


if __name__ == "__main__":
    main()


            






