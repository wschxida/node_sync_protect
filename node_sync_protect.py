#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : node_sync_protect.py
# @Author: Cedar
# @Date  : 2020/4/2
# @Desc  :


import pymysql


def query_mysql(config_params, query_sql):
    """
    执行SQL
    :param config_params:
    :param query_sql:
    :return:
    """
    # 连接mysql
    config = {
        'host': config_params["host"],
        'port': config_params["port"],
        'user': config_params["user"],
        'passwd': config_params["passwd"],
        'db': config_params["db"],
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }
    conn = pymysql.connect(**config)
    conn.autocommit(1)
    # 使用cursor()方法获取操作游标
    cur = conn.cursor()
    cur.execute(query_sql)  # 执行sql语句
    results = cur.fetchall()  # 获取查询的所有记录
    conn.close()  # 关闭连接

    return results


def sync_heart_beat_from_extractor_to_center(config_extractor, config_center):
    """
    只要采集节点的心跳10分钟内没同步到中心库，就执行更新
    """
    try:
        # 查询采集库node心跳，最近一小时有心跳的节点
        sql_extractor = "select Node_ID,Last_Heart_Beat_Time from node " \
              "where node_role='E' and Is_Enabled=1 and Sub_System_Name='KWM' " \
              "and Last_Heart_Beat_Time>DATE_SUB(CURRENT_TIME(),INTERVAL 1 hour);"
        query_result_extractor = query_mysql(config_extractor, sql_extractor)
        # 把数据库查询结果改成字典Node_ID：Last_Heart_Beat_Time的形式，便于后面操作
        node_heart_beat_extractor = {}
        for item in query_result_extractor:
            node_heart_beat_extractor[item["Node_ID"]] = item["Last_Heart_Beat_Time"]

        # 查询中心库node表心跳
        sql_center = "select Node_ID,Last_Heart_Beat_Time from node " \
                     "where node_role='E' and Is_Enabled=1 and Sub_System_Name='KWM';"
        query_result_center = query_mysql(config_center, sql_center)
        # 把数据库查询结果改成字典Node_ID：Last_Heart_Beat_Time的形式，便于后面操作
        node_heart_beat_center = {}
        for item in query_result_center:
            node_heart_beat_center[item["Node_ID"]] = item["Last_Heart_Beat_Time"]

        # 更新中心库的采集服务器心跳
        for i in node_heart_beat_extractor.keys():
            time_diff = node_heart_beat_extractor[i] - node_heart_beat_center[i]
            # print(node_heart_beat_extractor[i])
            # print(node_heart_beat_center[i])
            second_diff = time_diff.days*24*3600 + time_diff.seconds
            # print(i, second_diff)
            # 如果采集库的心跳比中心库大10分钟以上，就更新回去
            if second_diff > 600:
                print("sync_heart_beat_from_extractor_to_center node_id:", i, " second_diff:", second_diff)
                update_heart_beat_sql = f"update node set Last_Heart_Beat_Time='{node_heart_beat_extractor[i]}' where Node_ID={i}"
                query_mysql(config_center, update_heart_beat_sql)

    except Exception as e:
        raise e


def sync_node_from_center_to_extractor(config_center, config_extractor):
    """
    如果中心库和采集库node表的Is_Enabled和Is_Working查询结果不一致，就执行同步
    把中心库node表的Is_Enabled及Is_Working同步到采集库
    """
    try:
        sql = "select Node_ID,Is_Enabled,Is_Working from node;"
        # 查询中心库
        query_result_center = query_mysql(config_center, sql)
        # 查询采集库
        query_result_extractor = query_mysql(config_extractor, sql)

        # print(query_result_center)
        # print(query_result_extractor)

        # 如果中心库与采集库不一致，就执行更新
        if query_result_center != query_result_extractor:
            print("sync_node_from_center_to_extractor")
            for item in query_result_center:
                sql_text = f"update node set Is_Enabled={item['Is_Enabled']},Is_Working={item['Is_Working']} where Node_ID={item['Node_ID']};"
                sql_text = sql_text.replace('None', 'Null')
                query_mysql(config_extractor, sql_text)

    except Exception as e:
        raise e


def sync_node_in_node_group_from_center_to_extractor(config_center, config_extractor):
    """
    如果中心库和采集库node_in_node_group查询结果不一致，就执行同步
    把中心库node_in_node_group同步到采集库
    """
    try:
        sql = "select * from node_in_node_group;"
        # 查询中心库node_in_node_group
        query_result_center = query_mysql(config_center, sql)
        # 查询采集库node_in_node_group
        query_result_extractor = query_mysql(config_extractor, sql)

        # 如果中心库与采集库不一致，就执行更新
        if query_result_center != query_result_extractor:
            print("sync_node_in_node_group_from_center_to_extractor")
            for item in query_result_center:
                sql_text = f"replace into node_in_node_group(Node_In_Node_Group_ID, Node_Group_Code, Node_ID, Part_No, Part_Amount) values({item['Node_In_Node_Group_ID']}, '{item['Node_Group_Code']}', {item['Node_ID']}, {item['Part_No']}, {item['Part_Amount']});"
                sql_text = sql_text.replace('None', 'Null')
                # print(sql_text)
                query_mysql(config_extractor, sql_text)

    except Exception as e:
        raise e


if __name__ == '__main__':
    # extractor = {'host': '192.168.1.166', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'test_extractor'}
    # center = {'host': '192.168.1.166', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'test_center'}

    center = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    extractor_117 = {'host': '192.168.1.117', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    print("---117 start---")
    # 只要采集节点的心跳10分钟内没同步到中心库，就执行更新：采集->中心
    sync_heart_beat_from_extractor_to_center(extractor_117, center)
    # 只要中心库和采集库node表的Is_Enabled和Is_Working查询结果不一致，就执行同步：中心->采集
    sync_node_from_center_to_extractor(center, extractor_117)
    # 只要中心库和采集库node_in_node_group查询结果不一致，就执行同步：中心->采集
    sync_node_in_node_group_from_center_to_extractor(center, extractor_117)
    print("---117 end---")

    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    print("---118 start---")
    # 只要采集节点的心跳10分钟内没同步到中心库，就执行更新：采集->中心
    sync_heart_beat_from_extractor_to_center(extractor_118, center)
    # 只要中心库和采集库node表的Is_Enabled和Is_Working查询结果不一致，就执行同步：中心->采集
    sync_node_from_center_to_extractor(center, extractor_118)
    # 只要中心库和采集库node_in_node_group查询结果不一致，就执行同步：中心->采集
    sync_node_in_node_group_from_center_to_extractor(center, extractor_118)
    print("---118 end---")

